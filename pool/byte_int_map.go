package pool

import (
	"bytes"
	"sync"

	"github.com/zeebo/xxh3"
)

// byteIntMapPool is the sync.Pool backing [*ByteIntMap] reuse. Declared via an init
// function to satisfy go vet's noCopy linter on sync.Pool.
var byteIntMapPool *sync.Pool

func init() {
	byteIntMapPool = &sync.Pool{New: func() any { return &ByteIntMap{} }}
}

// ByteIntMap is an open-addressing hash map from []byte keys to int values, using
// xxh3 for hashing and linear probing. It is designed for the dictionary-encoding
// hot path ([chunk.EncodeBytes]) where Go's built-in map[string]int is bottlenecked
// by string-key overhead and a slower hash function.
//
// The zero value is not usable; create one with [NewByteIntMap]. [Reset] clears it
// for reuse without freeing the backing arrays (so a pooled instance amortizes
// allocation). Not safe for concurrent use; callers own synchronization or pool per-goroutine.
//
// Capacity is rounded up to a power of two; the load factor is ≤ 0.75 (probed slots)
// before [grow] is triggered. Keys are compared with bytes.Equal (no string conversion).
type ByteIntMap struct {
	keys   [][]byte // key at slot i; nil ⇒ empty
	values []int    // value at slot i
	hashes []uint64 // cached hash (0 is reserved for empty)
	mask   uint64   // len(hashes)-1
	count  int      // number of live entries
	used   int      // number of non-empty slots (live + tombstones)
	growAt int      // precomputed: len(hashes)*3/4; grow when used+1 > growAt
}

// NewByteIntMap returns a pooled, ready-to-use map. A reused instance is cleared
// via [ByteIntMap.Reset] so its backing arrays are retained; a fresh one allocates
// at the initial capacity.
func NewByteIntMap() *ByteIntMap {
	const initial = 1 << 5 // 32 slots
	m := byteIntMapPool.Get().(*ByteIntMap)
	if m.keys == nil {
		m.keys = make([][]byte, initial)
		m.values = make([]int, initial)
		m.hashes = make([]uint64, initial)
		m.mask = initial - 1
		m.growAt = initial * 3 / 4
	} else {
		m.Reset() // clear stale data from a previous use
	}
	return m
}

// Reset clears the map for reuse without freeing the backing arrays.
func (m *ByteIntMap) Reset() {
	clear(m.hashes)
	clear(m.keys)
	m.count = 0
	m.used = 0
}

// Len returns the number of live entries in the map.
func (m *ByteIntMap) Len() int { return m.count }

// hashKey returns the xxh3 hash of b, ensuring it's never 0 (reserved for empty slot).
// Inlined by the compiler.
//
//go:inline
func hashKey(b []byte) uint64 {
	h := xxh3.Hash(b)
	if h == 0 {
		return 1
	}
	return h
}

// Get returns the value for key b and true if present.
//
//go:nosplit
func (m *ByteIntMap) Get(b []byte) (int, bool) {
	h := hashKey(b)
	i := h & m.mask
	for {
		sh := m.hashes[i]
		if sh == 0 {
			return 0, false // empty
		}
		if sh == h && bytes.Equal(m.keys[i], b) {
			return m.values[i], true
		}
		i = (i + 1) & m.mask
	}
}

// Put inserts or updates key b → v. Returns the old value and true if the key
// existed, or (v, false) for a new insertion.
func (m *ByteIntMap) Put(b []byte, v int) (int, bool) {
	if m.used+1 > m.growAt {
		m.grow()
	}
	h := hashKey(b)
	i := h & m.mask
	for {
		sh := m.hashes[i]
		if sh == 0 {
			m.keys[i] = b
			m.values[i] = v
			m.hashes[i] = h
			m.count++
			m.used++
			return v, false
		}
		if sh == h && bytes.Equal(m.keys[i], b) {
			old := m.values[i]
			m.values[i] = v
			return old, true
		}
		i = (i + 1) & m.mask
	}
}

// PutOrGet inserts b → v if b is absent; otherwise returns the existing value and
// false. This is the single-lookup dedup path for dictionary building: one probe
// chain either finds the existing id or inserts a new one.
//
//go:nosplit
func (m *ByteIntMap) PutOrGet(b []byte, v int) (int, bool) {
	if m.used+1 > m.growAt {
		m.grow()
	}
	h := hashKey(b)
	i := h & m.mask
	for {
		sh := m.hashes[i]
		if sh == 0 {
			m.keys[i] = b
			m.values[i] = v
			m.hashes[i] = h
			m.count++
			m.used++
			return v, false
		}
		if sh == h && bytes.Equal(m.keys[i], b) {
			return m.values[i], true
		}
		i = (i + 1) & m.mask
	}
}

// Delete removes key b. Returns true if it was present.
func (m *ByteIntMap) Delete(b []byte) bool {
	h := hashKey(b)
	i := h & m.mask
	for {
		sh := m.hashes[i]
		if sh == 0 {
			return false
		}
		if sh == h && bytes.Equal(m.keys[i], b) {
			m.keys[i] = nil
			m.hashes[i] = 0
			m.count--
			m.used--
			j := (i + 1) & m.mask
			for m.hashes[j] != 0 {
				k := m.keys[j]
				v := m.values[j]
				oldH := m.hashes[j]
				m.keys[j] = nil
				m.values[j] = 0
				m.hashes[j] = 0
				m.count--
				m.used--
				m.PutRaw(k, v, oldH)
				j = (j + 1) & m.mask
			}
			return true
		}
		i = (i + 1) & m.mask
	}
}

// PutRaw inserts with a precomputed hash (internal, for re-insertion after Delete).
func (m *ByteIntMap) PutRaw(b []byte, v int, h uint64) {
	i := h & m.mask
	for {
		if m.hashes[i] == 0 {
			m.keys[i] = b
			m.values[i] = v
			m.hashes[i] = h
			m.count++
			m.used++
			return
		}
		i = (i + 1) & m.mask
	}
}

// PutBack returns m to the pool for reuse. After this, m must not be used.
func (m *ByteIntMap) PutBack() { byteIntMapPool.Put(m) }

// ForEach calls fn for each (key, value) pair. Iteration order is unspecified.
func (m *ByteIntMap) ForEach(fn func(key []byte, value int)) {
	for i := range m.hashes {
		if m.hashes[i] != 0 && m.keys[i] != nil {
			fn(m.keys[i], m.values[i])
		}
	}
}

func (m *ByteIntMap) grow() {
	newCap := len(m.hashes) * 2
	oldKeys, oldVals, oldHashes := m.keys, m.values, m.hashes
	m.keys = make([][]byte, newCap)
	m.values = make([]int, newCap)
	m.hashes = make([]uint64, newCap)
	m.mask = uint64(newCap - 1)
	m.growAt = newCap * 3 / 4
	m.count = 0
	m.used = 0
	for i := range oldHashes {
		if oldHashes[i] != 0 && oldKeys[i] != nil {
			m.PutRaw(oldKeys[i], oldVals[i], oldHashes[i])
		}
	}
}
