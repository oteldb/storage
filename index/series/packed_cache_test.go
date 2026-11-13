package series

import (
	"bytes"
	"encoding/binary"
	"math/bits"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zeebo/xxh3"

	"github.com/oteldb/storage/internal/memsize"
	"github.com/oteldb/storage/signal"
)

const packedCacheChunk = 64 << 10

type packedCacheSlot struct {
	hash, offset, generation uint64
	length, ref              uint32
}

// Prototype: bounded owned packed keys and exact comparisons, with generation-scoped references.
type packedCache struct {
	slots  []packedCacheSlot
	chunks [][]byte
	limit  int64
	count  int
	hash   func([]byte) uint64
}

func newPackedCache(limit int64, expected int) *packedCache {
	c := &packedCache{limit: limit}
	n := 1 << bits.Len(uint(max(expected, 1)*4/3))
	for n > 32 && int64(n)*memsize.Of[packedCacheSlot]() > limit/4 {
		n >>= 1
	}
	if int64(n)*memsize.Of[packedCacheSlot]()+packedCacheChunk > limit {
		return c
	}
	c.slots = make([]packedCacheSlot, n)
	return c
}

func (c *packedCache) sizeBytes() int64 {
	return memsize.Of[packedCache]() + memsize.Slice(c.slots) + memsize.Slice(c.chunks) + int64(len(c.chunks))*packedCacheChunk
}

func (c *packedCache) fingerprint(key []byte) uint64 {
	h := xxh3.Hash(key)
	if c.hash != nil {
		h = c.hash(key)
	}
	if h == 0 {
		h = 1
	}
	return h
}

func (c *packedCache) find(key []byte, h uint64) *packedCacheSlot {
	if len(c.slots) == 0 {
		return nil
	}
	mask := uint64(len(c.slots) - 1)
	for i := h & mask; ; i = (i + 1) & mask {
		slot := &c.slots[i]
		if slot.hash == 0 {
			return slot
		}
		if slot.hash == h && int(slot.length) == len(key) {
			page := c.chunks[slot.offset/packedCacheChunk]
			off := int(slot.offset % packedCacheChunk)
			if bytes.Equal(key, page[off+16:off+16+int(slot.length)]) {
				return slot
			}
		}
	}
}

func (c *packedCache) lookup(key []byte, generation uint64) (signal.SeriesID, uint32, bool, bool) {
	slot := c.find(key, c.fingerprint(key))
	if slot == nil || slot.hash == 0 {
		return signal.SeriesID{}, 0, false, false
	}
	page := c.chunks[slot.offset/packedCacheChunk]
	off := int(slot.offset % packedCacheChunk)
	id := signal.SeriesID{Hi: binary.BigEndian.Uint64(page[off:]), Lo: binary.BigEndian.Uint64(page[off+8:])}
	return id, slot.ref, slot.generation == generation, true
}

func (c *packedCache) put(key []byte, id signal.SeriesID, ref uint32, generation uint64) bool {
	if len(key)+16 > packedCacheChunk {
		return false
	}
	h := c.fingerprint(key)
	slot := c.find(key, h)
	if slot == nil {
		return false
	}
	if slot.hash != 0 {
		slot.ref = ref
		slot.generation = generation
		return true
	}
	if c.count >= len(c.slots)*3/4 {
		return false
	}
	page := len(c.chunks) - 1
	if page < 0 || cap(c.chunks[page])-len(c.chunks[page]) < len(key)+16 {
		if c.sizeBytes()+packedCacheChunk+int64(2*(len(c.chunks)+1))*24 > c.limit {
			return false
		}
		c.chunks = append(c.chunks, make([]byte, 0, packedCacheChunk))
		page++
	}
	off := len(c.chunks[page])
	data := id.AppendBinary(c.chunks[page])
	data = append(data, key...)
	c.chunks[page] = data
	*slot = packedCacheSlot{hash: h, offset: uint64(page)*packedCacheChunk + uint64(off), generation: generation, length: uint32(len(key)), ref: ref}
	c.count++
	return true
}

func TestPackedCachePrototype(t *testing.T) {
	t.Parallel()
	for _, collision := range []bool{false, true} {
		c := newPackedCache(256<<10, 128)
		if collision {
			c.hash = func([]byte) uint64 { return 0 }
		}
		for i := range 64 {
			key := []byte{byte(i), 0, 255}
			id := signal.SeriesID{Hi: uint64(i), Lo: uint64(i + 1)}
			require.True(t, c.put(key, id, uint32(i), 1))
			clear(key)
		}
		for i := range 64 {
			key := []byte{byte(i), 0, 255}
			id, ref, current, found := c.lookup(key, 1)
			require.True(t, found)
			require.True(t, current)
			require.Equal(t, uint32(i), ref)
			require.Equal(t, signal.SeriesID{Hi: uint64(i), Lo: uint64(i + 1)}, id)
			_, _, current, found = c.lookup(key, 2)
			require.True(t, found)
			require.False(t, current)
			require.True(t, c.put(key, id, 999, 2))
			_, ref, current, found = c.lookup(key, 2)
			require.True(t, found)
			require.True(t, current)
			require.EqualValues(t, 999, ref)
		}
		_, _, _, found := c.lookup([]byte("absent"), 1)
		require.False(t, found)
		require.LessOrEqual(t, c.sizeBytes(), c.limit)
	}
	for _, limit := range []int64{0, 100, 128 << 10, 256 << 10} {
		c := newPackedCache(limit, 1<<20)
		for i := range 4096 {
			var key [8]byte
			binary.LittleEndian.PutUint64(key[:], uint64(i))
			c.put(key[:], signal.SeriesID{Lo: uint64(i)}, uint32(i), 1)
		}
		if len(c.slots) > 0 {
			require.LessOrEqual(t, c.sizeBytes(), limit)
		}
	}
}

func FuzzPackedCacheExactKeys(f *testing.F) {
	for _, key := range [][]byte{nil, []byte("metric"), {0, 255, 0}, bytes.Repeat([]byte("x"), 512)} {
		f.Add(key)
	}
	f.Fuzz(func(t *testing.T, key []byte) {
		if len(key) > 2048 {
			return
		}
		c := newPackedCache(256<<10, 128)
		c.hash = func([]byte) uint64 { return 1 }
		a := bytes.Clone(key)
		b := append(bytes.Clone(key), 0)
		idA := signal.HashBytes(a)
		idB := signal.HashBytes(b)
		if !c.put(a, idA, 3, 1) || !c.put(b, idB, 7, 1) {
			t.Fatal("admission failed")
		}
		got, ref, current, ok := c.lookup(a, 1)
		if !ok || !current || got != idA || ref != 3 {
			t.Fatal("key A resolved incorrectly")
		}
		got, ref, current, ok = c.lookup(b, 1)
		if !ok || !current || got != idB || ref != 7 {
			t.Fatal("key B resolved incorrectly")
		}
	})
}
