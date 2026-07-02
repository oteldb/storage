package backend

import (
	"context"
	"slices"
	"strings"
	"sync"

	"github.com/go-faster/errors"
)

// Backend is the L1 storage seam (DESIGN.md §3, §5): a common interface over
// interchangeable implementations — memory (ephemeral, the reference), file, and
// (later) s3. The same engine code path runs over all three.
//
// Data is addressed by an opaque, slash-delimited string key (e.g. a time-bucketed
// object path or a file-relative path). Values are whole objects: the part format
// (`block`) maps one part to a key prefix and one object per column/marks/manifest,
// so whole-object Read/Write is sufficient and gives per-object write atomicity.
// All methods are safe for concurrent use.
//
// Ranged/streaming reads are deliberately not part of this interface: a part column is
// read whole, and the multi-key layout already gives projection pushdown (read only the
// referenced column objects) without ranged reads.
//
// [Backend.PutIfAbsent] is the conditional-write primitive (added in M5) on which atomic
// manifest / block-list commits build: a versioned manifest key is written only if no
// writer has claimed that version, so single-writer-wins coordination needs no Raft (it
// maps to S3 If-None-Match, a filesystem exclusive create, and a guarded map insert).
type Backend interface {
	// IsEphemeral reports whether the backend stores data only in RAM (dropped on
	// process exit). [Memory] is ephemeral; file and s3 are not.
	IsEphemeral() bool

	// PutIfAbsent stores data under key only if the key does not already exist. It
	// returns true if the write happened, false if the key was already present (no
	// change). Like [Backend.Write] it is atomic per object. It is the compare-and-swap
	// primitive for manifest commits.
	PutIfAbsent(ctx context.Context, key string, data []byte) (bool, error)

	// Write stores data under key, overwriting any existing value. The write is
	// atomic per object: a reader never observes a partially written value. The
	// implementation takes ownership semantics by copying data as needed; callers may
	// reuse the buffer after Write returns.
	Write(ctx context.Context, key string, data []byte) error

	// Read returns the value stored under key. It returns an error satisfying
	// errors.Is(err, [ErrNotExist]) if the key is absent. The returned slice is owned
	// by the caller (implementations return a fresh copy, never aliased state).
	Read(ctx context.Context, key string) ([]byte, error)

	// List returns, sorted ascending, every key with the given prefix (empty prefix
	// lists all keys).
	List(ctx context.Context, prefix string) ([]string, error)

	// Delete removes key. It returns an error satisfying errors.Is(err, [ErrNotExist])
	// if the key is absent.
	Delete(ctx context.Context, key string) error
}

// Viewer is an optional [Backend] capability: ReadView returns the value stored under key as a
// **read-only view** that may alias shared state (a cache entry, the in-memory store) instead of
// [Backend.Read]'s defensive copy. The caller MUST NOT mutate the returned slice; it MAY retain it
// indefinitely — a stored value is never mutated in place (Write/Delete replace or drop the map
// entry, they never rewrite the old array), so a view stays valid even after the key is
// overwritten, evicted, or deleted. It exists for hot read paths (part column objects, read once
// per query per column) where the copy is a measured allocation cost; use [ReadView] rather than
// asserting directly so callers stay correct over any backend.
type Viewer interface {
	// ReadView returns the value stored under key as a read-only view (do not mutate). Absent keys
	// error like [Backend.Read].
	ReadView(ctx context.Context, key string) ([]byte, error)
}

// ReadView returns key's value without a defensive copy when b implements [Viewer], falling back
// to a plain (caller-owned) Read otherwise. Either way the caller must treat the result as
// read-only — that is the contract that lets implementations skip the copy.
func ReadView(ctx context.Context, b Backend, key string) ([]byte, error) {
	if v, ok := b.(Viewer); ok {
		return v.ReadView(ctx, key)
	}

	return b.Read(ctx, key)
}

// Sizer is an optional [Backend] capability: report an object's stored byte size without reading
// its contents. Backends that can answer cheaply implement it (memory: the in-RAM length; file:
// os.Stat). Use [SizeOf] rather than asserting directly — it falls back to a full Read for backends
// that do not implement Sizer, so callers stay correct everywhere.
type Sizer interface {
	// Size returns the stored byte size of key, or an [ErrNotExist]-wrapping error if absent.
	Size(ctx context.Context, key string) (int64, error)
}

// SizeOf returns key's stored byte size. It uses the backend's [Sizer] fast path when available and
// otherwise falls back to reading the whole object and measuring it — so it is correct over any
// backend, and cheap over those (and the wrappers) that implement Sizer. It is intended for
// introspection (part byte accounting), not the hot path.
func SizeOf(ctx context.Context, b Backend, key string) (int64, error) {
	if s, ok := b.(Sizer); ok {
		return s.Size(ctx, key)
	}

	data, err := b.Read(ctx, key)
	if err != nil {
		return 0, err
	}

	return int64(len(data)), nil
}

// ErrNotExist is the sentinel returned (wrapped) by [Backend.Read] and [Backend.Delete]
// when a key is absent. Test for it with errors.Is.
var ErrNotExist = errors.New("backend: key does not exist")

// Memory returns an ephemeral in-memory [Backend] (DESIGN.md §5): the whole engine runs
// over it with no disk or object store; objects live in RAM and are dropped when the
// process exits. It is the reference implementation and the default in tests.
func Memory() Backend { return newMemory() }

// memoryBackend is the in-memory [Backend]: a concurrent map of key → value. Values are
// copied on Write and on Read so stored objects are immutable and never alias a caller's
// buffer (parts are immutable once written).
type memoryBackend struct {
	mu      sync.RWMutex
	objects map[string][]byte
}

func newMemory() *memoryBackend {
	return &memoryBackend{objects: make(map[string][]byte)}
}

func (*memoryBackend) IsEphemeral() bool { return true }

func (m *memoryBackend) Write(_ context.Context, key string, data []byte) error {
	cp := slices.Clone(data)
	if cp == nil {
		// Distinguish "stored empty value" from "absent" without a nil map entry.
		cp = []byte{}
	}

	m.mu.Lock()
	m.objects[key] = cp
	m.mu.Unlock()

	return nil
}

func (m *memoryBackend) PutIfAbsent(_ context.Context, key string, data []byte) (bool, error) {
	cp := slices.Clone(data)
	if cp == nil {
		cp = []byte{}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.objects[key]; ok {
		return false, nil
	}

	m.objects[key] = cp

	return true, nil
}

func (m *memoryBackend) Read(_ context.Context, key string) ([]byte, error) {
	m.mu.RLock()
	v, ok := m.objects[key]
	m.mu.RUnlock()

	if !ok {
		return nil, errors.Wrapf(ErrNotExist, "read %q", key)
	}

	return slices.Clone(v), nil
}

// ReadView returns the stored value itself (no copy). Safe because stored values are immutable:
// Write/PutIfAbsent insert private copies and only ever replace the map entry. Implements [Viewer].
func (m *memoryBackend) ReadView(_ context.Context, key string) ([]byte, error) {
	m.mu.RLock()
	v, ok := m.objects[key]
	m.mu.RUnlock()

	if !ok {
		return nil, errors.Wrapf(ErrNotExist, "read %q", key)
	}

	return v, nil
}

func (m *memoryBackend) List(_ context.Context, prefix string) ([]string, error) {
	m.mu.RLock()

	keys := make([]string, 0, len(m.objects))
	for k := range m.objects {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}

	m.mu.RUnlock()
	slices.Sort(keys)

	return keys, nil
}

func (m *memoryBackend) Size(_ context.Context, key string) (int64, error) {
	m.mu.RLock()
	v, ok := m.objects[key]
	m.mu.RUnlock()

	if !ok {
		return 0, errors.Wrapf(ErrNotExist, "size %q", key)
	}

	return int64(len(v)), nil
}

func (m *memoryBackend) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	_, ok := m.objects[key]
	if ok {
		delete(m.objects, key)
	}
	m.mu.Unlock()

	if !ok {
		return errors.Wrapf(ErrNotExist, "delete %q", key)
	}

	return nil
}
