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
// Ranged reads are not part of this interface, but they are available as the optional
// [ReaderAt] capability: the multi-key layout gives projection pushdown (read only the
// referenced column objects), and [ReaderAt] gives the pushdown *within* a column that keeps a
// query touching a few granules from paying for the whole thing.
//
// [Backend.PutIfAbsent] and [Backend.CompareAndSwap] are the conditional-write primitives on
// which atomic manifest / block-list / bucket-index commits build, so multi-writer
// coordination over one prefix needs no Raft. PutIfAbsent claims an *absent* key
// (S3 If-None-Match, a filesystem exclusive create, a guarded map insert); CompareAndSwap
// replaces an *existing* one only if it still holds the version the committer read
// (S3 If-Match, a digest checked under the store's own lock).
type Backend interface {
	// IsEphemeral reports whether the backend stores data only in RAM (dropped on
	// process exit). [Memory] is ephemeral; file and s3 are not.
	IsEphemeral() bool

	// PutIfAbsent stores data under key only if the key does not already exist. It
	// returns true if the write happened, false if the key was already present (no
	// change). Like [Backend.Write] it is atomic per object. It is the compare-and-swap
	// primitive for manifest commits.
	PutIfAbsent(ctx context.Context, key string, data []byte) (bool, error)

	// CompareAndSwap stores data under key only if the key's current version is expected,
	// atomically. Pass [VersionAbsent] to demand that the key not exist yet, so a first commit
	// and every later one are the same call. It returns the version the stored data now has,
	// which the committer holds for its next commit without re-reading the object.
	//
	// A lost race is (false, nil), not an error: nothing failed and nothing is broken, the
	// caller simply holds a stale version and must reload and retry. Reporting it as an error
	// would put it on the path every caller already funnels into "the backend is unhealthy",
	// which is how a contended commit becomes an outage. A returned error means the operation
	// could not be evaluated at all, and says nothing about whether the write landed.
	//
	// Both failing cases are conditional, never destructive: an absent key with a version
	// expected, and a present key with [VersionAbsent] expected, both report (false, nil).
	CompareAndSwap(ctx context.Context, key string, expected Version, data []byte) (Version, bool, error)

	// ReadVersioned returns the value stored under key together with the [Version] identifying
	// it — the token a later [Backend.CompareAndSwap] conditions on. An absent key is
	// ([]byte(nil), [VersionAbsent], nil): absence is a version, and the value a first
	// committer conditions on. Errors are reserved for a backend that could not answer.
	ReadVersioned(ctx context.Context, key string) ([]byte, Version, error)

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

// IsNodeLocal reports true: the objects are this process's heap, unreachable by any peer.
func (*memoryBackend) IsNodeLocal() bool { return true }

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

// CompareAndSwap replaces the value under key while holding the same lock the version was read
// under, so no writer can slip in between the comparison and the store. The version is a digest
// of the stored bytes rather than a counter: it needs no second map, and it survives the map
// entry being replaced by an identical value.
func (m *memoryBackend) CompareAndSwap(
	_ context.Context, key string, expected Version, data []byte,
) (Version, bool, error) {
	cp := slices.Clone(data)
	if cp == nil {
		cp = []byte{}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	current := VersionAbsent
	if cur, ok := m.objects[key]; ok {
		current = ContentVersion(cur)
	}

	if current != expected {
		return VersionAbsent, false, nil
	}

	m.objects[key] = cp

	return ContentVersion(cp), true, nil
}

func (m *memoryBackend) ReadVersioned(_ context.Context, key string) ([]byte, Version, error) {
	m.mu.RLock()
	v, ok := m.objects[key]
	m.mu.RUnlock()

	if !ok {
		return nil, VersionAbsent, nil
	}

	return slices.Clone(v), ContentVersion(v), nil
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

// ReadAt returns a copy of the stored value's [off, off+n) range, clamped to its end. Implemented
// so a caller taking a small slice of a large object does not clone the object to get it — the same
// reason the durable backends range. Implements [ReaderAt].
func (m *memoryBackend) ReadAt(_ context.Context, key string, off, n int64) ([]byte, error) {
	m.mu.RLock()
	v, ok := m.objects[key]
	m.mu.RUnlock()

	if !ok {
		return nil, errors.Wrapf(ErrNotExist, "read %q", key)
	}

	return slices.Clone(clampRange(v, off, n)), nil
}

// ReadViewAt returns the range as a view of the stored value, no copy — safe for the same reason
// [memoryBackend.ReadView] is: stored values are immutable. Implements [ViewerAt].
func (m *memoryBackend) ReadViewAt(_ context.Context, key string, off, n int64) ([]byte, error) {
	m.mu.RLock()
	v, ok := m.objects[key]
	m.mu.RUnlock()

	if !ok {
		return nil, errors.Wrapf(ErrNotExist, "read %q", key)
	}

	return clampRange(v, off, n), nil
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
