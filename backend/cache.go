package backend

import (
	"context"
	"math"
	"slices"

	"github.com/maypok86/otter/v2"
	"github.com/maypok86/otter/v2/stats"
)

// Cached wraps a [Backend] with a bounded in-memory cache over read objects — the object-store read
// cache. It targets the cold tier (file/S3), where a part column is otherwise re-read over the
// network on every query: because part objects are write-once immutable, a cached value is never
// stale, so the only invalidation is eviction (by byte budget) and an explicit Write/Delete of the
// same key (manifest/index objects, which the wrapper keeps coherent). List and PutIfAbsent are
// passed through.
//
// maxBytes is the cache's total value-byte budget; objects larger than it are not cached (they
// would evict everything else). maxBytes ≤ 0 disables caching (the inner backend is returned
// unchanged). The wrapper preserves the [Backend] copy semantics: stored and returned slices are
// private copies, so a caller may retain or mutate them freely — except [cachedBackend.ReadView],
// which returns the resident value under the read-only [Viewer] contract.
//
// The cache is otter's weight-bounded loading cache rather than a strict LRU, for three reasons
// measured against a real corpus:
//
//   - Concurrent misses on the same key collapse into ONE inner read. A dashboard refresh or a
//     multi-shard read that touches the same part column previously issued one object-store GET per
//     in-flight query (64 concurrent readers of one object → 64 GETs; now 1).
//   - Scan resistance. Part objects are the access pattern a strict LRU handles worst: a historical
//     query streams cold objects once and flushes the hot working set, and a repeating scan slightly
//     larger than the budget evicts exactly the object about to be reused. Replaying a captured
//     query trace, LRU sat at a 2.9–23.7% hit rate below its working set where W-TinyLFU reached
//     30–59%, roughly halving the inner reads. Above the working set the order reverses slightly:
//     admission needs frequency evidence, so a cache that could hold everything gives up a few
//     points to a strict LRU.
//   - Hits scale with cores instead of serializing on one mutex and a list splice (16 cores: ~4.6
//     ns/op versus ~90 ns/op).
func Cached(inner Backend, maxBytes int64) Backend {
	if maxBytes <= 0 {
		return inner
	}

	c := &cachedBackend{
		inner:    inner,
		maxBytes: maxBytes,
		stats:    stats.NewCounter(),
	}

	c.values = otter.Must(&otter.Options[string, []byte]{
		MaximumWeight: uint64(maxBytes),
		Weigher:       weighValue,
		StatsRecorder: c.stats,
	})

	// Built once: converting a capturing closure to otter.LoaderFunc at the call site boxes it,
	// which is an allocation on every read — hits included.
	c.load = otter.LoaderFunc[string, []byte](func(ctx context.Context, key string) ([]byte, error) {
		return c.inner.Read(ctx, key)
	})

	// [ObjectCreator] is forwarded by a distinct type rather than a method on cachedBackend, so the
	// wrapper claims the capability only when the inner backend actually has it: a method would make
	// [StreamsWrites] report a streaming write over an inner that buffers.
	if _, ok := inner.(ObjectCreator); ok {
		return &cachedStreamBackend{cachedBackend: c}
	}

	return c
}

// cachedStreamBackend is [cachedBackend] over an inner backend that implements [ObjectCreator].
type cachedStreamBackend struct {
	*cachedBackend
}

var _ ObjectCreator = (*cachedStreamBackend)(nil)

// CreateObject forwards the incremental write and drops any stale entry under key once it commits.
// The written bytes are never cached: an object worth streaming is one the caller does not want
// resident, which is exactly what the cache would undo. Implements [ObjectCreator].
func (c *cachedStreamBackend) CreateObject(ctx context.Context, key string) (ObjectWriter, error) {
	w, err := CreateObject(ctx, c.inner, key)
	if err != nil {
		return nil, err
	}

	return &cacheInvalidatingWriter{ObjectWriter: w, cache: c.cachedBackend, key: key}, nil
}

// cacheInvalidatingWriter drops the cached value under key when the object it builds commits, so a
// reader is never served the superseded one.
type cacheInvalidatingWriter struct {
	ObjectWriter

	cache *cachedBackend
	key   string
}

func (w *cacheInvalidatingWriter) Commit(ctx context.Context) error {
	if err := w.ObjectWriter.Commit(ctx); err != nil {
		return err
	}

	w.cache.values.Invalidate(w.key)

	return nil
}

// weighValue prices an entry by its value bytes, which is what maxBytes bounds. The weight is
// capped rather than truncated: an object above 4 GiB must read as "heavier than any budget" so the
// policy drops it, not wrap around to a small weight and pin it resident.
func weighValue(_ string, v []byte) uint32 {
	if len(v) > math.MaxUint32 {
		return math.MaxUint32
	}

	return uint32(len(v))
}

// CacheStats is a snapshot of a cached backend's effectiveness.
type CacheStats struct {
	Hits, Misses int64
	Bytes        int64 // resident value bytes
	Items        int   // resident objects
}

type cachedBackend struct {
	inner    Backend
	maxBytes int64

	values *otter.Cache[string, []byte]
	load   otter.Loader[string, []byte]
	stats  *stats.Counter
}

func (c *cachedBackend) IsEphemeral() bool { return c.inner.IsEphemeral() }

// IsNodeLocal forwards the [NodeLocal] capability; without it a cached backend would look shared.
func (c *cachedBackend) IsNodeLocal() bool { return IsNodeLocal(c.inner) }

// FreeSpace forwards the [SpaceReporter] capability. Without this the wrapper would hide it and
// every cached backend would silently fall back to the merge ceiling.
func (c *cachedBackend) FreeSpace(ctx context.Context) (int64, error) {
	return FreeSpace(ctx, c.inner)
}

func (c *cachedBackend) Read(ctx context.Context, key string) ([]byte, error) {
	v, err := c.ReadView(ctx, key)
	if err != nil {
		return nil, err
	}

	return slices.Clone(v), nil
}

// ReadView is [Backend.Read] without the defensive copy: it returns the resident value itself, and
// on a miss the freshly read value that was just cached — zero copies either way. Safe under the
// [Viewer] contract: the caller never mutates the view, and the cache never mutates a resident
// value in place (a store replaces the entry's slice, so an outstanding view keeps reading the old,
// unreachable-from-the-cache array). This is the hot path for part column objects, where the
// clone-per-hit was a dominant query-path allocation. Implements [Viewer].
//
// Concurrent misses on one key share a single inner read; the loader's error is propagated, never
// cached, so the next call retries.
func (c *cachedBackend) ReadView(ctx context.Context, key string) ([]byte, error) {
	v, err := c.values.Get(ctx, key, c.load)
	if err != nil {
		return nil, err
	}

	// An object larger than the whole budget is not cached. The policy drops an over-weight entry
	// on its own, but only when it next runs maintenance — until then the entry is resident and
	// over budget, which for a part column can be hundreds of MiB. Dropping it here bounds that to
	// this call and keeps the rule the same on the read and write paths.
	if int64(len(v)) > c.maxBytes {
		c.values.Invalidate(key)
	}

	return v, nil
}

// ReadAt serves the range from a resident whole-object entry when there is one, and otherwise
// forwards the ranged read **without caching it**. Neither half may go through the loading path: it
// would fetch the whole object to answer a range, which is exactly the cost the range exists to
// avoid — and for a part column, one that does not fit the budget anyway. Implements [ReaderAt].
func (c *cachedBackend) ReadAt(ctx context.Context, key string, off, n int64) ([]byte, error) {
	if v, ok := c.values.GetIfPresent(key); ok {
		return slices.Clone(clampRange(v, off, n)), nil
	}

	return ReadAt(ctx, c.inner, key, off, n)
}

// ReadViewAt is [cachedBackend.ReadAt] without the copy, under the [Viewer] contract: a resident
// entry is never mutated in place, so a slice of one stays valid. Implements [ViewerAt].
func (c *cachedBackend) ReadViewAt(ctx context.Context, key string, off, n int64) ([]byte, error) {
	if v, ok := c.values.GetIfPresent(key); ok {
		return clampRange(v, off, n), nil
	}

	return ReadViewAt(ctx, c.inner, key, off, n)
}

func (c *cachedBackend) Write(ctx context.Context, key string, data []byte) error {
	if err := c.inner.Write(ctx, key, data); err != nil {
		return err
	}

	c.store(key, data) // keep the cache coherent with the newly written value

	return nil
}

func (c *cachedBackend) PutIfAbsent(ctx context.Context, key string, data []byte) (bool, error) {
	ok, err := c.inner.PutIfAbsent(ctx, key, data)
	if err != nil {
		return false, err
	}

	if ok {
		c.store(key, data)
	}

	return ok, nil
}

func (c *cachedBackend) Delete(ctx context.Context, key string) error {
	err := c.inner.Delete(ctx, key)
	c.values.Invalidate(key) // drop the entry whether or not the object existed

	return err
}

func (c *cachedBackend) List(ctx context.Context, prefix string) ([]string, error) {
	// Listings change as parts are added/removed; caching them would go stale. Pass through.
	return c.inner.List(ctx, prefix)
}

// Size delegates to the inner backend (via [SizeOf], so the inner Sizer fast path is preserved). A
// resident cache entry's length would also answer it, but most sized objects (columns) are large and
// not cached, so going to the inner backend is the simpler, consistent path. Implements [Sizer].
func (c *cachedBackend) Size(ctx context.Context, key string) (int64, error) {
	return SizeOf(ctx, c.inner, key)
}

// Stats returns a snapshot of cache effectiveness (for benchmarks and operator visibility). It
// settles pending maintenance first, so the resident figures reflect the writes already made rather
// than the policy's backlog.
func (c *cachedBackend) Stats() CacheStats {
	c.values.CleanUp()

	s := c.stats.Snapshot()

	return CacheStats{
		Hits:   int64(s.Hits),
		Misses: int64(s.Misses),
		Bytes:  int64(c.values.WeightedSize()),
		Items:  c.values.EstimatedSize(),
	}
}

// cached is how [WriteUncached]/[ReadUncached] recognize a cached backend: a plain type assertion
// would miss [cachedStreamBackend], which wraps the same cache in a second type.
type cached interface{ cache() *cachedBackend }

func (c *cachedBackend) cache() *cachedBackend { return c }

// WriteUncached writes through b, keeping data out of the read cache while still dropping any
// stale entry under key (a reader must never be served the superseded value). Use it for the few
// objects that are rewritten far more often than they are read — the engines' identity sets, which
// are written on every flush and read only on recovery: caching one is pure eviction pressure, and
// a single large one can occupy most of the budget. Every other object goes through [Backend.Write].
func WriteUncached(ctx context.Context, b Backend, key string, data []byte) error {
	cb, ok := b.(cached)
	if !ok {
		return b.Write(ctx, key, data)
	}

	c := cb.cache()
	if err := c.inner.Write(ctx, key, data); err != nil {
		return err
	}

	c.values.Invalidate(key)

	return nil
}

// ReadUncached reads key from b without caching the value (and without consulting the cache — the
// inner backend is the truth). It is the read half of [WriteUncached]: an object written uncached
// would otherwise land in the cache the first time it is read.
func ReadUncached(ctx context.Context, b Backend, key string) ([]byte, error) {
	if c, ok := b.(cached); ok {
		return c.cache().inner.Read(ctx, key)
	}

	return b.Read(ctx, key)
}

// store caches a private copy of data under key, taking a copy because the caller may reuse its
// buffer after Write returns. An object larger than the whole budget is not cached — and any stale
// smaller entry under that key is dropped, so a reader cannot be served the superseded value.
func (c *cachedBackend) store(key string, data []byte) {
	if int64(len(data)) > c.maxBytes {
		c.values.Invalidate(key)

		return
	}

	cp := slices.Clone(data)
	if cp == nil {
		cp = []byte{}
	}

	c.values.Set(key, cp)
}
