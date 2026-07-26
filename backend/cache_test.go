package backend_test

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
)

// countingBackend counts inner Read calls so a test can prove cache hits never reach the backend.
type countingBackend struct {
	backend.Backend

	reads atomic.Int64
}

func newCounting() *countingBackend { return &countingBackend{Backend: backend.Memory()} }

func (c *countingBackend) Read(ctx context.Context, key string) ([]byte, error) {
	c.reads.Add(1)

	return c.Backend.Read(ctx, key)
}

func TestCacheDisabledReturnsInner(t *testing.T) {
	t.Parallel()

	inner := backend.Memory()
	assert.Same(t, inner, backend.Cached(inner, 0), "non-positive budget ⇒ no wrapper")
}

func TestCacheHitAvoidsInnerRead(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	inner := newCounting()
	c := backend.Cached(inner, 1<<20)
	require.NoError(t, c.Write(ctx, "k", []byte("value")))

	// Write is cached, so neither read touches the backend.
	for range 3 {
		v, err := c.Read(ctx, "k")
		require.NoError(t, err)
		assert.Equal(t, []byte("value"), v)
	}
	assert.Equal(t, int64(0), inner.reads.Load(), "cached-on-write key needs no inner read")

	// A key only ever read (not written through the cache) hits the backend exactly once.
	require.NoError(t, inner.Write(ctx, "z", []byte("zz")))
	for range 3 {
		_, err := c.Read(ctx, "z")
		require.NoError(t, err)
	}
	assert.Equal(t, int64(1), inner.reads.Load(), "first read misses, the rest hit the cache")
}

func TestCacheReturnsPrivateCopies(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	c := backend.Cached(backend.Memory(), 1<<20)
	require.NoError(t, c.Write(ctx, "k", []byte("abc")))

	got, err := c.Read(ctx, "k")
	require.NoError(t, err)
	got[0] = 'X' // mutating the returned slice must not corrupt the cache

	again, err := c.Read(ctx, "k")
	require.NoError(t, err)
	assert.Equal(t, []byte("abc"), again, "cached value is immutable to callers")
}

func TestCacheWriteOverwriteCoherent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	c := backend.Cached(backend.Memory(), 1<<20)
	require.NoError(t, c.Write(ctx, "k", []byte("v1")))
	require.NoError(t, c.Write(ctx, "k", []byte("v2")))

	v, err := c.Read(ctx, "k")
	require.NoError(t, err)
	assert.Equal(t, []byte("v2"), v, "cache reflects the latest write")
}

func TestCacheDeleteEvicts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	inner := newCounting()
	c := backend.Cached(inner, 1<<20)
	require.NoError(t, c.Write(ctx, "k", []byte("v")))
	_, err := c.Read(ctx, "k")
	require.NoError(t, err)

	require.NoError(t, c.Delete(ctx, "k"))
	_, err = c.Read(ctx, "k")
	require.ErrorIs(t, err, backend.ErrNotExist, "deleted key is gone, not served from cache")
}

func TestCacheEvictsByByteBudget(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	inner := newCounting()
	c := backend.Cached(inner, 100) // budget for ~10 ten-byte values
	cs, ok := c.(interface{ Stats() backend.CacheStats })
	require.True(t, ok)

	for i := range 50 {
		require.NoError(t, c.Write(ctx, "k"+strconv.Itoa(i), make([]byte, 10)))
	}
	assert.LessOrEqual(t, cs.Stats().Bytes, int64(100), "resident bytes stay within budget")

	// Most of the 50 keys are therefore not resident and fall through to the backend. Which ones
	// survive is the policy's choice — a strict LRU keeps the newest, W-TinyLFU keeps whichever it
	// has frequency evidence for — so assert the budget, not an eviction order.
	before := inner.reads.Load()
	for i := range 50 {
		_, err := c.Read(ctx, "k"+strconv.Itoa(i))
		require.NoError(t, err)
	}
	assert.GreaterOrEqual(t, inner.reads.Load()-before, int64(35),
		"a 100-byte budget holds ~10 of the 50 values; the rest miss")
}

func TestCacheSkipsOversizeObjects(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	inner := newCounting()
	c := backend.Cached(inner, 64)
	require.NoError(t, c.Write(ctx, "big", make([]byte, 1000))) // > budget ⇒ never cached

	for range 3 {
		_, err := c.Read(ctx, "big")
		require.NoError(t, err)
	}
	assert.Equal(t, int64(3), inner.reads.Load(), "oversize object is re-read every time")
}

func TestCacheStatsTracksHitsAndMisses(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	inner := backend.Memory()
	require.NoError(t, inner.Write(ctx, "k", []byte("v")))
	c := backend.Cached(inner, 1<<20)
	cs := c.(interface{ Stats() backend.CacheStats })

	_, _ = c.Read(ctx, "k") // miss
	_, _ = c.Read(ctx, "k") // hit
	_, _ = c.Read(ctx, "k") // hit

	s := cs.Stats()
	assert.Equal(t, int64(2), s.Hits)
	assert.Equal(t, int64(1), s.Misses)
}

func TestCacheListPassesThrough(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	c := backend.Cached(backend.Memory(), 1<<20)
	require.NoError(t, c.Write(ctx, "a/1", []byte("x")))
	require.NoError(t, c.Write(ctx, "a/2", []byte("y")))
	require.NoError(t, c.Write(ctx, "b/1", []byte("z")))

	keys, err := c.List(ctx, "a/")
	require.NoError(t, err)
	assert.Equal(t, []string{"a/1", "a/2"}, keys)
}

func TestCachePutIfAbsentCaches(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	inner := newCounting()
	c := backend.Cached(inner, 1<<20)

	ok, err := c.PutIfAbsent(ctx, "k", []byte("v"))
	require.NoError(t, err)
	require.True(t, ok)

	_, err = c.Read(ctx, "k")
	require.NoError(t, err)
	assert.Equal(t, int64(0), inner.reads.Load(), "a successful PutIfAbsent populates the cache")
}

func TestCacheConcurrent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	c := backend.Cached(backend.Memory(), 1<<16)

	var wg sync.WaitGroup
	for g := range 16 {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := range 200 {
				key := "k" + strconv.Itoa((g*200+i)%64)
				if i%4 == 0 {
					_ = c.Write(ctx, key, make([]byte, 128))
				} else {
					_, _ = c.Read(ctx, key)
				}
			}
		}(g)
	}
	wg.Wait()
}

// blockingBackend holds every inner read until the gate opens, so a burst of readers is guaranteed
// to overlap on the same cold key.
type blockingBackend struct {
	backend.Backend

	reads atomic.Int64
	gate  chan struct{}
}

func (b *blockingBackend) Read(ctx context.Context, key string) ([]byte, error) {
	b.reads.Add(1)
	<-b.gate

	return b.Backend.Read(ctx, key)
}

func TestCacheDedupsConcurrentMisses(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	const readers = 32

	inner := &blockingBackend{Backend: backend.Memory(), gate: make(chan struct{})}
	require.NoError(t, inner.Write(ctx, "k", []byte("value")))

	c := backend.Cached(inner, 1<<20)

	var wg, started sync.WaitGroup

	wg.Add(readers)
	started.Add(readers)

	for range readers {
		go func() {
			defer wg.Done()
			started.Done()

			v, err := backend.ReadView(ctx, c, "k")
			assert.NoError(t, err)
			assert.Equal(t, []byte("value"), v)
		}()
	}

	go func() {
		started.Wait()
		close(inner.gate) // release only once every reader is in flight
	}()

	wg.Wait()

	// The point of a loading cache: on an object store this is one GET instead of one per query.
	// The collapse is best-effort, not exclusive: the cache checks the map and then starts the
	// load, so a caller arriving in that window loads again. Harmless (a part object is immutable,
	// so a duplicate read returns identical bytes) — but it means the count is not deterministically
	// 1. It is 1 in the large majority of runs and a low single digit otherwise, so the bound here
	// is loose enough never to flake while still failing loudly if dedup regresses.
	assert.LessOrEqual(t, inner.reads.Load(), int64(readers/2),
		"concurrent misses on one key collapse to ~1 read, not %d", readers)
}

func TestCacheOversizeDoesNotDropWorkingSet(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	inner := newCounting()
	c := backend.Cached(inner, 1<<10)

	for i := range 4 {
		require.NoError(t, c.Write(ctx, "k"+strconv.Itoa(i), make([]byte, 64)))
	}

	// A single object above the whole budget must not take the resident set with it.
	require.NoError(t, inner.Write(ctx, "big", make([]byte, 4<<10)))
	_, err := c.Read(ctx, "big")
	require.NoError(t, err)

	before := inner.reads.Load()
	for i := range 4 {
		_, err := c.Read(ctx, "k"+strconv.Itoa(i))
		require.NoError(t, err)
	}
	assert.Equal(t, before, inner.reads.Load(), "the small working set survived the oversize read")
}

func TestCachedSize(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	c := backend.Cached(backend.Memory(), 1<<20)
	require.NoError(t, c.Write(ctx, "k", []byte("twelve bytes")))

	// Size delegates to the inner backend (via SizeOf) regardless of cache residency.
	n, err := backend.SizeOf(ctx, c, "k")
	require.NoError(t, err)
	assert.Equal(t, int64(len("twelve bytes")), n)

	_, err = backend.SizeOf(ctx, c, "absent")
	assert.ErrorIs(t, err, backend.ErrNotExist)
}

func TestCacheReadViewNoCopyOnHit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	inner := newCounting()
	c := backend.Cached(inner, 1<<20)
	require.NoError(t, c.Write(ctx, "k", []byte("value")))

	viewer, ok := c.(backend.Viewer)
	require.True(t, ok, "the cached backend exposes the read-only view capability")

	// Two hits return views of the same resident entry — no per-hit clone.
	v1, err := viewer.ReadView(ctx, "k")
	require.NoError(t, err)

	v2, err := viewer.ReadView(ctx, "k")
	require.NoError(t, err)

	assert.Equal(t, []byte("value"), v1)
	assert.Same(t, &v1[0], &v2[0], "hits share the resident entry's backing array")
	assert.Equal(t, int64(0), inner.reads.Load())
}

func TestCacheReadViewMissStoresAndStaysValidAfterOverwrite(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	inner := newCounting()
	c := backend.Cached(inner, 1<<20)
	require.NoError(t, inner.Write(ctx, "k", []byte("old")))

	// The miss reads through once, caches the value, and returns it as a view.
	v, err := backend.ReadView(ctx, c, "k")
	require.NoError(t, err)
	assert.Equal(t, []byte("old"), v)
	assert.Equal(t, int64(1), inner.reads.Load())

	hit, err := backend.ReadView(ctx, c, "k")
	require.NoError(t, err)
	assert.Same(t, &v[0], &hit[0], "the second read hits the entry stored by the miss")
	assert.Equal(t, int64(1), inner.reads.Load())

	// Overwriting replaces the entry's slice; the outstanding view keeps reading the old bytes.
	require.NoError(t, c.Write(ctx, "k", []byte("new")))
	assert.Equal(t, []byte("old"), v, "a retained view is immutable across overwrite")

	cur, err := backend.ReadView(ctx, c, "k")
	require.NoError(t, err)
	assert.Equal(t, []byte("new"), cur)
}

func TestReadViewFallsBackToRead(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// countingBackend embeds the Backend interface, so it does not implement Viewer itself; the
	// helper must fall back to a plain Read.
	inner := newCounting()
	require.NoError(t, inner.Write(ctx, "k", []byte("v")))

	v, err := backend.ReadView(ctx, inner, "k")
	require.NoError(t, err)
	assert.Equal(t, []byte("v"), v)
	assert.Equal(t, int64(1), inner.reads.Load())
}

func TestMemoryReadViewAliasesStore(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	m := backend.Memory()
	require.NoError(t, m.Write(ctx, "k", []byte("abc")))

	v1, err := backend.ReadView(ctx, m, "k")
	require.NoError(t, err)

	v2, err := backend.ReadView(ctx, m, "k")
	require.NoError(t, err)
	assert.Same(t, &v1[0], &v2[0], "memory views alias the stored object (no copy)")

	// A plain Read still returns a private copy.
	cp, err := m.Read(ctx, "k")
	require.NoError(t, err)
	assert.NotSame(t, &v1[0], &cp[0])
}
