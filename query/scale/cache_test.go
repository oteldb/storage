package scale_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/query/scale"
	"github.com/oteldb/storage/signal"
)

// countingFetcher returns a fixed batch and counts how many times it is invoked.
type countingFetcher struct {
	calls   atomic.Int64
	batches []*fetch.Batch
}

func (f *countingFetcher) Fetch(context.Context, fetch.Request) (fetch.Iterator, error) {
	f.calls.Add(1)

	return fetch.NewSliceIterator(f.batches), nil
}

func eqMatcher(name, value string) fetch.Matcher {
	return fetch.Matcher{
		Name:  []byte(name),
		Match: func(signal.Value) bool { return true },
		Spec:  &fetch.EqualMatcher{Name: name, Value: value},
	}
}

func sample(id uint64, ts int64, v float64) *fetch.Batch {
	return &fetch.Batch{ID: signal.SeriesID{Lo: id}, Timestamps: []int64{ts}, Values: []float64{v}}
}

func TestCacheHitsAvoidInnerFetch(t *testing.T) {
	t.Parallel()

	inner := &countingFetcher{batches: []*fetch.Batch{sample(1, 10, 1)}}
	cf := scale.CacheFetcher{Inner: inner, Cache: scale.NewMemoryCache(8)}

	r := fetch.Request{Tenant: "t1", Start: 0, End: 100, Matchers: []fetch.Matcher{eqMatcher("__name__", "cpu")}}

	first := drainFetch(t, cf, r)
	require.Len(t, first, 1)
	assert.Equal(t, int64(1), inner.calls.Load(), "first request misses and hits inner")

	second := drainFetch(t, cf, r)
	require.Len(t, second, 1)
	assert.Equal(t, int64(1), inner.calls.Load(), "identical request served from cache")
	assert.Equal(t, first[0].Timestamps, second[0].Timestamps)
}

func TestCacheKeyDistinguishesWindowAndMatchers(t *testing.T) {
	t.Parallel()

	inner := &countingFetcher{batches: []*fetch.Batch{sample(1, 10, 1)}}
	cf := scale.CacheFetcher{Inner: inner, Cache: scale.NewMemoryCache(8)}

	base := fetch.Request{Tenant: "t1", Start: 0, End: 100, Matchers: []fetch.Matcher{eqMatcher("__name__", "cpu")}}
	_ = drainFetch(t, cf, base)

	diffWindow := base
	diffWindow.End = 200
	_ = drainFetch(t, cf, diffWindow)

	diffMatcher := base
	diffMatcher.Matchers = []fetch.Matcher{eqMatcher("__name__", "mem")}
	_ = drainFetch(t, cf, diffMatcher)

	diffTenant := base
	diffTenant.Tenant = "t2"
	_ = drainFetch(t, cf, diffTenant)

	assert.Equal(t, int64(4), inner.calls.Load(), "tenant, window, and matcher each key separately")
}

func TestCacheMatcherOrderIsStable(t *testing.T) {
	t.Parallel()

	inner := &countingFetcher{batches: []*fetch.Batch{sample(1, 10, 1)}}
	cf := scale.CacheFetcher{Inner: inner, Cache: scale.NewMemoryCache(8)}

	ab := fetch.Request{Tenant: "t1", Start: 0, End: 100, Matchers: []fetch.Matcher{
		eqMatcher("a", "1"), eqMatcher("b", "2"),
	}}
	ba := fetch.Request{Tenant: "t1", Start: 0, End: 100, Matchers: []fetch.Matcher{
		eqMatcher("b", "2"), eqMatcher("a", "1"),
	}}

	_ = drainFetch(t, cf, ab)
	_ = drainFetch(t, cf, ba)

	assert.Equal(t, int64(1), inner.calls.Load(), "matcher order does not change the cache key")
}

func TestCacheBypassesNonEqualityMatchers(t *testing.T) {
	t.Parallel()

	inner := &countingFetcher{batches: []*fetch.Batch{sample(1, 10, 1)}}
	cf := scale.CacheFetcher{Inner: inner, Cache: scale.NewMemoryCache(8)}

	// A matcher with no equality Spec (an opaque predicate) is not cacheable.
	r := fetch.Request{Tenant: "t1", Start: 0, End: 100, Matchers: []fetch.Matcher{
		{Name: []byte("x"), Match: func(signal.Value) bool { return true }},
	}}

	_ = drainFetch(t, cf, r)
	_ = drainFetch(t, cf, r)
	assert.Equal(t, int64(2), inner.calls.Load(), "non-cacheable requests always hit inner")
}

func TestMemoryCacheEvictsLRU(t *testing.T) {
	t.Parallel()

	c := scale.NewMemoryCache(2)
	c.Put("a", []*fetch.Batch{sample(1, 1, 1)})
	c.Put("b", []*fetch.Batch{sample(2, 1, 1)})

	_, ok := c.Get("a") // touch a ⇒ b is now least-recently-used
	require.True(t, ok)

	c.Put("c", []*fetch.Batch{sample(3, 1, 1)}) // evicts b
	assert.Equal(t, 2, c.Len())

	_, ok = c.Get("b")
	assert.False(t, ok, "b was evicted as least-recently-used")

	_, ok = c.Get("a")
	assert.True(t, ok, "a survived")

	_, ok = c.Get("c")
	assert.True(t, ok, "c is present")
}

func TestMemoryCacheSnapshotsAgainstMutation(t *testing.T) {
	t.Parallel()

	c := scale.NewMemoryCache(8)
	original := []*fetch.Batch{sample(1, 10, 1)}
	c.Put("k", original)

	// Mutating the producer's batch after Put must not change the cached snapshot.
	original[0].Timestamps[0] = 999

	got, ok := c.Get("k")
	require.True(t, ok)
	assert.Equal(t, []int64{10}, got[0].Timestamps, "cache holds an independent snapshot")

	// Reslicing the returned slice must not disturb the cache's stored order.
	got = got[:0]
	_ = got
	again, ok := c.Get("k")
	require.True(t, ok)
	require.Len(t, again, 1, "a caller reslicing its result does not shrink the cached entry")
}

func TestCacheNilCacheAndErrorPassThrough(t *testing.T) {
	t.Parallel()

	r := fetch.Request{Tenant: "t1", Start: 0, End: 100, Matchers: []fetch.Matcher{eqMatcher("__name__", "cpu")}}

	// A nil Cache is a transparent pass-through (every call reaches inner).
	inner := &countingFetcher{batches: []*fetch.Batch{sample(1, 10, 1)}}
	cf := scale.CacheFetcher{Inner: inner, Cache: nil}
	_ = drainFetch(t, cf, r)
	_ = drainFetch(t, cf, r)
	assert.Equal(t, int64(2), inner.calls.Load(), "nil cache never memoizes")

	// An inner error propagates and nothing is cached.
	ecf := scale.CacheFetcher{Inner: errFetcher{}, Cache: scale.NewMemoryCache(4)}
	_, err := ecf.Fetch(context.Background(), r)
	require.Error(t, err)
}

func TestSplitOverCacheCachesEachSubWindow(t *testing.T) {
	t.Parallel()

	// Compose the two decorators: SplitFetcher over CacheFetcher caches each aligned sub-window,
	// so an overlapping second query reuses the sub-windows the first one populated.
	inner := &windowFetcher{id: 7, ts: []int64{10, 110, 210, 310}, val: []float64{1, 2, 3, 4}}
	stack := scale.SplitFetcher{
		Inner:    scale.CacheFetcher{Inner: inner, Cache: scale.NewMemoryCache(16)},
		Interval: 100,
	}

	first := drainFetch(t, stack, fetch.Request{
		Start: 0, End: 399, Matchers: []fetch.Matcher{eqMatcher("__name__", "cpu")},
	})
	require.Len(t, first, 1)
	assert.Equal(t, []int64{10, 110, 210, 310}, first[0].Timestamps)
	firstCalls := inner.calls.Load()
	assert.Equal(t, int64(4), firstCalls, "four aligned sub-windows fetched")

	// Re-query a range overlapping the first three sub-windows: all are cached, no inner fetch.
	second := drainFetch(t, stack, fetch.Request{
		Start: 0, End: 299, Matchers: []fetch.Matcher{eqMatcher("__name__", "cpu")},
	})
	require.Len(t, second, 1)
	assert.Equal(t, []int64{10, 110, 210}, second[0].Timestamps)
	assert.Equal(t, firstCalls, inner.calls.Load(), "overlapping sub-windows served entirely from cache")
}
