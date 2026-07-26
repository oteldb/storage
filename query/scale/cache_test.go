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

// recordBatch is a materialized record (log/trace) result: rows carry their data in Columns, not in
// Values.
func recordBatch() *fetch.Batch {
	return &fetch.Batch{
		ID:         signal.SeriesID{Lo: 7},
		Timestamps: []int64{10, 20},
		Columns: []fetch.NamedColumn{
			{Name: "severity", Bytes: [][]byte{[]byte("ERROR"), nil}},
			{Name: "body", Bytes: [][]byte{[]byte("boom"), []byte("ok")}},
			{Name: "code", Int64: []int64{500, 200}},
		},
	}
}

func TestMemoryCacheKeepsRecordColumns(t *testing.T) {
	t.Parallel()

	c := scale.NewMemoryCache(8)
	c.Put("k", []*fetch.Batch{recordBatch()})

	got, ok := c.Get("k")
	require.True(t, ok)
	require.Len(t, got, 1)

	// A record result whose columns were dropped comes back as the right number of rows with no
	// data at all — the failure this guards against.
	require.Len(t, got[0].Columns, 3, "cached record result kept its columns")

	body, ok := got[0].Column("body")
	require.True(t, ok)
	assert.Equal(t, [][]byte{[]byte("boom"), []byte("ok")}, body.Bytes)

	sev, ok := got[0].Column("severity")
	require.True(t, ok)
	assert.Nil(t, sev.Bytes[1], "an absent cell stays absent, not empty")

	code, ok := got[0].Column("code")
	require.True(t, ok)
	assert.Equal(t, []int64{500, 200}, code.Int64)
}

func TestMemoryCacheSnapshotsColumnsAgainstMutation(t *testing.T) {
	t.Parallel()

	c := scale.NewMemoryCache(8)
	original := recordBatch()
	c.Put("k", []*fetch.Batch{original})

	// The producing engine hands out cells that view a pooled blob it reuses, so the snapshot must
	// survive the producer overwriting them in place.
	copy(original.Columns[1].Bytes[0], "zzzz")
	original.Columns[2].Int64[0] = 999

	got, ok := c.Get("k")
	require.True(t, ok)

	body, ok := got[0].Column("body")
	require.True(t, ok)
	assert.Equal(t, []byte("boom"), body.Bytes[0], "cached cell is independent of the producer's buffer")

	code, ok := got[0].Column("code")
	require.True(t, ok)
	assert.Equal(t, int64(500), code.Int64[0])
}

func TestMemoryCacheKeepsScaleFactors(t *testing.T) {
	t.Parallel()

	c := scale.NewMemoryCache(8)
	b := sample(1, 10, 1)
	b.ScaleFactors = []float64{4}
	c.Put("k", []*fetch.Batch{b})

	b.ScaleFactors[0] = 999

	got, ok := c.Get("k")
	require.True(t, ok)

	// Dropping the weights would serve a sampled result as if every sample counted once.
	assert.Equal(t, []float64{4}, got[0].ScaleFactors)
	assert.InDelta(t, 4.0, got[0].ScaleFactor(0), 0)

	// A batch that was never sampled stays unsampled (nil), not weight-zero.
	c.Put("plain", []*fetch.Batch{sample(2, 10, 1)})
	plain, ok := c.Get("plain")
	require.True(t, ok)
	assert.Nil(t, plain[0].ScaleFactors)
	assert.InDelta(t, 1.0, plain[0].ScaleFactor(0), 0)
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

func TestCacheFreshnessGuardSkipsRecentWindow(t *testing.T) {
	t.Parallel()

	const now = int64(10_000)

	inner := &countingFetcher{batches: []*fetch.Batch{sample(1, 10, 1)}}
	cf := scale.CacheFetcher{
		Inner:     inner,
		Cache:     scale.NewMemoryCache(8),
		Freshness: 1000,
		Now:       func() int64 { return now },
	}

	m := []fetch.Matcher{eqMatcher("__name__", "cpu")}

	// A settled window (End well before now-Freshness=9000) is cached: the second call is a hit.
	settled := fetch.Request{Tenant: "t1", Start: 0, End: 5000, Matchers: m}
	_ = drainFetch(t, cf, settled)
	_ = drainFetch(t, cf, settled)
	assert.Equal(t, int64(1), inner.calls.Load(), "settled window is cached")

	// A window ending inside the freshness horizon (End=9500 > 9000) always bypasses the cache.
	recent := fetch.Request{Tenant: "t1", Start: 0, End: 9500, Matchers: m}
	_ = drainFetch(t, cf, recent)
	_ = drainFetch(t, cf, recent)
	assert.Equal(t, int64(3), inner.calls.Load(), "recent window is re-fetched every time")

	// Exactly on the horizon (End == now-Freshness) is settled and cacheable.
	boundary := fetch.Request{Tenant: "t1", Start: 0, End: now - 1000, Matchers: m}
	_ = drainFetch(t, cf, boundary)
	_ = drainFetch(t, cf, boundary)
	assert.Equal(t, int64(4), inner.calls.Load(), "the boundary window is cached")
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
