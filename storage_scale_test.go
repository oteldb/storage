package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/metric"
)

// cacheableNameMatcher is nameMatcher with a serializable equality Spec, so a request carrying
// it is eligible for the Fetcher results cache.
func cacheableNameMatcher(name string) fetch.Matcher {
	m := nameMatcher(name)
	m.Spec = &fetch.EqualMatcher{Name: string(metric.LabelName), Value: name}

	return m
}

func fetchTimestamps(t *testing.T, f fetch.Fetcher, start, end int64, m fetch.Matcher) []int64 {
	t.Helper()
	it, err := f.Fetch(context.Background(), fetch.Request{Start: start, End: end, Matchers: []fetch.Matcher{m}})
	require.NoError(t, err)
	got, err := fetch.Drain(context.Background(), it)
	require.NoError(t, err)

	if len(got) == 0 {
		return nil
	}

	require.Len(t, got, 1)

	return got[0].Timestamps
}

func TestFacadeQuerySplitReturnsCorrectResults(t *testing.T) {
	t.Parallel()

	// Split width 100; samples span four sub-windows. The facade must fetch them concurrently
	// and merge back into one time-ordered series.
	s, err := InMemory(WithQuerySplitInterval(100))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	ts := []int64{10, 110, 150, 240, 390}
	_, err = s.WriteMetrics(context.Background(), gaugeBatch("api", "http.requests", ts, []float64{1, 2, 3, 4, 5}))
	require.NoError(t, err)

	got := fetchTimestamps(t, s.Fetcher("default"), 0, 399, nameMatcher("http.requests"))
	assert.Equal(t, ts, got, "split sub-windows merge into the full, ordered series")
}

func TestFacadeQueryCacheMemoizesByKey(t *testing.T) {
	t.Parallel()

	// With the cache on, an identical (tenant, window, equality) query is served from the cache.
	// Observe it by writing more data after the first query: the second identical query returns
	// the stale cached result (the cache does not auto-invalidate), proving the hit.
	s, err := InMemory(WithQueryCache(16))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	_, err = s.WriteMetrics(context.Background(), gaugeBatch("api", "http.requests", []int64{100}, []float64{1}))
	require.NoError(t, err)

	m := cacheableNameMatcher("http.requests")
	first := fetchTimestamps(t, s.Fetcher("default"), 0, 1000, m)
	require.Equal(t, []int64{100}, first)

	// Ingest another sample into the same window.
	_, err = s.WriteMetrics(context.Background(), gaugeBatch("api", "http.requests", []int64{200}, []float64{2}))
	require.NoError(t, err)

	cached := fetchTimestamps(t, s.Fetcher("default"), 0, 1000, m)
	assert.Equal(t, []int64{100}, cached, "identical query served from cache (stale, not re-fetched)")

	// A non-equality matcher (no Spec) bypasses the cache and sees fresh data.
	fresh := fetchTimestamps(t, s.Fetcher("default"), 0, 1000, nameMatcher("http.requests"))
	assert.Equal(t, []int64{100, 200}, fresh, "non-cacheable query reflects the new sample")
}

func TestFacadeQueryCacheScopedByTenant(t *testing.T) {
	t.Parallel()

	// Two tenants with the same series name + equality matcher must not share a cache entry:
	// the cache key carries the tenant scope.
	s, err := InMemory(WithQueryCache(16), WithTenant(func(r signal.Resource, _ signal.Scope) signal.TenantID {
		v, _ := r.Attributes.Get([]byte("service.name"))

		return signal.TenantID(v.Str())
	}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	_, err = s.WriteMetrics(context.Background(), gaugeBatch("team-a", "http.requests", []int64{100}, []float64{1}))
	require.NoError(t, err)
	_, err = s.WriteMetrics(context.Background(), gaugeBatch("team-b", "http.requests", []int64{100, 200}, []float64{7, 8}))
	require.NoError(t, err)

	m := cacheableNameMatcher("http.requests")

	a := fetchTimestamps(t, s.Fetcher("team-a"), 0, 1000, m)
	b := fetchTimestamps(t, s.Fetcher("team-b"), 0, 1000, m)
	assert.Equal(t, []int64{100}, a, "team-a's own data")
	assert.Equal(t, []int64{100, 200}, b, "team-b is not served team-a's cached result")
}

func TestFacadeSplitAndCacheCompose(t *testing.T) {
	t.Parallel()

	s, err := InMemory(WithQuerySplitInterval(100), WithQueryCache(32))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	ts := []int64{10, 110, 210, 310}
	_, err = s.WriteMetrics(context.Background(), gaugeBatch("api", "http.requests", ts, []float64{1, 2, 3, 4}))
	require.NoError(t, err)

	m := cacheableNameMatcher("http.requests")
	first := fetchTimestamps(t, s.Fetcher("default"), 0, 399, m)
	assert.Equal(t, ts, first, "split+cache returns the full series")

	// An overlapping narrower query reuses the cached sub-windows and stays correct.
	second := fetchTimestamps(t, s.Fetcher("default"), 0, 299, m)
	assert.Equal(t, []int64{10, 110, 210}, second, "overlapping sub-windows served from cache")
}
