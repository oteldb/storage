package storage

import (
	"context"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// servicesOf projects enumerated identities to their sorted service.name values.
func servicesOf(t *testing.T, series []signal.Series) []string {
	t.Helper()

	out := make([]string, 0, len(series))
	for i := range series {
		v, ok := series[i].Resource.Attributes.Get([]byte("service.name"))
		require.True(t, ok, "identity must carry service.name")
		out = append(out, string(v.Str()))
	}

	slices.Sort(out)

	return out
}

// TestMetricSeries covers the metrics enumeration facade: matcher scoping, the window filter, and
// the zero start AND end convention it shares with [Storage.LogSeries].
func TestMetricSeries(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	_, err = s.WriteMetrics(ctx, gaugeBatch("api", "http.requests", []int64{100, 200}, []float64{1, 2}))
	require.NoError(t, err)
	_, err = s.WriteMetrics(ctx, gaugeBatch("web", "http.requests", []int64{500}, []float64{3}))
	require.NoError(t, err)
	_, err = s.WriteMetrics(ctx, gaugeBatch("api", "cpu.seconds", []int64{100}, []float64{4}))
	require.NoError(t, err)

	matchers := []fetch.Matcher{nameMatcher("http.requests")}

	all, err := s.MetricSeries(ctx, "default", matchers, 0, 0)
	require.NoError(t, err)
	assert.Equal(t, []string{"api", "web"}, servicesOf(t, all), "zero window ⇒ no time filter")

	windowed, err := s.MetricSeries(ctx, "default", matchers, 400, 600)
	require.NoError(t, err)
	assert.Equal(t, []string{"web"}, servicesOf(t, windowed), "only web has a sample in [400,600]")

	unmatched, err := s.MetricSeries(ctx, "default", []fetch.Matcher{nameMatcher("nope")}, 0, 0)
	require.NoError(t, err)
	assert.Empty(t, unmatched)

	// Every series of the tenant, the shape a label endpoint without match[] issues.
	every, err := s.MetricSeries(ctx, "default", nil, 0, 0)
	require.NoError(t, err)
	assert.Equal(t, []string{"api", "api", "web"}, servicesOf(t, every))

	unknown, err := s.MetricSeries(ctx, "no-such-tenant", nil, 0, 0)
	require.NoError(t, err)
	assert.Empty(t, unknown, "a tenant with no data enumerates empty, not an error")
}

// TestMetricSeriesSurvivesFlush pins that enumeration sees flushed parts (the series index outlives
// a flush) and still honors the window against a part's time range.
func TestMetricSeriesSurvivesFlush(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	_, err = s.WriteMetrics(ctx, gaugeBatch("api", "http.requests", []int64{100}, []float64{1}))
	require.NoError(t, err)
	require.NoError(t, mustEngine(s.engineFor("default")).Flush(ctx))

	_, err = s.WriteMetrics(ctx, gaugeBatch("web", "http.requests", []int64{500}, []float64{2}))
	require.NoError(t, err)

	matchers := []fetch.Matcher{nameMatcher("http.requests")}

	all, err := s.MetricSeries(ctx, "default", matchers, 0, 0)
	require.NoError(t, err)
	assert.Equal(t, []string{"api", "web"}, servicesOf(t, all), "part ∪ head")

	part, err := s.MetricSeries(ctx, "default", matchers, 50, 150)
	require.NoError(t, err)
	assert.Equal(t, []string{"api"}, servicesOf(t, part), "only the flushed series is in the window")
}

// TestFetcherExposesSeriesLister guards the capability discovery through the storage read seam: the
// PromQL label endpoints reach the engine's enumeration through the decorators [Storage.Fetcher]
// wraps it in (seed, scope, split, cache), instead of falling back to draining samples.
func TestFetcherExposesSeriesLister(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := InMemory(WithQuerySplitInterval(1_000), WithQueryCache(32))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	_, err = s.WriteMetrics(ctx, gaugeBatch("api", "http.requests", []int64{100}, []float64{1}))
	require.NoError(t, err)

	lister := fetch.SeriesListerOf(s.Fetcher("default"))
	require.NotNil(t, lister, "the decorated fetcher must expose the enumeration capability")

	got, err := lister.Series(ctx, fetch.Request{Start: 0, End: 1 << 60})
	require.NoError(t, err)
	assert.Equal(t, []string{"api"}, servicesOf(t, got))
}
