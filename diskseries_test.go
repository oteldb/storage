package storage

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

func TestDiskSeriesFacadeFlushReopen(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	dir := t.TempDir()
	open := func() *Storage {
		t.Helper()
		be, err := file.New(dir)
		require.NoError(t, err)
		s, err := Open(ctx, Options{MetricResidentHeadBytes: 1 << 20, FlushInterval: -1}, WithBackend(be), WithMetricDiskSeries())
		require.NoError(t, err)
		return s
	}
	s := open()
	for _, name := range []string{"one", "two"} {
		acc, err := s.WriteMetrics(ctx, gaugeBatch("api", name, []int64{100, 200}, []float64{1, 2}))
		require.NoError(t, err)
		require.EqualValues(t, 2, acc.Accepted)
	}
	eng, err := s.engineFor("default")
	require.NoError(t, err)
	require.NoError(t, eng.Flush(ctx))
	check := func(s *Storage) {
		t.Helper()
		stats, found := findSignal(mustTenant(t, s.Inspect(), "default"), signal.Metric)
		require.True(t, found)
		require.EqualValues(t, 2, stats.Series)
		require.True(t, stats.SeriesComplete)
		require.Zero(t, stats.HeadItems)
		cardinality := s.Cardinality("default", signal.Metric, 10)
		require.Equal(t, "resident_head", cardinality.IndexScope)
		require.True(t, cardinality.TotalSeriesComplete)
		require.Zero(t, cardinality.DistinctLabelNames)
		it, err := s.Fetcher("default").Fetch(ctx, fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{nameMatcher("one")}})
		require.NoError(t, err)
		rows, err := fetch.Drain(ctx, it)
		require.NoError(t, err)
		require.NoError(t, it.Close())
		require.Len(t, rows, 1)
		require.Equal(t, []float64{1, 2}, rows[0].Values)
	}
	check(s)
	require.NoError(t, s.Close(ctx))
	s = open()
	t.Cleanup(func() { require.NoError(t, s.Close(ctx)) })
	check(s)
}

func TestMetricResidentHeadBytesValidation(t *testing.T) {
	t.Parallel()
	for _, bytes := range []int64{-1, 0, 1 << 20} {
		o := Options{MetricResidentHeadBytes: bytes}
		if bytes < 0 {
			require.Error(t, o.validate())
		} else {
			require.NoError(t, o.validate())
		}
	}
}
