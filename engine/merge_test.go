package engine_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

func TestMergeCompactsParts(t *testing.T) {
	t.Parallel()

	b := backend.Memory()
	e := engine.New(engine.Config{Backend: b, Prefix: "default/metrics"})
	s := mkSeries("job", "api")

	mustAppend(t, e, s, 100, 1.0)
	require.NoError(t, e.Flush(context.Background()))
	mustAppend(t, e, s, 200, 2.0)
	require.NoError(t, e.Flush(context.Background()))
	mustAppend(t, e, s, 300, 3.0)
	require.NoError(t, e.Flush(context.Background()))
	assert.Equal(t, 3, e.PartCount())

	require.NoError(t, e.Merge(context.Background(), 0))
	assert.Equal(t, 1, e.PartCount(), "three parts compact to one")

	// The merged part has been written; the three source parts are gone from the backend.
	keys, err := b.List(context.Background(), "default/metrics")
	require.NoError(t, err)
	for _, k := range keys {
		assert.NotContains(t, k, "/0000000000/", "source part 0 deleted")
		assert.NotContains(t, k, "/0000000001/", "source part 1 deleted")
		assert.NotContains(t, k, "/0000000002/", "source part 2 deleted")
	}

	got := fetchAll(t, e, fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("job", "api")}})
	require.Len(t, got, 1)
	assert.Equal(t, []int64{100, 200, 300}, got[0].Timestamps)
	assert.Equal(t, []float64{1, 2, 3}, got[0].Values)
}

func TestMergeNoopBelowTwoParts(t *testing.T) {
	t.Parallel()

	e := flushEngine()
	mustAppend(t, e, mkSeries("job", "api"), 100, 1.0)
	require.NoError(t, e.Flush(context.Background()))

	require.NoError(t, e.Merge(context.Background(), 0), "one part, no retention ⇒ no-op")
	assert.Equal(t, 1, e.PartCount())

	// And with zero parts.
	empty := flushEngine()
	require.NoError(t, empty.Merge(context.Background(), 0))
	assert.Equal(t, 0, empty.PartCount())
}

func TestMergeRetentionDropsOldSamples(t *testing.T) {
	t.Parallel()

	e := flushEngine()
	s := mkSeries("job", "api")
	mustAppend(t, e, s, 100, 1.0)
	mustAppend(t, e, s, 200, 2.0)
	require.NoError(t, e.Flush(context.Background()))
	mustAppend(t, e, s, 300, 3.0)
	require.NoError(t, e.Flush(context.Background()))

	// Retain only ts >= 200.
	require.NoError(t, e.Merge(context.Background(), 200))
	assert.Equal(t, 1, e.PartCount())

	got := fetchAll(t, e, fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("job", "api")}})
	require.Len(t, got, 1)
	assert.Equal(t, []int64{200, 300}, got[0].Timestamps, "ts=100 dropped by retention")
	assert.Equal(t, []float64{2, 3}, got[0].Values)
}

func TestMergeRetentionDropsAllParts(t *testing.T) {
	t.Parallel()

	e := flushEngine()
	s := mkSeries("job", "api")
	mustAppend(t, e, s, 100, 1.0)
	require.NoError(t, e.Flush(context.Background()))
	mustAppend(t, e, s, 200, 2.0)
	require.NoError(t, e.Flush(context.Background()))

	// Cutoff past every sample drops all parts.
	require.NoError(t, e.Merge(context.Background(), 1000))
	assert.Equal(t, 0, e.PartCount())

	got := fetchAll(t, e, fetch.Request{Start: 0, End: 10000, Matchers: []fetch.Matcher{eqMatcher("job", "api")}})
	assert.Empty(t, got, "no samples remain")
}

func TestMergeSinglePartWithRetention(t *testing.T) {
	t.Parallel()

	e := flushEngine()
	s := mkSeries("job", "api")
	mustAppend(t, e, s, 100, 1.0)
	mustAppend(t, e, s, 200, 2.0)
	require.NoError(t, e.Flush(context.Background()))

	// One part but a retention cutoff ⇒ still compacts to apply retention.
	require.NoError(t, e.Merge(context.Background(), 200))
	assert.Equal(t, 1, e.PartCount())

	got := fetchAll(t, e, fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("job", "api")}})
	require.Len(t, got, 1)
	assert.Equal(t, []int64{200}, got[0].Timestamps)
}

func TestMergeMultipleSeries(t *testing.T) {
	t.Parallel()

	e := flushEngine()
	mustAppend(t, e, mkSeries("job", "api"), 100, 1.0)
	mustAppend(t, e, mkSeries("job", "web"), 100, 2.0)
	require.NoError(t, e.Flush(context.Background()))
	mustAppend(t, e, mkSeries("job", "api"), 200, 11.0)
	mustAppend(t, e, mkSeries("job", "db"), 200, 3.0)
	require.NoError(t, e.Flush(context.Background()))

	require.NoError(t, e.Merge(context.Background(), 0))
	assert.Equal(t, 1, e.PartCount())

	api := fetchAll(t, e, fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("job", "api")}})
	require.Len(t, api, 1)
	assert.Equal(t, []float64{1, 11}, api[0].Values)

	for _, name := range []string{"web", "db"} {
		got := fetchAll(t, e, fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("job", name)}})
		require.Len(t, got, 1, name)
	}
}

func TestCloseFlushesHead(t *testing.T) {
	t.Parallel()

	e := flushEngine()
	mustAppend(t, e, mkSeries("job", "api"), 100, 1.0)
	require.NoError(t, e.Close(context.Background()))
	assert.Equal(t, 1, e.PartCount(), "Close flushes the head")
}

// TestMergeEventuallyCompactsStrays is #285 end-to-end: parts of geometrically increasing size —
// what tiered compaction leaves behind, one per size class — must not sit uncompacted forever.
// Repeated maintenance ticks with no other work must eventually fold them together.
func TestMergeEventuallyCompactsStrays(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	e := engine.New(engine.Config{Backend: backend.Memory(), Prefix: "default/metrics"})

	// Each flush is ~4x the previous, so no two parts are close in size.
	base := int64(0)
	for _, samples := range []int{1, 4, 16, 64, 256} {
		ids, series, ts, vals := seriesWithSamples(samples, base)
		_, err := e.AppendBatch(ids, ts, vals, nil, func(i int) signal.Series { return series[i] }, engine.AppendLimits{})
		require.NoError(t, err)
		require.NoError(t, e.Flush(ctx))

		base += int64(samples)
	}

	require.Equal(t, 5, e.PartCount(), "five parts of unlike size")

	// Idle ticks: the write-amplification guard holds at first, then gives way rather than
	// stranding them. Comfortably more ticks than the engine's waive threshold.
	for range 8 {
		require.NoError(t, e.Merge(ctx, 0))
	}

	assert.Less(t, e.PartCount(), 5, "strays must eventually merge")

	// Every sample is still readable afterwards.
	got := fetchAll(t, e, fetch.Request{Start: 0, End: 1 << 62})
	assert.NotEmpty(t, got)
}

// seriesWithSamples builds one series carrying n samples starting at tsBase.
func seriesWithSamples(n int, tsBase int64) ([]signal.SeriesID, []signal.Series, []int64, []float64) {
	s := mkSeries("__name__", "m", "job", "api")

	ids := make([]signal.SeriesID, n)
	series := make([]signal.Series, n)
	ts := make([]int64, n)
	vals := make([]float64, n)

	for i := range n {
		ids[i], series[i] = s.Hash(), s
		ts[i] = tsBase + int64(i)
		vals[i] = float64(i)
	}

	return ids, series, ts, vals
}
