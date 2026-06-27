package engine_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"
)

// TestWALReplayAfterGroupedBatch covers the grouped-per-series WAL frames an AppendBatch logs: an
// interleaved multi-series, multi-sample batch must replay into the same engine state, proving the
// grouping (one WriteSamples per series) is equivalent to the old per-sample frames.
func TestWALReplayAfterGroupedBatch(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	sw, err := wal.Create(dir, 0)
	require.NoError(t, err)

	src := engine.New(engine.Config{WAL: sw})

	api, web := mkSeries("job", "api"), mkSeries("job", "web")
	series := []signal.Series{api, web, api, web, api} // interleaved, api seen 3×, web 2×
	ids := make([]signal.SeriesID, len(series))
	ts := []int64{100, 100, 200, 200, 300}
	values := []float64{1, 10, 2, 20, 3}
	for i, s := range series {
		ids[i] = s.Hash()
	}

	res, err := src.AppendBatch(ids, ts, values, nil, func(i int) signal.Series { return series[i] }, engine.AppendLimits{})
	require.NoError(t, err)
	require.Equal(t, 5, res.Accepted)
	require.NoError(t, sw.Close())

	restored := engine.New(engine.Config{})
	require.NoError(t, restored.Replay(dir))
	assert.Equal(t, 2, restored.SeriesCount())

	got := fetchAll(t, restored, fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("job", "api")}})
	require.Len(t, got, 1)
	assert.Equal(t, []int64{100, 200, 300}, got[0].Timestamps)
	assert.Equal(t, []float64{1, 2, 3}, got[0].Values)

	got = fetchAll(t, restored, fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("job", "web")}})
	require.Len(t, got, 1)
	assert.Equal(t, []int64{100, 200}, got[0].Timestamps)
	assert.Equal(t, []float64{10, 20}, got[0].Values)
}

// TestWALReplayRestoresScaleFactors covers the sampled-data crash-recovery path: a batch carrying
// lossy-sampling weights logs them to the WAL (the sf-carrying frame), and replay into a fresh
// engine restores those weights — not a flat weight 1 — so unflushed sampled data survives a crash.
func TestWALReplayRestoresScaleFactors(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	sw, err := wal.Create(dir, 0)
	require.NoError(t, err)

	src := engine.New(engine.Config{WAL: sw})

	s := mkSeries("job", "api")
	ids := []signal.SeriesID{s.Hash(), s.Hash(), s.Hash()}
	// Weights [1, 10, 1]: the middle sample represents 10 originals (budgeted sampling).
	_, err = src.AppendBatch(ids, []int64{1, 2, 3}, []float64{10, 20, 30}, []float64{1, 10, 1},
		func(int) signal.Series { return s }, engine.AppendLimits{})
	require.NoError(t, err)
	require.NoError(t, sw.Close())

	restored := engine.New(engine.Config{})
	require.NoError(t, restored.Replay(dir))

	got := fetchAll(t, restored, fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("job", "api")}})
	require.Len(t, got, 1)
	assert.Equal(t, []int64{1, 2, 3}, got[0].Timestamps)
	assert.Equal(t, []float64{10, 20, 30}, got[0].Values)
	assert.Equal(t, []float64{1, 10, 1}, got[0].ScaleFactors, "the representative weights survive crash recovery")
}

// TestWALReplayUnsampledNoScaleFactors confirms the unsampled path is untouched: a weight-1 batch
// logs the original (no-sf) frame and replays with a nil ScaleFactors column.
func TestWALReplayUnsampledNoScaleFactors(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	sw, err := wal.Create(dir, 0)
	require.NoError(t, err)

	src := engine.New(engine.Config{WAL: sw})
	s := mkSeries("job", "api")
	_, err = src.AppendBatch([]signal.SeriesID{s.Hash()}, []int64{1}, []float64{10}, nil,
		func(int) signal.Series { return s }, engine.AppendLimits{})
	require.NoError(t, err)
	require.NoError(t, sw.Close())

	restored := engine.New(engine.Config{})
	require.NoError(t, restored.Replay(dir))

	got := fetchAll(t, restored, fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("job", "api")}})
	require.Len(t, got, 1)
	assert.Nil(t, got[0].ScaleFactors, "unsampled data replays without a scale-factor column")
}

// TestWALBatchReusedAcrossAppends checks the reusable scratch is correctly reset between batches (no
// sample bleed from a prior batch).
func TestWALBatchReusedAcrossAppends(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	sw, err := wal.Create(dir, 0)
	require.NoError(t, err)

	src := engine.New(engine.Config{WAL: sw})
	api := mkSeries("job", "api")
	id := api.Hash()
	mat := func(int) signal.Series { return api }

	_, err = src.AppendBatch([]signal.SeriesID{id, id}, []int64{1, 2}, []float64{1, 2}, nil, mat, engine.AppendLimits{})
	require.NoError(t, err)
	_, err = src.AppendBatch([]signal.SeriesID{id}, []int64{3}, []float64{3}, nil, mat, engine.AppendLimits{})
	require.NoError(t, err)
	require.NoError(t, sw.Close())

	restored := engine.New(engine.Config{})
	require.NoError(t, restored.Replay(dir))

	got := fetchAll(t, restored, fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("job", "api")}})
	require.Len(t, got, 1)
	assert.Equal(t, []int64{1, 2, 3}, got[0].Timestamps, "second batch did not replay the first batch's samples")
}
