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

// appendBatch is a test helper that ingests parallel series/ts/value slices through AppendBatch.
func appendBatch(t *testing.T, e *engine.Engine, limits engine.AppendLimits, series []signal.Series, ts []int64, vals []float64) engine.AppendResult {
	t.Helper()

	ids := make([]signal.SeriesID, len(series))
	for i := range series {
		ids[i] = series[i].Hash()
	}

	res, err := e.AppendBatch(ids, ts, vals, func(i int) signal.Series { return series[i] }, limits)
	require.NoError(t, err)

	return res
}

func TestAppendBatchCardinalityLimit(t *testing.T) {
	t.Parallel()

	e := engine.New(engine.Config{})
	a, b, c := mkSeries("job", "a"), mkSeries("job", "b"), mkSeries("job", "c")

	// MaxSeries=2: the third distinct series is shed, the first two admitted.
	res := appendBatch(t, e, engine.AppendLimits{MaxSeries: 2},
		[]signal.Series{a, b, c}, []int64{1, 1, 1}, []float64{1, 2, 3})
	assert.Equal(t, 2, res.Accepted)
	assert.Equal(t, 1, res.RejectedCardinality)
	assert.Equal(t, 1, res.Rejected())
	assert.Equal(t, 2, e.SeriesCount())

	// An already-known series is never blocked by the cap, even at the limit.
	res = appendBatch(t, e, engine.AppendLimits{MaxSeries: 2},
		[]signal.Series{a, c}, []int64{2, 2}, []float64{10, 30})
	assert.Equal(t, 1, res.Accepted, "existing series a admitted")
	assert.Equal(t, 1, res.RejectedCardinality, "new series c still shed")
}

func TestAppendBatchInFlightBytesLimit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	e := engine.New(engine.Config{Backend: backend.Memory(), Prefix: "default/metrics"})
	s := mkSeries("job", "api")

	// Cap at two samples' worth of bytes; the third is shed (head already at the cap).
	limit := engine.AppendLimits{MaxInFlightBytes: 2 * engine.SampleBytes}
	res := appendBatch(t, e, limit, []signal.Series{s, s, s}, []int64{1, 2, 3}, []float64{1, 2, 3})
	assert.Equal(t, 2, res.Accepted)
	assert.Equal(t, 1, res.RejectedBytes)
	assert.Equal(t, int64(2*engine.SampleBytes), e.HeadBytes())

	// A flush drains the head, so the byte valve reopens.
	require.NoError(t, e.Flush(ctx))
	assert.Equal(t, int64(0), e.HeadBytes())

	res = appendBatch(t, e, limit, []signal.Series{s}, []int64{4}, []float64{4})
	assert.Equal(t, 1, res.Accepted, "flush reopened the valve")
}

func TestAppendBatchNoLimits(t *testing.T) {
	t.Parallel()

	e := engine.New(engine.Config{})
	a, b := mkSeries("job", "a"), mkSeries("job", "b")

	// Zero-value limits impose nothing.
	res := appendBatch(t, e, engine.AppendLimits{}, []signal.Series{a, b}, []int64{1, 1}, []float64{1, 2})
	assert.Equal(t, 2, res.Accepted)
	assert.Zero(t, res.Rejected())
	assert.Equal(t, int64(2*engine.SampleBytes), e.HeadBytes())

	got := fetchAll(t, e, fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("job", "a")}})
	require.Len(t, got, 1)
	assert.Equal(t, []float64{1}, got[0].Values)
}
