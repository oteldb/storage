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

// sampledEngine holds four samples of one series weighted [1, 10, 1, 5]: 17 originals stand behind
// the 4 stored rows, summing to 10 + 10·20 + 30 + 5·40 = 440 against a raw sum of 100.
const (
	scaleWantCount = 17.0
	scaleWantSum   = 440.0
	scaleWantRows  = 4
)

func sampledEngine(t *testing.T) (*engine.Engine, signal.Series) {
	t.Helper()

	e := engine.New(engine.Config{Backend: backend.Memory(), Prefix: "default/metrics"})
	s := mkSeries("job", "api")
	mat := func(int) signal.Series { return s }
	ids := []signal.SeriesID{s.Hash(), s.Hash(), s.Hash(), s.Hash()}

	_, err := e.AppendBatch(ids,
		[]int64{10, 20, 30, 40},
		[]float64{10, 20, 30, 40},
		[]float64{1, 10, 1, 5}, mat, engine.AppendLimits{})
	require.NoError(t, err)

	return e, s
}

func scaleReq() fetch.Request {
	return fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("job", "api")}}
}

func assertScaled(t *testing.T, a engine.SeriesAgg, where string) {
	t.Helper()

	assert.InDeltaf(t, scaleWantCount, a.Count, 1e-9, "%s: count is Σ weights, not stored rows", where)
	assert.InDeltaf(t, scaleWantSum, a.Sum, 1e-9, "%s: sum is Σ weight·value", where)
	assert.Equalf(t, int64(scaleWantRows), a.Rows, "%s: rows stays the unweighted row count", where)
	assert.InDeltaf(t, 10, a.Min, 0, "%s: an extremum does not depend on multiplicity", where)
	assert.InDeltaf(t, 40, a.Max, 0, "%s", where)
}

// TestAggregateRangeHonorsScaleFactors is the acceptance test for issue #570: a sampled tenant's
// aggregate pushdown must report the originals its kept rows stand for, at every tier the fold can
// come from — the head, a flushed part's decode, and a merged part's.
func TestAggregateRangeHonorsScaleFactors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	e, s := sampledEngine(t)

	got, err := e.AggregateRange(ctx, scaleReq())
	require.NoError(t, err)
	assertScaled(t, got[s.Hash()], "head")

	require.NoError(t, e.Flush(ctx))

	got, err = e.AggregateRange(ctx, scaleReq())
	require.NoError(t, err)
	assertScaled(t, got[s.Hash()], "flushed part")

	// A second part and a merge: the merged part is still sampled, so it carries no stats sidecar
	// and the fold decodes its weight column.
	_, err = e.AppendBatch([]signal.SeriesID{s.Hash()}, []int64{50}, []float64{50}, []float64{2},
		func(int) signal.Series { return s }, engine.AppendLimits{})
	require.NoError(t, err)
	require.NoError(t, e.Flush(ctx))
	require.NoError(t, e.Merge(ctx, 0))
	require.Equal(t, 1, e.PartCount())

	got, err = e.AggregateRange(ctx, scaleReq())
	require.NoError(t, err)

	a := got[s.Hash()]
	assert.InDelta(t, scaleWantCount+2, a.Count, 1e-9, "merged part: count is Σ weights")
	assert.InDelta(t, scaleWantSum+100, a.Sum, 1e-9, "merged part: sum is Σ weight·value")
	assert.Equal(t, int64(scaleWantRows+1), a.Rows)
}

// TestAggregateStepHonorsScaleFactors covers the bucketed path — the step grid folds weights the
// same way, and a bucket's aggregate is weighted whether it comes from a decode or a sidecar.
func TestAggregateStepHonorsScaleFactors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	e, s := sampledEngine(t)
	require.NoError(t, e.Flush(ctx))

	got, err := e.AggregateStep(ctx, scaleReq(), 1000)
	require.NoError(t, err)
	require.Len(t, got[s.Hash()], 1, "one bucket wide enough to hold every sample")
	assertScaled(t, got[s.Hash()][0].SeriesAgg, "single step bucket")

	// Per-sample buckets: each carries its own weight, and none is rounded to an integer.
	got, err = e.AggregateStep(ctx, scaleReq(), 10)
	require.NoError(t, err)

	buckets := got[s.Hash()]
	require.Len(t, buckets, 4)

	for i, want := range []float64{1, 10, 1, 5} {
		assert.InDeltaf(t, want, buckets[i].Count, 1e-9, "bucket %d count", i)
		assert.Equalf(t, int64(1), buckets[i].Rows, "bucket %d rows", i)
	}
}

// TestAggregateWindowHonorsScaleFactors covers the sliding accumulator, which adds and subtracts
// counts as windows move — the path where a weighted count could drift.
func TestAggregateWindowHonorsScaleFactors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	e, s := sampledEngine(t)
	require.NoError(t, e.Flush(ctx))

	got, err := e.AggregateWindow(ctx, scaleReq(), engine.WindowSpec{Step: 1000, Window: 1000})
	require.NoError(t, err)
	require.Len(t, got[s.Hash()], 1)
	assertScaled(t, got[s.Hash()][0].SeriesAgg, "single window")

	// Narrow windows that slide past every sample: after the last one leaves, no window is
	// reported — the emptiness test is the integer row count, so no residue survives the
	// subtraction of fractional weights.
	got, err = e.AggregateWindow(ctx, scaleReq(), engine.WindowSpec{Step: 10, Window: 10})
	require.NoError(t, err)

	var (
		count float64
		rows  int64
	)

	for _, w := range got[s.Hash()] {
		count += w.Count
		rows += w.Rows
	}

	assert.InDelta(t, scaleWantCount, count, 1e-9, "the weights survive a slide over disjoint windows")
	assert.Equal(t, int64(scaleWantRows), rows)
}

// TestAggregateUnsampledUsesSidecar pins the unsampled path the stats sidecar serves: every weight
// is 1, so Count and Rows agree and the sidecar needs no weight of its own.
func TestAggregateUnsampledUsesSidecar(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	e := aggEngine()
	s := mkSeries("job", "api")

	for i := range int64(4) {
		mustAppend(t, e, s, (i+1)*10, float64((i+1)*10))
	}

	require.NoError(t, e.Flush(ctx))

	got, err := e.AggregateRange(ctx, scaleReq())
	require.NoError(t, err)

	a := got[s.Hash()]
	assert.InDelta(t, 4, a.Count, 0)
	assert.Equal(t, int64(4), a.Rows, "an unsampled aggregate's count and row count are the same number")
	assert.InDelta(t, 100, a.Sum, 0)
}
