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

// recentTierEngine builds an in-memory engine with the recent tier on and a helper that appends one
// sample.
func recentTierEngine(t *testing.T, window int64) (*engine.Engine, signal.Series, func(ts int64, val float64)) {
	t.Helper()

	e := engine.New(engine.Config{
		Backend:      backend.Memory(),
		Prefix:       "default/metrics",
		RecentWindow: window,
	})

	s := mkSeries("__name__", "cpu", "host", "h1")
	id := s.Hash()

	return e, s, func(ts int64, val float64) {
		t.Helper()

		_, err := e.AppendBatch(
			[]signal.SeriesID{id}, []int64{ts}, []float64{val}, nil,
			func(int) signal.Series { return s }, engine.AppendLimits{},
		)
		require.NoError(t, err)
	}
}

// TestRecentTierAggregateRange is the reproducer for issue #471 (a): AggregateRange must see the
// samples the recent tier holds. planFetch deliberately acquires no parts for an in-window query
// because the tier answers it, so an aggregate that skips the tier sees nothing at all.
func TestRecentTierAggregateRange(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	e, _, appendOne := recentTierEngine(t, 60*1e9)

	for i := int64(1); i <= 5; i++ {
		appendOne(i*100, float64(i)*100)
		require.NoError(t, e.Flush(ctx))
	}

	got, err := e.AggregateRange(ctx, fetch.Request{Start: 100, End: 1 << 62})
	require.NoError(t, err)
	require.Len(t, got, 1, "the matched series must aggregate")

	for _, agg := range got {
		assert.InDelta(t, 5, agg.Count, 0)
		assert.InDelta(t, 1500.0, agg.Sum, 1e-9)
		assert.InDelta(t, 100.0, agg.Min, 1e-9)
		assert.InDelta(t, 500.0, agg.Max, 1e-9)
	}
}

// TestRecentTierAggregateStep is the reproducer for issue #471 (a) on the step-grid path: the same
// samples must bucket, and the grid must be sized to include them.
func TestRecentTierAggregateStep(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	e, _, appendOne := recentTierEngine(t, 60*1e9)

	for i := int64(1); i <= 5; i++ {
		appendOne(i*100, float64(i)*100)
		require.NoError(t, e.Flush(ctx))
	}

	got, err := e.AggregateStep(ctx, fetch.Request{Start: 100, End: 1 << 62}, 200)
	require.NoError(t, err)
	require.Len(t, got, 1, "the matched series must bucket")

	for _, buckets := range got {
		var (
			count float64
			sum   float64
		)

		for _, b := range buckets {
			count += b.Count
			sum += b.Sum
		}

		assert.InDelta(t, 5, count, 1e-9)
		assert.InDelta(t, 1500.0, sum, 1e-9)
	}
}

// TestRecentTierAggregateWindow is the reproducer for issue #471 (a) on the overlapping-window
// path, which shares bucketSeries and the grid sizing with AggregateStep.
func TestRecentTierAggregateWindow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	e, _, appendOne := recentTierEngine(t, 60*1e9)

	for i := int64(1); i <= 5; i++ {
		appendOne(i*100, float64(i)*100)
		require.NoError(t, e.Flush(ctx))
	}

	got, err := e.AggregateWindow(ctx, fetch.Request{Start: 100, End: 500},
		engine.WindowSpec{Step: 100, Window: 100})
	require.NoError(t, err)
	require.Len(t, got, 1, "the matched series must produce windows")

	var count float64
	for _, windows := range got {
		for _, w := range windows {
			count += w.Count
		}
	}

	assert.InDelta(t, 5, count, 1e-9)
}

// TestRecentTierAggregateMatchesFetch is the invariant behind (a): whatever a raw fetch returns for
// a window, the aggregate paths must fold exactly that — the tier must never make one API see data
// another cannot.
func TestRecentTierAggregateMatchesFetch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	e, _, appendOne := recentTierEngine(t, 60*1e9)

	for i := int64(1); i <= 5; i++ {
		appendOne(i*100, float64(i)*100)
		require.NoError(t, e.Flush(ctx))
	}

	appendOne(600, 600) // an unflushed head sample too

	for _, start := range []int64{0, 100, 250, 500} {
		r := fetch.Request{Start: start, End: 1 << 62}

		batches := fetchAll(t, e, r)

		var (
			wantCount float64
			wantSum   float64
		)

		// The weighted fold of what a raw fetch returns — what the aggregate paths must reproduce.
		for _, b := range batches {
			for i, v := range b.Values {
				w := b.ScaleFactor(i)
				wantCount += w
				wantSum += w * v
			}
		}

		agg, err := e.AggregateRange(ctx, r)
		require.NoError(t, err)

		var (
			gotCount float64
			gotSum   float64
		)

		for _, a := range agg {
			gotCount += a.Count
			gotSum += a.Sum
		}

		assert.InDelta(t, wantCount, gotCount, 1e-9, "AggregateRange count matches fetch at start=%d", start)
		assert.InDelta(t, wantSum, gotSum, 1e-9, "AggregateRange sum matches fetch at start=%d", start)

		step, err := e.AggregateStep(ctx, r, 150)
		require.NoError(t, err)

		var (
			stepCount float64
			stepSum   float64
		)

		for _, buckets := range step {
			for _, b := range buckets {
				stepCount += b.Count
				stepSum += b.Sum
			}
		}

		assert.InDelta(t, wantCount, stepCount, 1e-9, "AggregateStep count matches fetch at start=%d", start)
		assert.InDelta(t, wantSum, stepSum, 1e-9, "AggregateStep sum matches fetch at start=%d", start)
	}
}

// TestRecentTierDuplicateTimestampNotDoubleCounted covers the drift the recent tier makes reachable
// on the pushdown path: a re-append of an already-flushed timestamp leaves the same timestamp in
// both the tier and the head. A raw fetch dedups it freshest-wins; the aggregate fold must reach the
// same answer rather than counting the sample twice.
func TestRecentTierDuplicateTimestampNotDoubleCounted(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	e, _, appendOne := recentTierEngine(t, 60*1e9)

	appendOne(100, 1)
	require.NoError(t, e.Flush(ctx))
	appendOne(100, 5) // same timestamp, now live in the tier and in the head

	r := fetch.Request{Start: 100, End: 1 << 62}

	got := fetchAll(t, e, r)
	require.Len(t, got, 1)
	require.Equal(t, []int64{100}, got[0].Timestamps, "fetch dedups by timestamp")
	require.Equal(t, []float64{5}, got[0].Values, "freshest wins")

	agg, err := e.AggregateRange(ctx, r)
	require.NoError(t, err)
	require.Len(t, agg, 1)

	for _, a := range agg {
		assert.InDelta(t, 1, a.Count, 0)
		assert.InDelta(t, 5.0, a.Sum, 1e-9)
	}
}

// TestRecentTierScaleFactorsNilWhenUnsampled is the reproducer for issue #471 (b): an unsampled
// buffer promoted into the tier must keep the nil-means-weight-1 convention, not materialize a
// slice of zeros that a weight-honoring consumer would multiply by.
func TestRecentTierScaleFactorsNilWhenUnsampled(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	e, _, appendOne := recentTierEngine(t, 60*1e9)

	for i := int64(1); i <= 5; i++ {
		appendOne(i*100, float64(i)*100)
	}

	require.NoError(t, e.Flush(ctx))

	got := fetchAll(t, e, fetch.Request{Start: 100, End: 1 << 62})
	require.Len(t, got, 1)
	assert.Equal(t, []int64{100, 200, 300, 400, 500}, got[0].Timestamps)
	assert.Nil(t, got[0].ScaleFactors, "unsampled tier data carries no weights")
}

// TestRecentTierScaleFactorsPreserved guards the other half of (b): a genuinely sampled weight must
// survive the trip through the tier.
func TestRecentTierScaleFactorsPreserved(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	e := engine.New(engine.Config{
		Backend:      backend.Memory(),
		Prefix:       "default/metrics",
		RecentWindow: 60 * 1e9,
	})

	s := mkSeries("__name__", "cpu", "host", "h1")
	id := s.Hash()

	_, err := e.AppendBatch(
		[]signal.SeriesID{id, id}, []int64{100, 200}, []float64{1, 2}, []float64{4, 4},
		func(int) signal.Series { return s }, engine.AppendLimits{},
	)
	require.NoError(t, err)
	require.NoError(t, e.Flush(ctx))

	got := fetchAll(t, e, fetch.Request{Start: 100, End: 1 << 62})
	require.Len(t, got, 1)
	assert.Equal(t, []float64{4, 4}, got[0].ScaleFactors)
}

// TestRecentTierResetClearsTier is the reproducer for issue #471 (c): Reset must drop the tier
// along with the head and the parts, or pre-Reset samples reappear on the next append.
func TestRecentTierResetClearsTier(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	e, _, appendOne := recentTierEngine(t, 60*1e9)

	appendOne(100, 1)
	require.NoError(t, e.Flush(ctx))

	require.NoError(t, e.Reset(ctx))

	appendOne(200, 2)

	got := fetchAll(t, e, fetch.Request{Start: 0, End: 1 << 62})
	require.Len(t, got, 1)
	assert.Equal(t, []int64{200}, got[0].Timestamps, "pre-Reset samples must not survive Reset")
	assert.Equal(t, []float64{2}, got[0].Values)

	agg, err := e.AggregateRange(ctx, fetch.Request{Start: 0, End: 1 << 62})
	require.NoError(t, err)

	for _, a := range agg {
		assert.InDelta(t, 1, a.Count, 0, "aggregate sees only post-Reset samples")
	}
}
