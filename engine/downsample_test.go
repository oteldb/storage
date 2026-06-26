package engine

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

func TestDownsampleNoTiers(t *testing.T) {
	t.Parallel()

	ts := []int64{1, 2, 3}
	vals := []float64{10, 20, 30}

	gotTs, gotVal := downsample(ts, vals, nil)
	assert.Equal(t, ts, gotTs)
	assert.Equal(t, vals, gotVal)

	// A tier with a non-positive interval is inert.
	gotTs, gotVal = downsample(ts, vals, []DownsampleTier{{Before: 1 << 40, Interval: 0, Agg: signal.AggSum}})
	assert.Equal(t, ts, gotTs)
	assert.Equal(t, vals, gotVal)
}

// TestDownsampleAggregations pins each aggregation over one fixed input: three samples in the
// [10] bucket, one in [20], and two raw samples newer than the Before cutoff.
func TestDownsampleAggregations(t *testing.T) {
	t.Parallel()

	ts := []int64{10, 12, 15, 25, 105, 110}
	vals := []float64{1, 2, 3, 4, 5, 6}
	tier := func(a signal.Aggregation) []DownsampleTier {
		return []DownsampleTier{{Before: 100, Interval: 10, Agg: a}}
	}

	cases := []struct {
		agg  signal.Aggregation
		want []float64
	}{
		{signal.AggLast, []float64{3, 4, 5, 6}},  // bucket10 last=ts15, bucket20=4, raw 5,6
		{signal.AggFirst, []float64{1, 4, 5, 6}}, // bucket10 first=ts10
		{signal.AggMin, []float64{1, 4, 5, 6}},
		{signal.AggMax, []float64{3, 4, 5, 6}},
		{signal.AggSum, []float64{6, 4, 5, 6}},   // 1+2+3
		{signal.AggAvg, []float64{2, 4, 5, 6}},   // (1+2+3)/3
		{signal.AggCount, []float64{3, 1, 5, 6}}, // raw samples keep their value
	}

	for _, c := range cases {
		t.Run(c.agg.String(), func(t *testing.T) {
			t.Parallel()

			gotTs, gotVal := downsample(ts, vals, tier(c.agg))
			assert.Equal(t, []int64{10, 20, 105, 110}, gotTs)
			assert.Equal(t, c.want, gotVal)
		})
	}
}

// TestDownsampleMultiTier checks age-banded coarsening: the oldest samples land in the coarse
// (Before 50, Interval 20) tier, mid samples in the fine (Before 100, Interval 10) tier, and the
// newest sample stays raw.
func TestDownsampleMultiTier(t *testing.T) {
	t.Parallel()

	ts := []int64{10, 15, 30, 70, 75, 120}
	vals := []float64{1, 2, 3, 4, 5, 6}
	tiers := []DownsampleTier{
		{Before: 100, Interval: 10, Agg: signal.AggLast}, // fine, mid-age
		{Before: 50, Interval: 20, Agg: signal.AggLast},  // coarse, oldest (order intentionally reversed)
	}

	gotTs, gotVal := downsample(ts, vals, tiers)
	assert.Equal(t, []int64{0, 20, 70, 120}, gotTs)
	assert.Equal(t, []float64{2, 3, 5, 6}, gotVal)
}

func TestDownsampleNegativeTimestamps(t *testing.T) {
	t.Parallel()

	// alignDown must floor toward negative infinity: -25 and -22 share bucket -30 at interval 10.
	ts := []int64{-25, -22, -5}
	vals := []float64{1, 2, 3}
	tiers := []DownsampleTier{{Before: 0, Interval: 10, Agg: signal.AggSum}}

	gotTs, gotVal := downsample(ts, vals, tiers)
	assert.Equal(t, []int64{-30, -10}, gotTs)
	assert.Equal(t, []float64{3, 3}, gotVal) // (-25,-22)→bucket-30 sum 3; (-5)→bucket-10 sum 3
}

// TestDownsampleIdempotent verifies that re-downsampling an already-rolled-up series with the same
// tiers is a no-op for every aggregation except Count (a one-sample bucket aggregates to itself).
func TestDownsampleIdempotent(t *testing.T) {
	t.Parallel()

	ts := []int64{10, 12, 15, 25, 33, 48, 105}
	vals := []float64{1, 2, 3, 4, 5, 6, 7}

	for _, agg := range []signal.Aggregation{
		signal.AggLast, signal.AggFirst, signal.AggMin, signal.AggMax, signal.AggSum, signal.AggAvg,
	} {
		t.Run(agg.String(), func(t *testing.T) {
			t.Parallel()

			tiers := []DownsampleTier{{Before: 100, Interval: 10, Agg: agg}}
			ts1, val1 := downsample(ts, vals, tiers)
			ts2, val2 := downsample(ts1, val1, tiers)
			assert.Equal(t, ts1, ts2, "timestamps stable under re-merge")
			assert.Equal(t, val1, val2, "values stable under re-merge")
		})
	}
}

func TestDownsampleApplies(t *testing.T) {
	t.Parallel()

	tiers := []DownsampleTier{{Before: 100, Interval: 10, Agg: signal.AggLast}}
	assert.True(t, downsampleApplies(tiers, 50), "min older than Before ⇒ applies")
	assert.False(t, downsampleApplies(tiers, 100), "min at Before ⇒ nothing strictly older")
	assert.False(t, downsampleApplies(tiers, 200), "all data newer than Before")
	assert.False(t, downsampleApplies([]DownsampleTier{{Before: 100, Interval: 0}}, 0), "disabled tier")
}

func TestAlignDown(t *testing.T) {
	t.Parallel()

	assert.Equal(t, int64(10), alignDown(15, 10))
	assert.Equal(t, int64(10), alignDown(10, 10))
	assert.Equal(t, int64(0), alignDown(9, 10))
	assert.Equal(t, int64(-10), alignDown(-1, 10))
	assert.Equal(t, int64(-10), alignDown(-10, 10))
	assert.Equal(t, int64(-20), alignDown(-11, 10))
}

// FuzzDownsample asserts the structural invariants hold for arbitrary input and that AggLast is a
// fixed point under re-downsampling.
func FuzzDownsample(f *testing.F) {
	f.Add([]byte{1, 2, 3, 4, 5, 6}, int64(100), int64(10), uint8(0))
	f.Add([]byte{0, 0, 9, 9}, int64(5), int64(3), uint8(4))

	f.Fuzz(func(t *testing.T, raw []byte, before, interval int64, aggByte uint8) {
		// Build a sorted, unique timestamp series (downsample's precondition) from the bytes.
		seen := map[int64]struct{}{}
		var ts []int64
		var vals []float64

		for i, b := range raw {
			v := int64(b)
			if _, ok := seen[v]; ok {
				continue
			}

			seen[v] = struct{}{}
			ts = append(ts, v)
			vals = append(vals, float64(i))
		}

		slices.Sort(ts)

		agg := signal.Aggregation(aggByte % 7)
		tiers := []DownsampleTier{{Before: before, Interval: interval, Agg: agg}}

		gotTs, gotVal := downsample(ts, vals, tiers)
		require.Len(t, gotVal, len(gotTs))
		require.LessOrEqual(t, len(gotTs), len(ts), "rollup never grows the series")
		require.True(t, slices.IsSorted(gotTs), "output is sorted")

		for i := 1; i < len(gotTs); i++ {
			require.NotEqual(t, gotTs[i-1], gotTs[i], "output timestamps are unique")
		}

		if agg == signal.AggLast {
			ts2, val2 := downsample(gotTs, gotVal, tiers)
			require.Equal(t, gotTs, ts2, "AggLast is a fixed point")
			require.Equal(t, gotVal, val2, "AggLast is a fixed point")
		}
	})
}
