package engine

import (
	"math"
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

	gotTs, gotVal, _ := downsample(ts, vals, nil, nil)
	assert.Equal(t, ts, gotTs)
	assert.Equal(t, vals, gotVal)

	// A tier with a non-positive interval is inert.
	gotTs, gotVal, _ = downsample(ts, vals, nil, []DownsampleTier{{Before: 1 << 40, Interval: 0, Agg: signal.AggSum}})
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

	// The value-selecting aggregations emit the chosen sample; the summarizing ones the bucket start.
	cases := []struct {
		agg    signal.Aggregation
		wantTs []int64
		want   []float64
		wantSF []float64
	}{
		{signal.AggLast, []int64{15, 25, 105, 110}, []float64{3, 4, 5, 6}, nil},
		{signal.AggFirst, []int64{10, 25, 105, 110}, []float64{1, 4, 5, 6}, nil},
		{signal.AggMin, []int64{10, 25, 105, 110}, []float64{1, 4, 5, 6}, nil},
		{signal.AggMax, []int64{15, 25, 105, 110}, []float64{3, 4, 5, 6}, nil},
		{signal.AggSum, []int64{10, 20, 105, 110}, []float64{6, 4, 5, 6}, nil},
		{signal.AggAvg, []int64{10, 20, 105, 110}, []float64{2, 4, 5, 6}, []float64{3, 1, 1, 1}},
		{signal.AggCount, []int64{10, 20, 105, 110}, []float64{3, 1, 5, 6}, nil}, // raw samples keep their value
	}

	for _, c := range cases {
		t.Run(c.agg.String(), func(t *testing.T) {
			t.Parallel()

			gotTs, gotVal, gotSF := downsample(ts, vals, nil, tier(c.agg))
			assert.Equal(t, c.wantTs, gotTs)
			assert.Equal(t, c.want, gotVal)
			assert.Equal(t, c.wantSF, gotSF)
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

	gotTs, gotVal, _ := downsample(ts, vals, nil, tiers)
	assert.Equal(t, []int64{15, 30, 75, 120}, gotTs)
	assert.Equal(t, []float64{2, 3, 5, 6}, gotVal)
}

// TestDownsampleWidestTierWins checks a sample past several cutoffs lands in the widest tier however
// the cutoffs order, including when they tie.
func TestDownsampleWidestTierWins(t *testing.T) {
	t.Parallel()

	ts := []int64{10, 15, 30, 70, 75, 120}
	vals := []float64{1, 2, 3, 4, 5, 6}

	for _, tc := range []struct {
		name         string
		fine, coarse int64
		wantTs       []int64
		wantVal      []float64
	}{
		{name: "Tied", fine: 100, coarse: 100, wantTs: []int64{0, 20, 60, 120}, wantVal: []float64{3, 3, 9, 6}},
		{name: "Inverted", fine: 50, coarse: 100, wantTs: []int64{0, 20, 60, 120}, wantVal: []float64{3, 3, 9, 6}},
		{name: "Ordered", fine: 100, coarse: 50, wantTs: []int64{0, 20, 70, 120}, wantVal: []float64{3, 3, 9, 6}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tiers := []DownsampleTier{
				{Before: tc.fine, Interval: 10, Agg: signal.AggSum},
				{Before: tc.coarse, Interval: 20, Agg: signal.AggSum},
			}

			gotTs, gotVal, _ := downsample(ts, vals, nil, tiers)
			assert.Equal(t, tc.wantTs, gotTs)
			assert.Equal(t, tc.wantVal, gotVal)
		})
	}
}

func TestDownsampleNegativeTimestamps(t *testing.T) {
	t.Parallel()

	// alignDown must floor toward negative infinity: -25 and -22 share bucket -30 at interval 10.
	ts := []int64{-25, -22, -5}
	vals := []float64{1, 2, 3}
	tiers := []DownsampleTier{{Before: 0, Interval: 10, Agg: signal.AggSum}}

	gotTs, gotVal, _ := downsample(ts, vals, nil, tiers)
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
			ts1, val1, sf1 := downsample(ts, vals, nil, tiers)
			ts2, val2, sf2 := downsample(ts1, val1, sf1, tiers)
			assert.Equal(t, ts1, ts2, "timestamps stable under re-merge")
			assert.Equal(t, val1, val2, "values stable under re-merge")
			assert.Equal(t, sf1, sf2, "weights stable under re-merge")
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

// regroupTolerance bounds how far a Sum or Avg rolled up fine-then-coarse may land from the one-pass
// rollup of the same samples. Recursive summation of n terms errs by at most (n−1)·u·Σ|x| (u = 2⁻⁵³).
// Regrouping adds one rounding per stored representative, and Avg one more for the divide and one
// for re-weighting the mean, so with m ≤ n representatives both results lie within (n+3m)·u·Σ|x| of
// the true sum; 3n+4 machine epsilons (2u each) covers both, and an Avg, divided by a weight ≥ 1,
// errs by no more than its sum.
func regroupTolerance(vals, sf []float64) float64 {
	var abs float64

	for i, v := range vals {
		w := 1.0
		if sf != nil {
			w = sf[i]
		}

		abs += math.Abs(v * w)
	}

	return float64(3*len(vals)+4) * 0x1p-52 * abs
}

// TestDownsampleCoarseningCancellation pins the bound regrouping is held to: fine buckets [1e16] and
// [−1e16, 1] store −1e16 for the second (1e16 − 1 is not a float64), so the coarse Sum is 0 where one
// pass over the raw samples gives 1.
func TestDownsampleCoarseningCancellation(t *testing.T) {
	t.Parallel()

	ts := []int64{0, 10, 11}
	vals := []float64{1e16, -1e16, 1}

	for _, agg := range []signal.Aggregation{signal.AggSum, signal.AggAvg} {
		t.Run(agg.String(), func(t *testing.T) {
			t.Parallel()

			fine := []DownsampleTier{{Before: 100, Interval: 10, Agg: agg}}
			coarse := append(slices.Clone(fine), DownsampleTier{Before: 100, Interval: 20, Agg: agg})

			_, direct, _ := downsample(ts, vals, nil, coarse)
			fineTs, fineVals, fineSF := downsample(ts, vals, nil, fine)
			_, regrouped, _ := downsample(fineTs, fineVals, fineSF, coarse)

			want, lost := 1.0, 0.0
			if agg == signal.AggAvg {
				want = 1.0 / 3
			}

			assert.Equal(t, []float64{want}, direct, "one pass is compensated")
			assert.Equal(t, []float64{lost}, regrouped, "the stored representative already rounded the 1 away")
			assert.InDelta(t, direct[0], regrouped[0], regroupTolerance(vals, nil))
		})
	}
}

// FuzzDownsample asserts the structural invariants hold for arbitrary input, that every aggregation
// but Count is a fixed point under re-downsampling, and that coarsening a rollup equals rolling the
// raw samples up at the coarse interval once: exactly for the value-selecting aggregations, within
// [regroupTolerance] for Sum and Avg. Values span twenty decades and both signs, so sums cancel.
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
			vals = append(vals, float64(int8(b*37))*math.Pow10(i%20))
		}

		slices.Sort(ts)

		agg := signal.Aggregation(aggByte % 7)
		tiers := []DownsampleTier{{Before: before, Interval: interval, Agg: agg}}

		gotTs, gotVal, gotSF := downsample(ts, vals, nil, tiers)
		require.Len(t, gotVal, len(gotTs))
		require.LessOrEqual(t, len(gotTs), len(ts), "rollup never grows the series")
		require.True(t, slices.IsSorted(gotTs), "output is sorted")

		for i := 1; i < len(gotTs); i++ {
			require.NotEqual(t, gotTs[i-1], gotTs[i], "output timestamps are unique")
		}

		if agg == signal.AggCount || interval <= 0 {
			return
		}

		ts2, val2, sf2 := downsample(gotTs, gotVal, gotSF, tiers)
		require.Equal(t, gotTs, ts2, "fixed point")
		require.Equal(t, gotVal, val2, "fixed point")
		require.Equal(t, gotSF, sf2, "fixed point")

		if interval > 1<<40 {
			return // the coarse interval would overflow
		}

		coarse := append(slices.Clone(tiers), DownsampleTier{Before: before, Interval: interval * int64(2+aggByte%3), Agg: agg})
		wantTs, wantVal, wantSF := downsample(ts, vals, nil, coarse)
		gotTs, gotVal, gotSF = downsample(gotTs, gotVal, gotSF, coarse)
		require.Equal(t, wantTs, gotTs, "coarsening keeps the one-pass timestamps")
		require.Equal(t, wantSF, gotSF, "coarsening keeps the one-pass weights")

		if agg == signal.AggSum || agg == signal.AggAvg {
			require.InDeltaSlice(t, wantVal, gotVal, regroupTolerance(vals, nil), "coarsening is the one-pass rollup, regrouped")
		} else {
			require.Equal(t, wantVal, gotVal, "coarsening is the one-pass rollup")
		}
	})
}
