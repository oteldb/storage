package engine

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

//nolint:paralleltest,tparallel // Subtests reuse one scratch instance in sequence.
func TestRollupScratchReuse(t *testing.T) {
	t.Parallel()

	var scratch rollupScratch
	for _, tc := range []struct {
		name  string
		runs  []sourceRun
		tiers []DownsampleTier
	}{
		{"duplicates", []sourceRun{rawRun([]int64{1, 1, 2}, []float64{10, 11, 20}, nil)}, nil},
		{"weighted", []sourceRun{rawRun([]int64{3, 4}, []float64{30, 40}, []float64{2, 3})}, nil},
		{"empty", nil, nil},
		{"overlapping", []sourceRun{
			rawRun([]int64{1, 3, 5}, []float64{10, 30, 50}, nil),
			rawRun([]int64{2, 3, 6}, []float64{20, 31, 60}, nil),
		}, nil},
		{"recorded", []sourceRun{
			rolledRun([]int64{10}, []float64{7}, nil, tierOf(signal.AggCount)),
			rawRun([]int64{10, 11}, []float64{20, 21}, nil),
		}, nil},
		{"rollup", []sourceRun{rawRun([]int64{1, 2, 3}, []float64{10, 20, 30}, nil)}, tierOf(signal.AggSum)},
		{"shorter", []sourceRun{rawRun([]int64{9}, []float64{90}, nil)}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var m sampleMerge
			for _, r := range tc.runs {
				m.add(r.run.ts, r.run.vals, r.run.sf, r.layout, minInt64, maxInt64)
			}
			wantTS, wantValues, wantSF, wantCovered := rollSeries(&m, tc.tiers)
			before := make([]tsRun, len(tc.runs))
			for i, r := range tc.runs {
				before[i] = tsRun{slices.Clone(r.run.ts), slices.Clone(r.run.vals), slices.Clone(r.run.sf)}
			}
			ts, values, sf, covered := scratch.roll(&m, tc.tiers)
			require.True(t, slices.Equal(wantTS, ts))
			require.True(t, slices.Equal(wantValues, values))
			require.Equal(t, wantSF, sf)
			require.Equal(t, wantCovered, covered)
			for i, r := range tc.runs {
				require.Equal(t, before[i], r.run, "source arrays must remain immutable")
			}
		})
	}
}

func TestSampleMergeReset(t *testing.T) {
	t.Parallel()
	var m sampleMerge
	m.add([]int64{1}, []float64{10}, nil, tierOf(signal.AggSum), minInt64, maxInt64)
	m.ambiguous = 1
	capacity := cap(m.runs)
	m.reset()
	require.Empty(t, m.runs)
	require.Equal(t, capacity, cap(m.runs))
	require.Nil(t, m.layouts)
	require.Zero(t, m.ambiguous)
	for _, r := range m.runs[:capacity] {
		require.Equal(t, tsRun{}, r)
	}
	m.add([]int64{2}, []float64{20}, nil, nil, minInt64, maxInt64)
	require.Nil(t, m.layouts)
	ts, values, sf := m.collect(nil, nil)
	require.Equal(t, []int64{2}, ts)
	require.Equal(t, []float64{20}, values)
	require.Nil(t, sf)
}
