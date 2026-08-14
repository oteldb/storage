package engine

import (
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"
)

// collectManyNaive is the straightforward k-way merge collectMany replaced: one row per iteration,
// scanning every run twice. It is the oracle the fast path is checked against.
func collectManyNaive(runs []tsRun) (ts []int64, values, sf []float64) {
	total := 0
	for i := range runs {
		total += len(runs[i].ts)
	}

	cur := make([]int, len(runs))

	for {
		minTs := int64(0)
		found := false

		for i := range runs {
			if cur[i] < len(runs[i].ts) {
				if t := runs[i].ts[cur[i]]; !found || t < minTs {
					minTs, found = t, true
				}
			}
		}

		if !found {
			return ts, values, sf
		}

		var winVal, winW float64 = 0, 1

		for i := range runs {
			if cur[i] < len(runs[i].ts) && runs[i].ts[cur[i]] == minTs {
				winVal, winW = runs[i].vals[cur[i]], runs[i].weight(cur[i])
				cur[i]++
			}
		}

		ts = append(ts, minTs)
		values = append(values, winVal)
		sf = appendWeight(sf, winW, len(values), total)
	}
}

func run(ts []int64, vals, sf []float64) tsRun { return tsRun{ts: ts, vals: vals, sf: sf} }

func TestCollectMany(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		runs    []tsRun
		wantTS  []int64
		wantVal []float64
		wantSF  []float64
	}{
		{
			name: "disjoint runs concatenate in time order",
			runs: []tsRun{
				run([]int64{1, 2, 3}, []float64{1, 2, 3}, nil),
				run([]int64{4, 5}, []float64{4, 5}, nil),
			},
			wantTS:  []int64{1, 2, 3, 4, 5},
			wantVal: []float64{1, 2, 3, 4, 5},
		},
		{
			name: "a later run starting earlier still sorts",
			runs: []tsRun{
				run([]int64{10, 11}, []float64{10, 11}, nil),
				run([]int64{1, 2}, []float64{1, 2}, nil),
			},
			wantTS:  []int64{1, 2, 10, 11},
			wantVal: []float64{1, 2, 10, 11},
		},
		{
			name: "interleaved runs alternate",
			runs: []tsRun{
				run([]int64{1, 3, 5}, []float64{1, 3, 5}, nil),
				run([]int64{2, 4, 6}, []float64{2, 4, 6}, nil),
			},
			wantTS:  []int64{1, 2, 3, 4, 5, 6},
			wantVal: []float64{1, 2, 3, 4, 5, 6},
		},
		{
			name: "a tie is emitted once and the freshest run wins",
			runs: []tsRun{
				run([]int64{1, 2}, []float64{100, 200}, nil),
				run([]int64{2, 3}, []float64{999, 300}, nil),
			},
			wantTS:  []int64{1, 2, 3},
			wantVal: []float64{100, 999, 300},
		},
		{
			name: "every timestamp tied keeps only the freshest",
			runs: []tsRun{
				run([]int64{1, 2}, []float64{1, 2}, nil),
				run([]int64{1, 2}, []float64{10, 20}, nil),
				run([]int64{1, 2}, []float64{100, 200}, nil),
			},
			wantTS:  []int64{1, 2},
			wantVal: []float64{100, 200},
		},
		{
			name: "duplicates inside one run are kept",
			runs: []tsRun{
				run([]int64{1, 1, 2}, []float64{1, 2, 3}, nil),
				run([]int64{5}, []float64{5}, nil),
			},
			wantTS:  []int64{1, 1, 2, 5},
			wantVal: []float64{1, 2, 3, 5},
		},
		{
			name: "weights follow their run across a bulk stretch",
			runs: []tsRun{
				run([]int64{1, 2, 3}, []float64{1, 2, 3}, []float64{4, 4, 4}),
				run([]int64{7, 8}, []float64{7, 8}, nil),
			},
			wantTS:  []int64{1, 2, 3, 7, 8},
			wantVal: []float64{1, 2, 3, 7, 8},
			wantSF:  []float64{4, 4, 4, 1, 1},
		},
		{
			name: "a weighted run after an unweighted one backfills unit weights",
			runs: []tsRun{
				run([]int64{1, 2}, []float64{1, 2}, nil),
				run([]int64{7, 8}, []float64{7, 8}, []float64{3, 3}),
			},
			wantTS:  []int64{1, 2, 7, 8},
			wantVal: []float64{1, 2, 7, 8},
			wantSF:  []float64{1, 1, 3, 3},
		},
		{
			name: "an empty run is ignored",
			runs: []tsRun{
				run(nil, nil, nil),
				run([]int64{1, 2}, []float64{1, 2}, nil),
			},
			wantTS:  []int64{1, 2},
			wantVal: []float64{1, 2},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ts, values, sf := collectMany(tt.runs, nil, nil)
			require.Equal(t, tt.wantTS, ts)
			require.Equal(t, tt.wantVal, values)
			require.Equal(t, tt.wantSF, sf)

			// The naive merge is the definition; the fast path must be indistinguishable from it.
			nts, nvalues, nsf := collectManyNaive(tt.runs)
			require.Equal(t, nts, ts)
			require.Equal(t, nvalues, values)
			require.Equal(t, nsf, sf)
		})
	}
}

// TestCollectManyMatchesNaive covers the shapes a table cannot enumerate: many runs (past the
// stack-cursor bound), long unchallenged stretches, dense ties, and mixed weights.
func TestCollectManyMatchesNaive(t *testing.T) {
	t.Parallel()

	rng := rand.New(rand.NewPCG(1, 2))

	for iter := range 300 {
		nRuns := 1 + rng.IntN(70) // spans the [64]int cursor boundary
		spread := int64(1 + rng.IntN(40))
		weighted := iter%3 == 0

		runs := make([]tsRun, 0, nRuns)

		for r := range nRuns {
			n := rng.IntN(60)
			if n == 0 {
				runs = append(runs, tsRun{})

				continue
			}

			ts := make([]int64, n)
			vals := make([]float64, n)

			var sf []float64
			if weighted && r%2 == 0 {
				sf = make([]float64, n)
			}

			t0 := int64(rng.IntN(200))
			for i := range n {
				t0 += rng.Int64N(spread) // repeats when spread is small, so ties are common
				ts[i] = t0
				vals[i] = float64(r*1000 + i)

				if sf != nil {
					sf[i] = float64(1 + r)
				}
			}

			runs = append(runs, tsRun{ts: ts, vals: vals, sf: sf})
		}

		gotTS, gotVal, gotSF := collectMany(runs, nil, nil)
		wantTS, wantVal, wantSF := collectManyNaive(runs)

		require.Equalf(t, wantTS, gotTS, "iter %d timestamps", iter)
		require.Equalf(t, wantVal, gotVal, "iter %d values", iter)
		require.Equalf(t, wantSF, gotSF, "iter %d weights", iter)
	}
}
