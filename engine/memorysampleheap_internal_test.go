package engine

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

type memorySampleOracle struct {
	ts            int64
	value, weight float64
}

func FuzzMemorySampleHeap(f *testing.F) {
	for _, corpus := range [][]byte{{}, {0}, {3, 1, 1, 0, 2, 0, 255}, {1, 0, 0, 0, 0}, {255, 255, 254, 253, 0}} {
		f.Add(corpus)
	}
	f.Fuzz(func(t *testing.T, input []byte) {
		if len(input) > 4096 {
			input = input[:4096]
		}
		runs := make([]tsRun, 0, 64)
		want := make([]memorySampleOracle, 0, len(input))
		for first := 0; first < len(input); first += 64 {
			end := min(first+64, len(input))
			ts := make([]int64, end-first)
			values, sf := make([]float64, len(ts)), make([]float64, len(ts))
			var timestamp int64
			for i, value := range input[first:end] {
				timestamp += int64(value % 5)
				ts[i], values[i], sf[i] = timestamp, float64(first+i), float64(value%3+1)
				want = append(want, memorySampleOracle{timestamp, values[i], sf[i]})
			}
			runs = append(runs, tsRun{ts: ts, vals: values, sf: sf})
		}
		slices.SortStableFunc(want, func(a, b memorySampleOracle) int {
			if a.ts < b.ts {
				return -1
			}
			if a.ts > b.ts {
				return 1
			}
			return 0
		})
		merge := memorySampleHeap{}
		merge.reset(runs, make([]int, len(runs)))
		got := make([]memorySampleOracle, 0, len(want))
		for {
			index, first, end, ok := merge.next()
			if !ok {
				break
			}
			for i := first; i < end; i++ {
				r := runs[index]
				got = append(got, memorySampleOracle{r.ts[i], r.vals[i], r.weight(i)})
			}
		}
		require.Equal(t, want, got)
	})
}
