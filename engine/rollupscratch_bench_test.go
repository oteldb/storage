package engine

import (
	"fmt"
	"testing"
)

func BenchmarkRollupScratch(b *testing.B) {
	for _, n := range []int{1, 4, 64, 1024, 4096} {
		for _, reuse := range []bool{false, true} {
			b.Run(fmt.Sprintf("points=%d/reuse=%t", n, reuse), func(b *testing.B) {
				ts, values := make([]int64, n), make([]float64, n)
				for i := range ts {
					ts[i], values[i] = int64(i), float64(i)
				}
				var scratch rollupScratch
				var m sampleMerge
				b.ReportAllocs()
				b.SetBytes(int64(n) * 16)
				for b.Loop() {
					if reuse {
						m.reset()
					} else {
						m = sampleMerge{}
						scratch = rollupScratch{}
					}
					m.add(ts, values, nil, nil, minInt64, maxInt64)
					out, _, _, _ := scratch.roll(&m, nil)
					if len(out) != n {
						b.Fatal("unexpected output length")
					}
				}
			})
		}
	}
}
