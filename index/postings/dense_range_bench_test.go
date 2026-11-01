package postings

import (
	"fmt"
	"testing"

	"github.com/oteldb/storage/signal"
)

func BenchmarkDenseSeekRange(b *testing.B) {
	for _, n := range []int{1 << 16, 14_490_000} {
		for _, stride := range []uint32{1, 2} {
			b.Run(fmt.Sprintf("ids=%d/stride=%d", n, stride), func(b *testing.B) {
				ids := make([]uint32, n)
				for i := range ids {
					ids[i] = uint32(i) * stride
				}
				targets := make([]signal.SeriesID, 1449)
				for i := range targets {
					targets[i] = signal.SeriesID{Lo: uint64(ids[i*n/len(targets)])}
				}
				b.ReportAllocs()
				for b.Loop() {
					p := fromDenseSlice(ids)
					for _, target := range targets {
						if !p.Seek(target) || p.At() != target {
							b.Fatal("seek mismatch")
						}
					}
				}
				b.ReportMetric(float64(len(targets)), "seeks/op")
			})
		}
	}
}
