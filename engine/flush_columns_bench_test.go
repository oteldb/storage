package engine

import (
	"testing"

	"github.com/oteldb/storage/signal"
)

func BenchmarkBuildFlushColumns(b *testing.B) {
	for _, tc := range []struct {
		name                string
		series, points      int
		unordered, weighted bool
	}{
		{"single-point", 65536, 1, false, false},
		{"short-ordered", 65536, 8, false, false},
		{"long-ordered", 1024, 128, false, false},
		{"short-weighted", 65536, 8, false, true},
		{"short-unordered", 65536, 8, true, false},
	} {
		b.Run(tc.name, func(b *testing.B) {
			samples := make(map[signal.SeriesID]*sampleBuf, tc.series)
			for i := range tc.series {
				buf := &sampleBuf{ts: make([]int64, tc.points), values: make([]float64, tc.points)}
				if tc.weighted {
					buf.sf = make([]float64, tc.points)
				}
				for j := range tc.points {
					buf.ts[j] = int64(j)
					if tc.unordered {
						buf.ts[j] = int64(tc.points - j)
					}
					buf.values[j] = float64(j)
					if tc.weighted {
						buf.sf[j] = float64(1 + j%2)
					}
				}
				samples[signal.SeriesID{Hi: uint64(i * 31), Lo: uint64(i)}] = buf
			}
			b.ReportAllocs()
			b.SetBytes(int64(tc.series * tc.points * partRowBytes))
			for b.Loop() {
				cols := buildFlushColumns(samples)
				if len(cols.ts) != tc.series*tc.points {
					b.Fatal("lost rows")
				}
			}
		})
	}
}
