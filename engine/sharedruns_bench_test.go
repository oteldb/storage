package engine

import (
	"strconv"
	"testing"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/chunk"
)

func BenchmarkOutputSeriesState(b *testing.B) {
	for _, count := range []int{4096, 65536} {
		b.Run(strconv.Itoa(count), func(b *testing.B) {
			e := New(Config{Backend: backend.Memory(), MetricSeriesChunks: true})
			b.ReportAllocs()
			b.SetBytes(int64(count) * 16)
			for b.Loop() {
				w, err := newPartStreamWriter(b.Context(), e, compressProfile{}, 0, false, true, nil)
				if err != nil {
					b.Fatal(err)
				}
				if err := w.reserveSeries(count); err != nil {
					b.Fatal(err)
				}
				for i := range count {
					if err := w.appendSeries(chunk.U128{Lo: uint64(i)}, []int64{100}, []float64{float64(i)}, nil, 100); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportMetric(float64(w.residentBytes()), "writer-resident-B")
				b.ReportMetric(float64(w.residentBytes()-w.w.ResidentBytes())/float64(count), "side-state-B/series")
				w.abort()
			}
		})
	}
}
