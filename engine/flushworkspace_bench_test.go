package engine

import (
	"context"
	"fmt"
	"os"
	"testing"

	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/signal"
)

func BenchmarkFlushAdmissionShape(b *testing.B) {
	for _, count := range []int{512, 8192} {
		identities := make([]signal.Series, count)
		ids := make([]signal.SeriesID, count)
		ts, values := make([]int64, count), make([]float64, count)
		for i := range count {
			identities[i] = benchSeries(i)
			ids[i] = identities[i].Hash()
			ts[i] = 100
			values[i] = float64(i)
		}
		for _, known := range []bool{false, true} {
			b.Run(fmt.Sprintf("series=%d/known=%t", count, known), func(b *testing.B) {
				var reservation int64
				b.ReportAllocs()
				b.SetBytes(int64(count) * 16)
				for b.Loop() {
					b.StopTimer()
					dir := b.TempDir()
					files, err := backendfile.New(dir)
					if err != nil {
						b.Fatal(err)
					}
					e := New(Config{Backend: files, Prefix: "shape", DiskSeries: true, SeparateSeries: true, MetricSeriesChunks: true,
						ResidentHeadBytes: 1 << 30, FlushMemoryBytes: 448 << 20, SeriesCacheBytes: 8 << 20,
						FlushAdmission: func(_ context.Context, n int64) (func(), error) {
							reservation = max(reservation, n)
							return func() {}, nil
						}})
					if known {
						if _, err := e.AppendBatch(ids, ts, values, nil, func(i int) signal.Series { return identities[i] }, AppendLimits{}); err != nil {
							b.Fatal(err)
						}
						if err := e.Flush(b.Context()); err != nil {
							b.Fatal(err)
						}
					}
					if _, err := e.AppendBatch(ids, ts, values, nil, func(i int) signal.Series { return identities[i] }, AppendLimits{}); err != nil {
						b.Fatal(err)
					}
					reservation = 0
					before := e.PartCount()
					b.StartTimer()
					err = e.Flush(b.Context())
					b.StopTimer()
					if err != nil {
						b.Fatal(err)
					}
					if e.PartCount()-before != 1 {
						b.Fatalf("flush outputs=%d", e.PartCount()-before)
					}
					b.ReportMetric(float64(reservation), "reservation-B")
					b.ReportMetric(1, "parts/op")
					if err := e.CloseWAL(); err != nil {
						b.Fatal(err)
					}
					if err := os.RemoveAll(dir); err != nil {
						b.Fatal(err)
					}
					b.StartTimer()
				}
			})
		}
	}
}
