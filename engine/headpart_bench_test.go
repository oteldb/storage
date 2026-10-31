package engine

import (
	"context"
	"strconv"
	"testing"

	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/signal"
)

func BenchmarkFlushSealedHead(b *testing.B) {
	const seriesCount = 65536
	for _, points := range []int{1, 8, 128} {
		b.Run("points="+strconv.Itoa(points), func(b *testing.B) {
			be, err := file.New(b.TempDir())
			if err != nil {
				b.Fatal(err)
			}
			var peakReservation int64
			e := New(Config{Backend: be, Prefix: "metrics", MaxPartBytes: 64 << 20, FlushMemoryBytes: 448 << 20,
				FlushAdmission: func(_ context.Context, bytes int64) (func(), error) {
					peakReservation = max(peakReservation, bytes)
					return func() {}, nil
				}})
			samples := make(map[signal.SeriesID]*sampleBuf, seriesCount)
			for i := range seriesCount {
				s := signal.Series{Attributes: signal.NewAttributes(
					signal.KeyValue{Key: []byte("job"), Value: signal.StringValue([]byte("node_exporter"))},
					signal.KeyValue{Key: []byte("instance"), Value: signal.StringValue([]byte(strconv.Itoa(i)))},
				)}
				e.head.registerSeries(s)
				buf := &sampleBuf{ts: make([]int64, points), values: make([]float64, points)}
				for row := range points {
					buf.ts[row], buf.values[row] = int64(row), float64(row+i%17)
				}
				samples[s.Hash()] = buf
			}
			rows := seriesCount * points
			b.ReportAllocs()
			b.SetBytes(int64(rows) * partRowBytes)
			var parts int
			for b.Loop() {
				b.StopTimer()
				e.head.samples, e.head.bytes = samples, int64(rows)*SampleBytes
				e.parts = nil
				b.StartTimer()
				flushed, _, err := e.flush(b.Context())
				b.StopTimer()
				if err != nil {
					b.Fatal(err)
				}
				if flushed != rows {
					b.Fatalf("flushed %d rows, want %d", flushed, rows)
				}
				parts = len(e.parts)
				b.StartTimer()
			}
			b.ReportMetric(float64(parts), "parts/op")
			b.ReportMetric(float64(peakReservation), "admit-bytes")
		})
	}
}
