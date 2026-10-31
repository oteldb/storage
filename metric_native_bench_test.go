package storage

import (
	"context"
	"strconv"
	"testing"

	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/metric"
)

// BenchmarkMetricSourceNative measures the existing native facade without pdata conversion.
func BenchmarkMetricSourceNative(b *testing.B) {
	for _, shape := range []struct {
		name            string
		metrics, points int
	}{{"grouped", 1, 1024}, {"fragmented", 1024, 1}} {
		b.Run(shape.name, func(b *testing.B) {
			var md metric.Metrics
			scope := md.AddResource().AddScope()
			for mi := range shape.metrics {
				m := scope.AddMetric()
				m.Name = []byte("metric-" + strconv.Itoa(mi))
				for pi := range shape.points {
					point := m.AddPoint()
					point.Ts = 1
					point.Value = 1
					point.Attributes = signal.NewAttributes(signal.KeyValue{Key: []byte("point"), Value: signal.IntValue(int64(pi))})
				}
			}
			s, err := InMemory()
			if err != nil {
				b.Fatal(err)
			}
			ctx := context.Background()
			if _, err := s.WriteMetrics(ctx, md); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			n := 0
			for b.Loop() {
				if _, err := s.WriteMetrics(ctx, md); err != nil {
					b.Fatal(err)
				}
				n++
				if n%1024 == 0 {
					b.StopTimer()
					if err := s.Reset(ctx); err != nil {
						b.Fatal(err)
					}
					if _, err := s.WriteMetrics(ctx, md); err != nil {
						b.Fatal(err)
					}
					b.StartTimer()
				}
			}
			if err := s.Close(ctx); err != nil {
				b.Fatal(err)
			}
		})
	}
}
