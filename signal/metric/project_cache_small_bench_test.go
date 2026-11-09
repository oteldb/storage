package metric

import (
	"strconv"
	"testing"

	"github.com/oteldb/storage/signal"
)

var projectPointSmallCount int

func discardSmallMetricBatch(*Batch) {}

func BenchmarkProjectPointCacheSmall(b *testing.B) {
	for _, tc := range []struct {
		name     string
		points   int
		repeated bool
	}{
		{"unique4", 4, false},
		{"unique8", 8, false},
		{"unique16", 16, false},
		{"ABAB4", 4, true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			const metrics = 1024
			var md Metrics
			rm := md.AddResource()
			rm.Resource.Attributes = signal.NewAttributes(kv("service.name", str("node-exporter")))
			sm := rm.AddScope()
			sm.Scope.Name = []byte("prometheus")
			for mi := range metrics {
				metric := sm.AddMetric()
				metric.Name = []byte("node_cpu_seconds_total")
				metric.Unit = []byte("s")
				metric.Kind = KindSum
				metric.Temporality, metric.Monotonic = TemporalityCumulative, true
				attrs := make([]signal.Attributes, tc.points)
				for i := range attrs {
					attrs[i] = signal.NewAttributes(kv("cpu", str(strconv.Itoa(i))), kv("instance", str("host-"+strconv.Itoa(mi))), kv("job", str("node_exporter")), kv("mode", str("idle")))
				}
				for i := range tc.points {
					point := metric.AddPoint()
					index := i
					if tc.repeated {
						index = i % 2
					}
					point.Attributes = attrs[index]
					point.Ts = int64(mi*tc.points + i)
					point.Value = float64(mi*tc.points + i)
				}
			}
			points := metrics * tc.points
			Project(md, discardSmallMetricBatch)
			b.SetBytes(int64(points) * 32)
			b.ReportAllocs()
			for b.Loop() {
				projectPointSmallCount = Project(md, discardSmallMetricBatch)
			}
			b.ReportMetric(float64(metrics), "metrics/op")
			b.ReportMetric(float64(points), "points/op")
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/(float64(b.N)*float64(points)), "ns/point")
		})
	}
}
