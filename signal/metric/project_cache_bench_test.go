package metric

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/oteldb/storage/signal"
)

var projectedPointCacheCount int

func discardPointCacheBatch(*Batch) {}

func BenchmarkProjectPointCache(b *testing.B) {
	for _, tc := range []struct {
		name                  string
		series, labels        int
		interleave, perMetric bool
		prefix, repeat        int
	}{
		{name: "interleaved/two", series: 2, interleave: true},
		{name: "interleaved/four", series: 4, interleave: true},
		{name: "interleaved/eight", series: 8, interleave: true},
		{name: "interleaved/labels16", series: 2, labels: 16, interleave: true},
		{name: "history/shared", series: 64},
		{name: "unique/one-metric", series: 4096},
		{name: "unique/live", series: 4096, perMetric: true},
		{name: "tail/AA", series: 4096, prefix: 2},
		{name: "tail/ABAB", series: 4096, prefix: 4},
		{name: "tail/sparse8", series: 4096, prefix: 4, repeat: 8},
		{name: "tail/sparse16", series: 4096, prefix: 4, repeat: 16},
	} {
		b.Run(tc.name, func(b *testing.B) {
			const points = 4096
			attrs := make([]signal.Attributes, tc.series)
			for i := range attrs {
				values := []signal.KeyValue{kv("cpu", str(strconv.Itoa(i%8))), kv("instance", str("host-"+strconv.Itoa(i/8))), kv("job", str("node_exporter")), kv("mode", str("idle"))}
				for k := len(values); k < tc.labels; k++ {
					values = append(values, kv(fmt.Sprintf("dimension-%d", k), str(fmt.Sprintf("node-exporter-label-value-%d", k))))
				}
				attrs[i] = signal.NewAttributes(values...)
			}
			var md Metrics
			rm := md.AddResource()
			rm.Resource.Attributes = signal.NewAttributes(kv("service.name", str("node-exporter")))
			sm := rm.AddScope()
			sm.Scope.Name = []byte("prometheus")
			var metric *Metric
			for i := range points {
				if i == 0 || tc.perMetric {
					metric = sm.AddMetric()
					metric.Name = []byte("node_cpu_seconds_total")
					metric.Unit = []byte("s")
					metric.Kind = KindSum
					metric.Temporality = TemporalityCumulative
					metric.Monotonic = true
				}
				series := i / (points / tc.series)
				if tc.interleave {
					series = i % tc.series
				}
				if i < tc.prefix {
					series = i % max(tc.prefix/2, 1)
				} else if tc.repeat != 0 && (i-tc.prefix)%tc.repeat == tc.repeat-1 {
					series = i - 1
				}
				point := metric.AddPoint()
				point.Attributes = attrs[series]
				point.Ts = int64(i)
				point.Value = float64(i)
			}
			Project(md, discardPointCacheBatch)
			b.SetBytes(points * (16 + 8 + 8))
			b.ReportAllocs()
			for b.Loop() {
				projectedPointCacheCount = Project(md, discardPointCacheBatch)
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/(float64(b.N)*points), "ns/point")
		})
	}
}
