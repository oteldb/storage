package metric

import (
	"strconv"
	"testing"

	"github.com/oteldb/storage/signal"
)

var projectedSeriesSink signal.SeriesID

func BenchmarkProjectSeries(b *testing.B) {
	for _, tc := range []struct {
		name              string
		series, points    int
		clone, interleave bool
		separateMetrics   bool
	}{
		{name: "one-point", series: 1, points: 1},
		{name: "one-series/shared", series: 1, points: 64},
		{name: "one-series/cloned", series: 1, points: 64, clone: true},
		{name: "grouped", series: 64, points: 64},
		{name: "interleaved", series: 64, points: 64, interleave: true},
		{name: "unique", series: 4096, points: 1},
		{name: "scrape", series: 4096, points: 1, separateMetrics: true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			var md Metrics
			rm := md.AddResource()
			rm.Resource.Attributes = signal.NewAttributes(kv("service.name", str("node-exporter")))
			sm := rm.AddScope()
			sm.Scope.Name = []byte("prometheus")
			attrs := make([]signal.Attributes, tc.series)
			for i := range attrs {
				attrs[i] = signal.NewAttributes(kv("instance", str("host-"+strconv.Itoa(i))),
					kv("cpu", str("0")), kv("mode", str("idle")))
			}
			mt := sm.AddMetric()
			mt.Name, mt.Unit, mt.Kind = []byte("node_cpu_seconds_total"), []byte("s"), KindSum
			for i := range tc.series * tc.points {
				series := i / tc.points
				if tc.interleave {
					series = i % tc.series
				}
				if tc.separateMetrics && i > 0 {
					mt = sm.AddMetric()
					mt.Name, mt.Unit, mt.Kind = []byte("node_cpu_seconds_total"), []byte("s"), KindSum
				}
				pt := mt.AddPoint()
				pt.Attributes, pt.Ts, pt.Value = attrs[series], int64(i), float64(i)
				if tc.clone {
					pt.Attributes = pt.Attributes.Clone()
				}
			}
			count := tc.series * tc.points
			consume := func(batch *Batch) {
				projectedSeriesSink = batch.IDs[len(batch.IDs)-1]
			}
			if got := Project(md, consume); got != count {
				b.Fatalf("projected %d points, want %d", got, count)
			}
			b.ReportAllocs()
			b.ReportMetric(float64(count), "points/op")
			for b.Loop() {
				Project(md, consume)
			}
		})
	}
}
