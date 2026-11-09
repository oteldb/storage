package metric

import (
	"context"
	"fmt"
	"runtime/pprof"
	"strconv"
	"testing"

	"github.com/oteldb/storage/signal"
)

type projectNoopShape struct {
	name                          string
	series, samples, labels       int
	resources, scopes             int
	perSeries, cloned, interleave bool
	variedNames                   bool
}

var projectNoopShapes = []projectNoopShape{
	{name: "distinct/one-metric", series: 4096},
	{name: "distinct/per-series-metric", series: 4096, perSeries: true},
	{name: "distinct/varied-metric-names", series: 4096, perSeries: true, variedNames: true},
	{name: "distinct/resources=64", series: 4096, resources: 64, perSeries: true},
	{name: "distinct/scopes=64", series: 4096, scopes: 64, perSeries: true},
	{name: "distinct/labels=16", series: 4096, labels: 16, perSeries: true},
	{name: "history/shared", series: 64, samples: 64},
	{name: "history/cloned", series: 64, samples: 64, cloned: true},
	{name: "history/interleaved", series: 64, samples: 64, interleave: true},
	{name: "cpu-10k/one-metric", series: 80_000},
	{name: "cpu-10k/per-series-metric", series: 80_000, perSeries: true},
	{name: "cpu-10k/history-per-series-shared", series: 80_000, samples: 4, perSeries: true},
	{name: "cpu-10k/history-per-series-cloned", series: 80_000, samples: 4, perSeries: true, cloned: true},
	{name: "cpu-10k/history-packed-grouped", series: 80_000, samples: 4},
	{name: "cpu-10k/history-packed-interleaved", series: 80_000, samples: 4, interleave: true},
}

var projectedNoopPoints int

func discardMetricBatch(*Batch) {}

// BenchmarkProjectNoop excludes input construction, decoding, identity materialization and storage.
func BenchmarkProjectNoop(b *testing.B) {
	for _, shape := range projectNoopShapes {
		b.Run(shape.name, func(b *testing.B) {
			md := newProjectNoopInput(shape)
			points := shape.series * max(shape.samples, 1)
			if got := Project(md, discardMetricBatch); got != points {
				b.Fatalf("projected %d points, want %d", got, points)
			}
			b.SetBytes(int64(points) * (16 + 8 + 8))
			b.ReportAllocs()
			pprof.Do(b.Context(), pprof.Labels("benchmark", "metric.Project", "shape", shape.name), func(context.Context) {
				for b.Loop() {
					projectedNoopPoints = Project(md, discardMetricBatch)
				}
			})
			b.ReportMetric(float64(points), "points/op")
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/(float64(b.N)*float64(points)), "ns/point")
		})
	}
}

func newProjectNoopInput(shape projectNoopShape) Metrics {
	samples := max(shape.samples, 1)
	resources, scopes := max(shape.resources, 1), max(shape.scopes, 1)
	attrs := make([]signal.Attributes, shape.series)
	for i := range attrs {
		values := []signal.KeyValue{
			kv("instance", str("host-"+strconv.Itoa(i/8))),
			kv("cpu", str(strconv.Itoa(i%8))),
			kv("mode", str("idle")),
			kv("job", str("node_exporter")),
		}
		for k := len(values); k < shape.labels; k++ {
			values = append(values, kv("dimension-"+strconv.Itoa(k), str("node-exporter-label-value-"+strconv.Itoa(k))))
		}
		attrs[i] = signal.NewAttributes(values...)
	}
	var md Metrics
	groupSize := shape.series / (resources * scopes)
	name, unit := []byte("node_cpu_seconds_total"), []byte("s")
	for ri := range resources {
		rm := md.AddResource()
		rm.Resource.Attributes = signal.NewAttributes(kv("service.name", str("node-exporter")))
		for si := range scopes {
			sm := rm.AddScope()
			sm.Scope.Name = []byte("prometheus")
			start := (ri*scopes + si) * groupSize
			addMetric := func(series int) *Metric {
				mt := sm.AddMetric()
				mt.Name, mt.Unit, mt.Kind = name, unit, KindSum
				mt.Temporality, mt.Monotonic = TemporalityCumulative, true
				if shape.variedNames {
					mt.Name = []byte(fmt.Sprintf("node_cpu_seconds_%05d", series))
				}
				return mt
			}
			var mt *Metric
			for row := range groupSize * samples {
				series, sample := start+row/samples, row%samples
				if shape.interleave {
					series, sample = start+row%groupSize, row/groupSize
				}
				if row == 0 || shape.perSeries && sample == 0 {
					mt = addMetric(series)
				}
				point := mt.AddPoint()
				point.Attributes = attrs[series]
				if shape.cloned {
					point.Attributes = point.Attributes.Clone()
				}
				point.Ts, point.Value = int64(sample+1)*15_000_000_000, float64(series+sample)
			}
		}
	}
	return md
}

// BenchmarkProjectStages identifies work retained even by a no-op projection sink.
func BenchmarkProjectStages(b *testing.B) {
	md := newProjectNoopInput(projectNoopShape{series: 1})
	rm := &md.Resources[0]
	sm := &rm.Scopes[0]
	mt := &sm.Metrics[0]
	b.Run("group-prefix", func(b *testing.B) {
		var p projector
		p.setGroup(rm.Resource, sm.Scope)
		b.ReportAllocs()
		for b.Loop() {
			p.setGroup(rm.Resource, sm.Scope)
		}
	})
	b.Run("metric-prefix", func(b *testing.B) {
		var p projector
		p.setMetric(mt)
		b.ReportAllocs()
		for b.Loop() {
			p.setMetric(mt)
		}
	})
	b.Run("point-identity", func(b *testing.B) {
		var p projector
		p.setGroup(rm.Resource, sm.Scope)
		p.setMetric(mt)
		p.id(mt.Points[0].Attributes)
		b.ReportAllocs()
		for b.Loop() {
			projectedSeriesSink = p.id(mt.Points[0].Attributes)
		}
	})
}
