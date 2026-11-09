package metric

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

//nolint:paralleltest,tparallel // Steps deliberately mutate one cached projector.
func TestProjectorDescriptorReuseTracksMutations(t *testing.T) {
	t.Parallel()
	var p projector
	resource := signal.Resource{Attributes: signal.NewAttributes(kv("service.name", str("exporter")))}
	scope := signal.Scope{Name: []byte("prometheus")}
	p.setGroup(resource, scope)
	metric := Metric{Name: []byte("metric_a"), Unit: []byte("ms"), Kind: KindSum, Temporality: TemporalityCumulative, Monotonic: true}
	for _, step := range []struct {
		name   string
		change func()
	}{
		{"initial", func() {}},
		{"same descriptor", func() {}},
		{"same object new name", func() { metric.Name = []byte("metric_b") }},
		{"same name buffer changed", func() { copy(metric.Name, "metric_c") }},
		{"shorter name", func() { metric.Name = []byte("m") }},
		{"longer name", func() { metric.Name = bytes.Repeat([]byte("n"), 4096) }},
		{"longer unit", func() { metric.Unit = bytes.Repeat([]byte("u"), 2048) }},
		{"shorter unit", func() { metric.Unit = []byte("ms") }},
		{"same unit buffer changed", func() { copy(metric.Unit, "us") }},
		{"kind", func() { metric.Kind = KindGauge }},
		{"temporality", func() { metric.Temporality = TemporalityDelta }},
		{"monotonicity", func() { metric.Monotonic = false }},
		{"empty names", func() { metric.Name = nil; metric.Unit = nil }},
		{"empty slices", func() { metric.Name = []byte{}; metric.Unit = []byte{} }},
		{"default descriptor", func() { metric = Metric{} }},
		{"same default descriptor", func() {}},
	} {
		t.Run(step.name, func(t *testing.T) {
			step.change()
			p.setMetric(&metric)
			for _, attrs := range []signal.Attributes{
				nil,
				signal.NewAttributes(kv("cpu", str("0"))),
				signal.NewAttributes(kv("__name__", str("point-name")), kv("__unit__", str("point-unit"))),
			} {
				id := Identity{Series: signal.Series{Resource: resource, Scope: scope, Attributes: attrs}, Name: metric.Name, Unit: metric.Unit,
					Kind: metric.Kind, Temporality: metric.Temporality, Monotonic: metric.Monotonic}
				require.Equal(t, id.SeriesID(), p.id(attrs))
			}
		})
	}
}

func TestProjectorDescriptorSnapshotOwnsReservedValues(t *testing.T) {
	t.Parallel()
	var p projector
	p.setGroup(signal.Resource{}, signal.Scope{})
	first := Metric{Name: []byte("metric_a"), Unit: []byte("ms"), Kind: KindGauge}
	second := Metric{Name: bytes.Clone(first.Name), Unit: bytes.Clone(first.Unit), Kind: KindGauge}
	p.setMetric(&first)
	p.setMetric(&second)
	copy(first.Name, "changed!")
	copy(first.Unit, "xx")
	attrs := signal.NewAttributes(kv("__name__", str("collision")))
	want := Identity{Series: signal.Series{Attributes: attrs}, Name: second.Name, Unit: second.Unit, Kind: second.Kind}
	require.Equal(t, want.SeriesID(), p.id(attrs))
}

func TestProjectDescriptorReuseKeepsBatchSource(t *testing.T) {
	t.Parallel()
	for _, shape := range []string{"groups", "empty metric", "separate calls"} {
		t.Run(shape, func(t *testing.T) {
			t.Parallel()
			var md Metrics
			for ri := range 2 {
				rm := md.AddResource()
				rm.Resource.Attributes = signal.NewAttributes(kv("resource", signal.IntValue(int64(ri))))
				for si := range 2 {
					sm := rm.AddScope()
					sm.Scope.Name = []byte{byte('a' + si)}
					for mi := range 3 {
						mt := sm.AddMetric()
						mt.Name = []byte("metric")
						mt.Unit = []byte("s")
						mt.Kind = KindSum
						mt.Temporality = TemporalityCumulative
						mt.Monotonic = true
						if shape == "empty metric" && mi == 1 {
							continue
						}
						pt := mt.AddPoint()
						pt.Ts = int64(ri*10 + si*3 + mi)
						pt.Value = float64(mi)
						pt.Attributes = signal.NewAttributes(kv("point", signal.IntValue(int64(mi))))
					}
				}
			}
			calls := 1
			if shape == "separate calls" {
				calls = 2
			}
			for range calls {
				rows := 0
				got := Project(md, func(batch *Batch) {
					for i, id := range batch.IDs {
						require.Equal(t, batch.Series(i).Hash(), id)
						rows++
					}
				})
				want := 12
				if shape == "empty metric" {
					want = 8
				}
				require.Equal(t, want, rows)
				require.Equal(t, rows, got)
			}
		})
	}
}

func FuzzProjectorDescriptorReuse(f *testing.F) {
	f.Add([]byte("node_cpu_seconds_total"), []byte("s"), []byte("cpu"), uint8(0))
	f.Add([]byte{}, []byte{}, []byte("__name__"), uint8(7))
	f.Add([]byte("name"), []byte("ms"), []byte("__unit__"), uint8(3))
	f.Fuzz(func(t *testing.T, name, unit, key []byte, flags uint8) {
		if len(name)+len(unit)+len(key) > 4096 {
			return
		}
		var p projector
		resource := signal.Resource{}
		scope := signal.Scope{}
		p.setGroup(resource, scope)
		metric := Metric{Name: bytes.Clone(name), Unit: bytes.Clone(unit), Kind: KindSum, Temporality: TemporalityCumulative, Monotonic: true}
		attrs := signal.NewAttributes(signal.KeyValue{Key: key, Value: signal.StringValue([]byte("value"))})
		for i := range 4 {
			p.setMetric(&metric)
			want := Identity{Series: signal.Series{Resource: resource, Scope: scope, Attributes: attrs}, Name: metric.Name, Unit: metric.Unit,
				Kind: metric.Kind, Temporality: metric.Temporality, Monotonic: metric.Monotonic}
			if want.SeriesID() != p.id(attrs) {
				t.Fatal("descriptor reuse changed canonical identity")
			}
			if i%2 == 0 {
				metric.Name = mutateDescriptorBytes(metric.Name, flags)
				metric.Unit = mutateDescriptorBytes(metric.Unit, flags)
			} else {
				metric.Monotonic = !metric.Monotonic
				metric.Kind = KindGauge
				metric.Temporality = TemporalityDelta
			}
		}
	})
}

func mutateDescriptorBytes(value []byte, flags byte) []byte {
	if len(value) == 0 {
		return []byte{flags}
	}
	value[0] ^= flags
	return value
}
