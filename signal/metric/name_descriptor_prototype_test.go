package metric

import (
	"bytes"
	"fmt"
	"slices"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/index/symbols"
	"github.com/oteldb/storage/internal/memsize"
	"github.com/oteldb/storage/signal"
)

type prototypeDescriptorKey struct {
	name, unit  symbols.ID
	kind        PointKind
	temporality Temporality
	monotonic   bool
}

type prototypeDescriptor struct {
	key        prototypeDescriptorKey
	name, unit []byte
	encoded    []byte
	reserved   [5]signal.KeyValue
}

type prototypeDescriptors struct {
	symbols *symbols.Table
	byKey   map[prototypeDescriptorKey]uint32
	rows    []prototypeDescriptor
	limit   int
	last    int
}

func newPrototypeDescriptors(limit int) *prototypeDescriptors {
	return &prototypeDescriptors{symbols: symbols.New(), byKey: make(map[prototypeDescriptorKey]uint32), limit: limit, last: -1}
}

func (t *prototypeDescriptors) release() { t.symbols.Release() }

func (t *prototypeDescriptors) intern(m *Metric) (uint32, bool) {
	if t.last >= 0 {
		d := &t.rows[t.last]
		if d.key.kind == m.Kind && d.key.temporality == m.Temporality && d.key.monotonic == m.Monotonic && bytes.Equal(d.name, m.Name) && bytes.Equal(d.unit, m.Unit) {
			return uint32(t.last), true
		}
	}
	var name, unit symbols.ID
	if len(t.rows) >= t.limit {
		var ok bool
		if name, ok = t.symbols.Lookup(m.Name); !ok {
			return 0, false
		}
		if unit, ok = t.symbols.Lookup(m.Unit); !ok {
			return 0, false
		}
	} else {
		name = t.symbols.Intern(m.Name)
		unit = t.symbols.Intern(m.Unit)
	}
	key := prototypeDescriptorKey{name, unit, m.Kind, m.Temporality, m.Monotonic}
	if id, ok := t.byKey[key]; ok {
		t.last = int(id)
		return id, true
	}
	if len(t.rows) >= t.limit {
		return 0, false
	}
	var p projector
	p.setMetric(m)
	ownedName, _ := t.symbols.Get(name)
	ownedUnit, _ := t.symbols.Get(unit)
	p.reserved[2].Value = signal.StringValue(ownedName)
	p.reserved[4].Value = signal.StringValue(ownedUnit)
	id := uint32(len(t.rows))
	t.rows = append(t.rows, prototypeDescriptor{key, ownedName, ownedUnit, p.reservedBuf, p.reserved})
	t.byKey[key] = id
	t.last = int(id)
	return id, true
}

func (t *prototypeDescriptors) sizeBytes() int64 {
	n := t.symbols.SizeBytes() + memsize.Map(t.byKey) + memsize.Slice(t.rows)
	for i := range t.rows {
		n += int64(cap(t.rows[i].encoded))
	}
	return n
}

func projectWithPrototypeDescriptors(md Metrics, t *prototypeDescriptors, handles []uint32, emit func(*Batch)) (accepted int) {
	var p, fallback projector
	b, _ := batchPool.Get().(*Batch)
	defer func() { b.base = Identity{}; b.points = nil; b.source = Source{}; batchPool.Put(b) }()
	handle := 0
	var cachedDescriptor uint32
	descriptorValid := false
	for ri := range md.Resources {
		rm := &md.Resources[ri]
		b.base.Series.Resource = rm.Resource
		b.source.resource = &rm.Resource
		for si := range rm.Scopes {
			sm := &rm.Scopes[si]
			b.base.Series.Scope = sm.Scope
			b.source.scope = &sm.Scope
			p.setGroup(rm.Resource, sm.Scope)
			for mi := range sm.Metrics {
				m := &sm.Metrics[mi]
				b.source.metric = m
				if len(m.Points) == 0 {
					continue
				}
				b.base.Name, b.base.Unit, b.base.Kind = m.Name, m.Unit, m.Kind
				b.base.Temporality, b.base.Monotonic = m.Temporality, m.Monotonic
				var id uint32
				var ok bool
				if handles == nil {
					id, ok = t.intern(m)
				} else {
					id = handles[handle]
					ok = id != ^uint32(0)
					handle++
				}
				if ok {
					if !descriptorValid || cachedDescriptor != id {
						d := &t.rows[id]
						p.reserved, p.reservedBuf = d.reserved, d.encoded
						cachedDescriptor, descriptorValid = id, true
					}
				} else {
					descriptorValid = false
					fallback.setMetric(m)
					p.reserved, p.reservedBuf = fallback.reserved, fallback.reservedBuf
				}
				b.IDs, b.Ts, b.Values = b.IDs[:0], b.Ts[:0], b.Values[:0]
				p.projectPoints(b, m.Points)
				b.points = m.Points
				emit(b)
				accepted += len(m.Points)
			}
		}
	}
	return accepted
}

type prototypeDescriptorShape struct {
	name                                  string
	metrics, names, groups, resourceBytes int
	varyMetadata                          bool
}

func prototypeDescriptorInput(shape prototypeDescriptorShape) Metrics {
	var md Metrics
	names := max(shape.names, 1)
	groups := max(shape.groups, 1)
	nameBytes := make([][]byte, names)
	for i := range nameBytes {
		nameBytes[i] = []byte(fmt.Sprintf("node_metric_%04d", i))
	}
	for g := range groups {
		rm := md.AddResource()
		rm.Resource.Attributes = signal.NewAttributes(kv("service.name", str("node-exporter")))
		if shape.resourceBytes > 0 {
			rm.Resource.Attributes = signal.NewAttributes(kv("service.name", str("node-exporter")), kv("long.resource", signal.StringValue(bytes.Repeat([]byte{'r'}, shape.resourceBytes))))
		}
		sm := rm.AddScope()
		sm.Scope.Name = []byte("prometheus")
		for i := g; i < shape.metrics; i += groups {
			m := sm.AddMetric()
			m.Name = nameBytes[i%names]
			m.Unit = []byte("s")
			m.Kind = KindSum
			m.Temporality = TemporalityCumulative
			m.Monotonic = true
			if shape.varyMetadata {
				m.Unit = []byte(strconv.Itoa(i % 4))
				m.Kind = PointKind(i % 2)
				m.Temporality = Temporality(i % 3)
				m.Monotonic = i%2 == 0
			}
			point := m.AddPoint()
			point.Attributes = signal.NewAttributes(kv("instance", str("host-"+strconv.Itoa(i/8))), kv("cpu", str(strconv.Itoa(i%8))), kv("mode", str("idle")), kv("job", str("node_exporter")))
			point.Ts = int64(i + 1)
			point.Value = float64(i)
		}
	}
	return md
}

func prototypeDescriptorHandles(md Metrics, t *prototypeDescriptors) []uint32 {
	var out []uint32
	for ri := range md.Resources {
		for si := range md.Resources[ri].Scopes {
			for mi := range md.Resources[ri].Scopes[si].Metrics {
				m := &md.Resources[ri].Scopes[si].Metrics[mi]
				if len(m.Points) == 0 {
					continue
				}
				id, ok := t.intern(m)
				if !ok {
					id = ^uint32(0)
				}
				out = append(out, id)
			}
		}
	}
	return out
}

var prototypeDescriptorAccepted int

func BenchmarkMetricNameDescriptors(b *testing.B) {
	shapes := []prototypeDescriptorShape{
		{name: "repeated", metrics: 4096, names: 1},
		{name: "round-robin16", metrics: 4096, names: 16},
		{name: "round-robin256", metrics: 4096, names: 256},
		{name: "unique", metrics: 4096, names: 4096},
		{name: "metadata-mixed", metrics: 4096, names: 16, varyMetadata: true},
		{name: "scope-groups", metrics: 4096, names: 16, groups: 64},
		{name: "long-resource", metrics: 4096, names: 16, resourceBytes: 4096},
		{name: "cpu-10k", metrics: 80_000, names: 1},
		{name: "mixed-80k", metrics: 80_000, names: 16},
	}
	for _, shape := range shapes {
		b.Run(shape.name, func(b *testing.B) {
			md := prototypeDescriptorInput(shape)
			for _, mode := range []string{"project", "table-warm", "table-cold", "preinterned-handles"} {
				b.Run(mode, func(b *testing.B) {
					table := newPrototypeDescriptors(1024)
					defer table.release()
					handles := prototypeDescriptorHandles(md, table)
					consume := func(*Batch) {}
					b.ReportAllocs()
					b.SetBytes(int64(shape.metrics) * 32)
					switch mode {
					case "project":
						Project(md, consume)
						for b.Loop() {
							prototypeDescriptorAccepted = Project(md, consume)
						}
					case "table-warm":
						projectWithPrototypeDescriptors(md, table, nil, consume)
						for b.Loop() {
							prototypeDescriptorAccepted = projectWithPrototypeDescriptors(md, table, nil, consume)
						}
					case "table-cold":
						for b.Loop() {
							cold := newPrototypeDescriptors(1024)
							prototypeDescriptorAccepted = projectWithPrototypeDescriptors(md, cold, nil, consume)
							cold.release()
						}
					case "preinterned-handles":
						projectWithPrototypeDescriptors(md, table, handles, consume)
						for b.Loop() {
							prototypeDescriptorAccepted = projectWithPrototypeDescriptors(md, table, handles, consume)
						}
					}
					b.ReportMetric(float64(shape.metrics), "points/op")
					b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*shape.metrics), "ns/point")
					b.ReportMetric(float64(table.sizeBytes()), "table-B")
				})
			}
		})
	}
}

func TestPrototypeDescriptorsPreserveCanonicalIdentity(t *testing.T) {
	t.Parallel()
	for _, shape := range []prototypeDescriptorShape{{name: "repeated", metrics: 64, names: 1}, {name: "alternating", metrics: 64, names: 8}, {name: "bounded", metrics: 64, names: 64}, {name: "groups", metrics: 64, names: 8, groups: 4, varyMetadata: true}} {
		t.Run(shape.name, func(t *testing.T) {
			t.Parallel()
			md := prototypeDescriptorInput(shape)
			// A point-reserved collision must retain the same canonical duplicate-label ordering.
			md.Resources[0].Scopes[0].Metrics[0].Points[0].Attributes = signal.NewAttributes(kv("__name__", str("collision")), kv("__unit__", str("point-unit")))
			var want, got []signal.SeriesID
			Project(md, func(b *Batch) { want = append(want, b.IDs...) })
			table := newPrototypeDescriptors(8)
			defer table.release()
			for pass := range 2 {
				got = got[:0]
				projectWithPrototypeDescriptors(md, table, nil, func(b *Batch) {
					got = append(got, b.IDs...)
					for i, id := range b.IDs {
						require.Equal(t, b.Series(i).Hash(), id)
					}
				})
				require.Equal(t, want, got, "pass %d", pass)
			}
			require.LessOrEqual(t, len(table.rows), 8)
			require.LessOrEqual(t, table.symbols.Len(), 16)
		})
	}
}

func TestPrototypeDescriptorOwnsInputs(t *testing.T) {
	t.Parallel()
	table := newPrototypeDescriptors(4)
	defer table.release()
	m := Metric{Name: []byte("name"), Unit: []byte("ms"), Kind: KindSum, Temporality: TemporalityCumulative}
	first, ok := table.intern(&m)
	require.True(t, ok)
	snapshot := slices.Clone(table.rows[first].encoded)
	copy(m.Name, "next")
	copy(m.Unit, "us")
	second, ok := table.intern(&m)
	require.True(t, ok)
	require.NotEqual(t, first, second)
	require.Equal(t, snapshot, table.rows[first].encoded)
}

func FuzzPrototypeDescriptorIdentity(f *testing.F) {
	f.Add([]byte("metric"), []byte("ms"), []byte("__name__"), uint8(3))
	f.Add([]byte{}, []byte{}, []byte("cpu"), uint8(0))
	f.Fuzz(func(t *testing.T, name, unit, key []byte, flags uint8) {
		if len(name)+len(unit)+len(key) > 4096 {
			return
		}
		var md Metrics
		rm := md.AddResource()
		rm.Resource.SchemaURL = bytes.Clone(name)
		sm := rm.AddScope()
		sm.Scope.Name = bytes.Clone(unit)
		for i := range 4 {
			m := sm.AddMetric()
			m.Name = bytes.Clone(name)
			m.Unit = bytes.Clone(unit)
			m.Kind = PointKind(flags % 2)
			m.Temporality = Temporality(flags % 3)
			m.Monotonic = i%2 == 0
			pt := m.AddPoint()
			pt.Attributes = signal.NewAttributes(signal.KeyValue{Key: bytes.Clone(key), Value: signal.StringValue([]byte{byte(i)})})
			pt.Value = float64(i)
		}
		table := newPrototypeDescriptors(2)
		defer table.release()
		var want, got []signal.SeriesID
		Project(md, func(b *Batch) { want = append(want, b.IDs...) })
		projectWithPrototypeDescriptors(md, table, nil, func(b *Batch) { got = append(got, b.IDs...) })
		if !slices.Equal(want, got) {
			t.Fatal("descriptor table changed canonical identity")
		}
	})
}
