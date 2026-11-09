package series

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
	"github.com/oteldb/storage/signal/metric"
)

type registryDescriptorKey struct {
	name, unit  symbols.ID
	kind        metric.PointKind
	temporality metric.Temporality
	monotonic   bool
}
type registryDescriptor struct {
	key      registryDescriptorKey
	reserved [5]signal.KeyValue
}
type prototypeMetricRegistry struct {
	base         *Index
	rows         []uint32
	descriptors  []registryDescriptor
	byDescriptor map[registryDescriptorKey]uint32
	last         int
	limit        int
	scratch      []byte
}

func newPrototypeMetricRegistry(limit int) *prototypeMetricRegistry {
	return &prototypeMetricRegistry{base: New(), byDescriptor: make(map[registryDescriptorKey]uint32), last: -1, limit: limit}
}

func (p *prototypeMetricRegistry) descriptor(id metric.Identity) (uint32, bool) {
	if p.last >= 0 {
		d := &p.descriptors[p.last]
		if d.key.kind == id.Kind && d.key.temporality == id.Temporality && d.key.monotonic == id.Monotonic && bytes.Equal(d.reserved[2].Value.Str(), id.Name) && bytes.Equal(d.reserved[4].Value.Str(), id.Unit) {
			return uint32(p.last), true
		}
	}
	p.scratch = signal.AppendValue(p.scratch[:0], signal.StringValue(id.Name))
	nameBytes := p.scratch
	var name, unit symbols.ID
	if len(p.descriptors) >= p.limit {
		var ok bool
		if name, ok = p.base.sym.Lookup(nameBytes); !ok {
			return 0, false
		}
	} else {
		name = p.base.sym.Intern(nameBytes)
	}
	p.scratch = signal.AppendValue(p.scratch[:0], signal.StringValue(id.Unit))
	unitBytes := p.scratch
	if len(p.descriptors) >= p.limit {
		var ok bool
		if unit, ok = p.base.sym.Lookup(unitBytes); !ok {
			return 0, false
		}
	} else {
		unit = p.base.sym.Intern(unitBytes)
	}
	key := registryDescriptorKey{name, unit, id.Kind, id.Temporality, id.Monotonic}
	if found, ok := p.byDescriptor[key]; ok {
		p.last = int(found)
		return found, true
	}
	if len(p.descriptors) >= p.limit {
		return 0, false
	}
	ownedName, _ := p.base.sym.Get(name)
	nameValue, _, _ := signal.DecodeValue(ownedName)
	ownedUnit, _ := p.base.sym.Get(unit)
	unitValue, _, _ := signal.DecodeValue(ownedUnit)
	d := registryDescriptor{key: key, reserved: [5]signal.KeyValue{
		{Key: metric.LabelKind, Value: signal.IntValue(int64(id.Kind))},
		{Key: metric.LabelMonotonic, Value: signal.BoolValue(id.Monotonic)},
		{Key: metric.LabelName, Value: nameValue},
		{Key: metric.LabelTemporality, Value: signal.IntValue(int64(id.Temporality))},
		{Key: metric.LabelUnit, Value: unitValue},
	}}
	ref := uint32(len(p.descriptors))
	p.descriptors = append(p.descriptors, d)
	p.byDescriptor[key] = ref
	p.last = int(ref)
	return ref, true
}

func prototypeReservedCollision(attrs signal.Attributes) bool {
	for _, attr := range attrs {
		for _, key := range [][]byte{metric.LabelKind, metric.LabelMonotonic, metric.LabelName, metric.LabelTemporality, metric.LabelUnit} {
			if bytes.Equal(attr.Key, key) {
				return true
			}
		}
	}
	return false
}

func (p *prototypeMetricRegistry) add(ident metric.Identity) signal.SeriesID {
	return p.addFull(ident, ident.ToSeries())
}

func (p *prototypeMetricRegistry) addFull(ident metric.Identity, full signal.Series) signal.SeriesID {
	p.base.buf = full.AppendHashInput(p.base.buf[:0])
	id := signal.HashBytes(p.base.buf)
	if _, ok := p.base.byID[id]; ok {
		return id
	}
	descriptor, ok := p.descriptor(ident)
	if !ok || prototypeReservedCollision(ident.Series.Attributes) {
		p.base.Add(full)
		p.rows = append(p.rows, ^uint32(0))
		return id
	}
	ix := p.base
	s := ident.Series
	c := contextKey{resource: ix.internResource(s.Resource), scope: ix.internScope(s.Scope)}
	context, ok := ix.context[c]
	if !ok {
		context = uint32(len(ix.contexts))
		ix.context[c] = context
		ix.contexts = append(ix.contexts, identityContext{resource: ix.resources[c.resource], scope: ix.scopes[c.scope]})
		ix.bytes += memsize.MapEntry[contextKey, uint32]()
	}
	keys := ix.internKeys(s.Attributes)
	offset := uint64(len(ix.values))
	for _, attr := range s.Attributes {
		ix.vbuf = signal.AppendValue(ix.vbuf[:0], attr.Value)
		ix.values = append(ix.values, ix.sym.Intern(ix.vbuf))
	}
	ix.byID[id] = uint32(len(ix.rows))
	ix.rows = append(ix.rows, row{id: id, offset: offset, context: context, keys: keys})
	ix.bytes += memsize.MapEntry[signal.SeriesID, uint32]()
	p.rows = append(p.rows, descriptor)
	return id
}

func (p *prototypeMetricRegistry) at(ref int, dst signal.Attributes) signal.Series {
	descriptor := p.rows[ref]
	if descriptor == ^uint32(0) {
		return p.base.View().At(ref, dst)
	}
	ix := p.base
	r := ix.rows[ref]
	context := ix.contexts[r.context]
	keys := ix.keys[r.keys]
	reserved := &p.descriptors[descriptor].reserved
	dst = dst[:0]
	i, j := 0, 0
	for i < len(keys) || j < len(reserved) {
		if j < len(reserved) && (i == len(keys) || bytes.Compare(reserved[j].Key, ix.sym.Snapshot()[keys[i]]) < 0) {
			dst = append(dst, reserved[j])
			j++
			continue
		}
		key, _ := ix.sym.Get(keys[i])
		raw, _ := ix.sym.Get(ix.values[int(r.offset)+i])
		value, _, _ := signal.DecodeValue(raw)
		dst = append(dst, signal.KeyValue{Key: key, Value: value})
		i++
	}
	return signal.Series{Resource: context.resource, Scope: context.scope, Attributes: dst}
}

func (p *prototypeMetricRegistry) sizeBytes() int64 {
	return p.base.SizeBytes() + int64(cap(p.scratch)) + memsize.Slice(p.rows) + memsize.Slice(p.descriptors) + memsize.Map(p.byDescriptor)
}

func prototypeRegistryIdentity(i, names int) metric.Identity {
	return metric.Identity{Series: signal.Series{Resource: signal.Resource{Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte("node-exporter"))})}, Scope: signal.Scope{Name: []byte("prometheus")}, Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("instance"), Value: signal.StringValue([]byte("host-" + strconv.Itoa(i/8)))},
		signal.KeyValue{Key: []byte("cpu"), Value: signal.StringValue([]byte(strconv.Itoa(i % 8)))},
		signal.KeyValue{Key: []byte("mode"), Value: signal.StringValue([]byte("idle"))},
		signal.KeyValue{Key: []byte("job"), Value: signal.StringValue([]byte("node_exporter"))},
	)}, Name: []byte(fmt.Sprintf("metric_%04d", i%names)), Unit: []byte("s"), Kind: metric.KindSum, Temporality: metric.TemporalityCumulative, Monotonic: true}
}

var prototypeRegistryID signal.SeriesID
var prototypeRegistrySeries signal.Series

func BenchmarkMetricDescriptorRegistry(b *testing.B) {
	const count = 80_000
	inputs := make([]metric.Identity, count)
	full := make([]signal.Series, count)
	for i := range inputs {
		inputs[i] = prototypeRegistryIdentity(i, 16)
		full[i] = inputs[i].ToSeries()
	}
	var logicalBytes int64
	var encoded []byte
	for i := range full {
		encoded = full[i].AppendHashInput(encoded[:0])
		logicalBytes += int64(len(encoded))
	}
	for _, variant := range []string{"generic", "descriptor", "generic-from-identity", "descriptor-from-identity"} {
		b.Run("build/"+variant, func(b *testing.B) {
			var resident int64
			b.ReportAllocs()
			b.SetBytes(logicalBytes)
			for b.Loop() {
				if variant == "generic" || variant == "generic-from-identity" {
					ix := New()
					for i := range full {
						if variant == "generic-from-identity" {
							prototypeRegistryID = ix.Add(inputs[i].ToSeries())
						} else {
							prototypeRegistryID = ix.Add(full[i])
						}
					}
					resident = ix.SizeBytes()
					continue
				}
				{
					ix := newPrototypeMetricRegistry(1024)
					for i := range inputs {
						if variant == "descriptor" {
							prototypeRegistryID = ix.addFull(inputs[i], full[i])
						} else {
							prototypeRegistryID = ix.add(inputs[i])
						}
					}
					resident = ix.sizeBytes()
				}
			}
			b.ReportMetric(float64(resident), "resident-B")
			b.ReportMetric(count, "series/op")
		})
	}
	generic, compact := New(), newPrototypeMetricRegistry(1024)
	for i := range full {
		generic.Add(full[i])
		compact.add(inputs[i])
	}
	for _, variant := range []string{"generic", "descriptor"} {
		b.Run("materialize/"+variant, func(b *testing.B) {
			var attrs signal.Attributes
			if variant == "generic" {
				attrs = generic.View().At(0, nil).Attributes
			} else {
				attrs = compact.at(0, nil).Attributes
			}
			b.ReportAllocs()
			b.SetBytes(logicalBytes)
			for b.Loop() {
				for i := range count {
					if variant == "generic" {
						prototypeRegistrySeries = generic.View().At(i, attrs)
					} else {
						prototypeRegistrySeries = compact.at(i, attrs)
					}
					attrs = prototypeRegistrySeries.Attributes
				}
			}
			b.ReportMetric(count, "series/op")
			if variant == "generic" {
				b.ReportMetric(float64(generic.SizeBytes()), "resident-B")
			} else {
				b.ReportMetric(float64(compact.sizeBytes()), "resident-B")
			}
		})
	}
}

func TestPrototypeMetricRegistryRoundTrip(t *testing.T) {
	t.Parallel()
	p := newPrototypeMetricRegistry(8)
	for i := range 100 {
		id := prototypeRegistryIdentity(i, 16)
		if i%7 == 0 {
			id.Series.Attributes = signal.NewAttributes(signal.KeyValue{Key: metric.LabelName, Value: signal.StringValue([]byte("collision"))})
		}
		full := id.ToSeries()
		ref := len(p.rows)
		canonical := p.add(id)
		require.Equal(t, full.Hash(), canonical)
		if ref == len(p.rows) {
			continue
		}
		require.Equal(t, canonical, p.base.ID(uint32(ref)))
		got := p.at(ref, nil)
		require.Equal(t, full, got)
		require.Equal(t, canonical, got.Hash())
	}
	require.LessOrEqual(t, len(p.descriptors), 8)
}

func FuzzPrototypeMetricRegistry(f *testing.F) {
	f.Add([]byte("name"), []byte("s"), []byte("cpu"), uint8(3))
	f.Add([]byte{}, []byte{}, []byte("__unit__"), uint8(0))
	f.Fuzz(func(t *testing.T, name, unit, key []byte, flags uint8) {
		if len(name)+len(unit)+len(key) > 4096 {
			return
		}
		p := newPrototypeMetricRegistry(2)
		for i := range 4 {
			ident := metric.Identity{Series: signal.Series{Resource: signal.Resource{SchemaURL: []byte{byte(i)}}, Scope: signal.Scope{Name: []byte("scope")}, Attributes: signal.NewAttributes(signal.KeyValue{Key: bytes.Clone(key), Value: signal.IntValue(int64(i))})}, Name: bytes.Clone(name), Unit: bytes.Clone(unit), Kind: metric.PointKind(flags % 2), Temporality: metric.Temporality(flags % 3), Monotonic: i%2 == 0}
			ref := len(p.rows)
			id := p.add(ident)
			if id != ident.SeriesID() {
				t.Fatal("canonical id changed")
			}
			if ref < len(p.rows) {
				got := p.at(ref, nil)
				want := ident.ToSeries()
				if !slices.EqualFunc(got.Attributes, want.Attributes, func(a, b signal.KeyValue) bool { return bytes.Equal(a.Key, b.Key) && a.Value.Equal(b.Value) }) || got.Hash() != id {
					t.Fatal("descriptor row changed identity")
				}
			}
		}
	})
}
