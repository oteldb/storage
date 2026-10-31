package pdataconv

import (
	"bytes"
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/metric"
)

func typedAttributes(m pcommon.Map) {
	m.PutEmpty("empty")
	m.PutStr("string", "a\x00b")
	m.PutBool("boolean", true)
	m.PutInt("integer", math.MinInt64)
	m.PutDouble("negative-zero", math.Float64frombits(1<<63))
	m.PutDouble("nan", math.Float64frombits(0x7ff8000000000042))
	m.PutDouble("infinity", math.Inf(-1))
	m.PutEmptyBytes("bytes").FromRaw([]byte{0, 255, 1})
	array := m.PutEmptySlice("array")
	array.AppendEmpty().SetInt(1)
	nested := array.AppendEmpty().SetEmptyMap()
	nested.PutStr("z", "last")
	nested.PutStr("a", "first")
	nested.PutEmptySlice("nested").AppendEmpty().SetDouble(1.25)
	m.PutEmptyMap("map").PutBool("flag", false)
}

func projectorFixture() pmetric.Metrics {
	md := pmetric.NewMetrics()
	for ri := range 2 {
		rm := md.ResourceMetrics().AppendEmpty()
		rm.SetSchemaUrl("resource-schema")
		typedAttributes(rm.Resource().Attributes())
		rm.Resource().Attributes().PutInt("resource", int64(ri))
		for si := range 2 {
			sm := rm.ScopeMetrics().AppendEmpty()
			sm.SetSchemaUrl("scope-schema")
			sm.Scope().SetName("scope")
			sm.Scope().SetVersion("v1")
			typedAttributes(sm.Scope().Attributes())
			sm.Scope().Attributes().PutInt("scope", int64(si))
			for mi := range 4 {
				m := sm.Metrics().AppendEmpty()
				m.SetName("metric")
				m.SetUnit("seconds")
				points := m.SetEmptyGauge().DataPoints()
				if mi > 0 {
					sum := m.SetEmptySum()
					sum.SetAggregationTemporality(pmetric.AggregationTemporality(mi - 1))
					sum.SetIsMonotonic(mi%2 == 0)
					points = sum.DataPoints()
				}
				for pi := range 3 {
					point := points.AppendEmpty()
					point.SetTimestamp(pcommon.Timestamp(pi + 1))
					point.SetStartTimestamp(pcommon.Timestamp(pi))
					if pi == 1 {
						continue
					}
					if pi == 0 {
						point.SetIntValue(42)
					} else {
						point.SetDoubleValue(-0.5)
					}
					typedAttributes(point.Attributes())
					point.Attributes().PutInt("point", int64(pi))
					point.Attributes().PutStr("__name__", "incoming-name")
					point.Attributes().PutInt("__kind__", 99)
					point.Attributes().PutStr("!before", "reserved")
				}
			}
		}
	}
	return md
}

func assertProjection(t *testing.T, md pmetric.Metrics) {
	t.Helper()
	var native metric.Metrics
	dropped := AppendMetrics(&native, md)
	type expectedBatch struct {
		source  metric.Source
		ids     []signal.SeriesID
		samples []metric.Sample
	}
	var expected []expectedBatch
	metric.Project(native, func(b *metric.Batch) {
		e := expectedBatch{source: b.Source(), ids: append([]signal.SeriesID(nil), b.IDs...)}
		for i := range b.Len() {
			e.samples = append(e.samples, b.Sample(i))
		}
		expected = append(expected, e)
	})
	var p MetricsProjector
	require.True(t, p.Prepare(md))
	require.Equal(t, dropped, p.Dropped())
	var captures []metric.Source
	var index, points int
	emitted := p.Project(func(b *metric.Batch) {
		require.Less(t, index, len(expected))
		e := expected[index]
		require.Equal(t, e.ids, b.IDs)
		for i := range b.Len() {
			want := e.source.Identity(i)
			got := b.Identity(i)
			require.True(t, want.Series.Equal(got.Series))
			require.Equal(t, want.Name, got.Name)
			require.Equal(t, want.Unit, got.Unit)
			require.Equal(t, want.Kind, got.Kind)
			require.Equal(t, want.Temporality, got.Temporality)
			require.Equal(t, want.Monotonic, got.Monotonic)
			require.Equal(t, e.samples[i], b.Sample(i))
			require.Empty(t, b.Exemplars(i))
			require.Equal(t, want.ToSeries().AppendHashInput(nil), b.Series(i).AppendHashInput(nil))
		}
		captures = append(captures, b.Source())
		points += b.Len()
		index++
	})
	require.Len(t, expected, index)
	require.Equal(t, points, emitted)
	for i, source := range captures {
		for j, id := range expected[i].ids {
			require.Equal(t, id, source.Series(j).Hash())
		}
	}
	require.Zero(t, p.Project(func(*metric.Batch) { t.Fatal("projected twice") }))
	p.Reset()
	require.Empty(t, p.groups)
	require.Empty(t, p.metrics)
	require.Empty(t, p.hash.entries)
}

func TestMetricsProjectorParity(t *testing.T) {
	t.Parallel()
	assertProjection(t, projectorFixture())
}

func TestMetricsProjectorEligibility(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name string
		fill func(pmetric.Metric)
	}{
		{"empty", func(pmetric.Metric) {}},
		{"histogram", func(m pmetric.Metric) { m.SetEmptyHistogram() }},
		{"exponential", func(m pmetric.Metric) { m.SetEmptyExponentialHistogram() }},
		{"summary", func(m pmetric.Metric) { m.SetEmptySummary() }},
		{"exemplar", func(m pmetric.Metric) { p := m.SetEmptyGauge().DataPoints().AppendEmpty(); p.Exemplars().AppendEmpty() }},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			md := pmetric.NewMetrics()
			metrics := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics()
			metrics.AppendEmpty().SetEmptyGauge().DataPoints().AppendEmpty().SetIntValue(1)
			test.fill(metrics.AppendEmpty())
			var p MetricsProjector
			require.False(t, p.Prepare(md))
			require.Zero(t, p.Project(func(*metric.Batch) { t.Fatal("partial projection") }))
			require.True(t, p.Prepare(pmetric.NewMetrics()))
			require.Zero(t, p.Project(func(*metric.Batch) { t.Fatal("empty projection") }))
			require.Equal(t, Dropped{}, p.Dropped())
		})
	}
}

func TestMetricsHashCanonical(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name  string
		attrs func(pcommon.Map)
	}{
		{"empty", func(pcommon.Map) {}},
		{"typed", typedAttributes},
		{"empty-containers", func(m pcommon.Map) {
			m.PutEmptyMap("map")
			m.PutEmptySlice("slice")
			m.PutEmptyBytes("bytes")
		}},
		{"small-threshold", func(m pcommon.Map) { hashAttributes(8, 0).CopyTo(m) }},
		{"medium", func(m pcommon.Map) { hashAttributes(9, 0).CopyTo(m) }},
		{"threshold", func(m pcommon.Map) { hashAttributes(32, 0).CopyTo(m) }},
		{"fallback", func(m pcommon.Map) { hashAttributes(33, 0).CopyTo(m) }},
		{"nested-threshold", func(m pcommon.Map) { hashAttributes(2, 32).CopyTo(m) }},
		{"nested-stack", func(m pcommon.Map) { hashAttributes(32, 2).CopyTo(m) }},
		{"nested-fallback", func(m pcommon.Map) { hashAttributes(2, 33).CopyTo(m) }},
		{"fallback-parent", func(m pcommon.Map) { hashAttributes(33, 2).CopyTo(m) }},
		{"fallback-parent-child", func(m pcommon.Map) { hashAttributes(33, 33).CopyTo(m) }},
		{"reserved", func(m pcommon.Map) {
			for _, key := range metricKeys {
				m.PutStr(key, "point-value")
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			var h metricsHash
			m := pmetric.NewMetric()
			m.SetName("name")
			m.SetUnit("unit")
			m.SetEmptyGauge()
			attrs := pcommon.NewMap()
			test.attrs(attrs)
			h.setGroup(signal.Resource{}, signal.Scope{})
			h.setMetric(m)
			got := h.id(attrs)
			want := (metric.Identity{Name: []byte(m.Name()), Unit: []byte(m.Unit()), Series: signal.Series{Attributes: convertMap(attrs)}}).ToSeries()
			require.Equal(t, want.AppendHashInput(nil), h.buf)
			require.Equal(t, want.Hash(), got)
		})
	}
}

func FuzzMetricsHash(f *testing.F) {
	f.Add("__name__", "value", []byte{0, 1, 2, 3, 4, 5, 6, 7, 8}, uint64(0x7ff8000000000042))
	f.Add("!before", "a\x00b", []byte{8, 8, 8, 8, 1}, uint64(1<<63))
	f.Add("__unit__", "nested-fallback", bytes.Repeat([]byte{62}, 32), uint64(42))
	f.Fuzz(func(t *testing.T, key, value string, data []byte, bits uint64) {
		if len(key)+len(value)+len(data) > 4096 {
			t.Skip()
		}
		attrs := pcommon.NewMap()
		attrs.PutStr(key, value)
		attrs.PutDouble("double", math.Float64frombits(bits))
		slice := attrs.PutEmptySlice("nested")
		for i, b := range data {
			if i >= 32 {
				break
			}
			attrs.PutStr("attribute-"+strconv.Itoa(i), value)
			v := slice.AppendEmpty()
			switch b % 9 {
			case 0:
			case 1:
				v.SetStr(value)
			case 2:
				v.SetBool(b%2 == 0)
			case 3:
				v.SetInt(int64(bits))
			case 4:
				v.SetDouble(math.Float64frombits(bits))
			case 5:
				v.SetEmptyBytes().FromRaw(data)
			case 6:
				v.SetEmptySlice().AppendEmpty().SetStr(value)
			case 7:
				v.SetEmptyMap().PutStr(key, value)
			case 8:
				m := v.SetEmptyMap()
				for j := int(b)%65 - 1; j >= 0; j-- {
					m.PutInt("nested-"+strconv.Itoa(j), int64(j))
				}
			}
		}
		m := pmetric.NewMetric()
		m.SetName(value)
		m.SetUnit(key)
		sum := m.SetEmptySum()
		sum.SetIsMonotonic(bits%2 != 0)
		sum.SetAggregationTemporality(pmetric.AggregationTemporality(bits % 4))
		kind, temporality, monotonic := metricMetadata(m)
		want := (metric.Identity{Name: []byte(m.Name()), Unit: []byte(m.Unit()), Kind: kind, Temporality: temporality, Monotonic: monotonic, Series: signal.Series{Attributes: convertMap(attrs)}}).ToSeries()
		var h metricsHash
		h.setGroup(signal.Resource{}, signal.Scope{})
		h.setMetric(m)
		id := h.id(attrs)
		assert.Equal(t, want.AppendHashInput(nil), h.buf)
		assert.Equal(t, want.Hash(), id)
	})
}

func TestMetricsProjectorReset(t *testing.T) {
	t.Parallel()
	var p MetricsProjector
	require.True(t, p.Prepare(projectorFixture()))
	p.Project(func(b *metric.Batch) {
		for i := range b.Len() {
			b.Series(i)
		}
	})
	groups, metrics, entries := p.groups[:cap(p.groups)], p.metrics[:cap(p.metrics)], p.hash.entries[:cap(p.hash.entries)]
	p.Reset()
	for _, group := range groups {
		require.Equal(t, metricContext{}, group)
	}
	for _, m := range metrics {
		require.Equal(t, metricInput{}, m)
	}
	for _, entry := range entries {
		require.Equal(t, mapEntry{}, entry)
	}
	require.Zero(t, p.Project(func(*metric.Batch) { t.Fatal("reset projector") }))
	p.hash.buf = make([]byte, 0, maxProjectorRetainedBytes+1)
	p.Reset()
	require.Zero(t, p.retainedBytes())
	for _, md := range []pmetric.Metrics{projectorFixture(), pmetric.NewMetrics(), projectorFixture(), pmetric.NewMetrics()} {
		require.True(t, p.Prepare(md))
		p.Project(func(b *metric.Batch) {
			for i := range b.Len() {
				b.Series(i)
			}
		})
		p.Reset()
		for _, group := range p.groups[:cap(p.groups)] {
			require.Equal(t, metricContext{}, group)
		}
		for _, m := range p.metrics[:cap(p.metrics)] {
			require.Equal(t, metricInput{}, m)
		}
		for _, entry := range p.hash.entries[:cap(p.hash.entries)] {
			require.Equal(t, mapEntry{}, entry)
		}
		require.Equal(t, metric.Source{}, p.batch.Source())
		require.Equal(t, pmetric.Metrics{}, p.input)
	}
	require.True(t, p.Prepare(pmetric.NewMetrics()))
	require.Zero(t, p.Project(func(*metric.Batch) { t.Fatal("empty projector") }))
}
