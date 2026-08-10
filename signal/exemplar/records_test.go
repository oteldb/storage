package exemplar

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/metric"
)

func attrs(kvs ...signal.KeyValue) signal.Attributes { return signal.NewAttributes(kvs...) }

func str(k, v string) signal.KeyValue {
	return signal.KeyValue{Key: []byte(k), Value: signal.StringValue([]byte(v))}
}

// corpus builds a one-resource, one-scope, one-metric batch whose points carry the given exemplar
// counts (index i of counts is point i).
func corpus(counts ...int) metric.Metrics {
	var md metric.Metrics

	rm := md.AddResource()
	rm.Resource = signal.Resource{Attributes: attrs(str("service.name", "api"))}

	sm := rm.AddScope()
	sm.Scope = signal.Scope{Name: []byte("scope")}

	mt := sm.AddMetric()
	mt.Name = []byte("http.server.duration")
	mt.Unit = []byte("ms")
	mt.Kind = metric.KindGauge

	for i, n := range counts {
		p := mt.AddPoint()
		p.Attributes = attrs(str("http.route", string(rune('a'+i))))
		p.Ts = int64(1000 + i)
		p.Value = float64(i)

		for j := range n {
			e := p.AddExemplar()
			e.Ts = int64(2000 + i*10 + j)
			e.Value = float64(i) + float64(j)/8
			e.TraceID = []byte{byte(i), byte(j), 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
			e.SpanID = []byte{byte(i), byte(j), 2, 3, 4, 5, 6, 7}
			e.FilteredAttributes = attrs(str("peer.service", "db"))
		}
	}

	return md
}

// row is one flattened projected exemplar, with the stream it was emitted under.
type row struct {
	stream   signal.SeriesID
	identity signal.Series
	ts       int64
	value    float64
	traceID  []byte
	spanID   []byte
	attrs    signal.Attributes
}

// collect runs the metric projection and, per emitted metric batch, the exemplar projection,
// flattening every exemplar row. It also returns the metric-side SeriesID of each point, so a test
// can tie the two together.
func collect(tb testing.TB, md metric.Metrics) (rows []row, pointIDs []signal.SeriesID, accepted int) {
	tb.Helper()

	metric.Project(md, func(mb *metric.Batch) {
		pointIDs = append(pointIDs, mb.IDs...)

		accepted += Project(mb, func(b *recordengine.Batch) {
			require.Len(tb, b.Ints, 1)
			require.Len(tb, b.Bytes, 3)

			identity := b.Identity()

			for i := range b.Len() {
				a, _, err := signal.DecodeAttributes(b.Bytes[bAttrs][i])
				require.NoError(tb, err)

				rows = append(rows, row{
					stream:   b.Stream,
					identity: identity.Clone(),
					ts:       b.Ts[i],
					value:    DecodeValue(b.Ints[iValue][i]),
					traceID:  append([]byte(nil), b.Bytes[bTraceID][i]...),
					spanID:   append([]byte(nil), b.Bytes[bSpanID][i]...),
					attrs:    a.Clone(),
				})
			}
		})
	})

	return rows, pointIDs, accepted
}

// The invariant the whole design rests on: an exemplar's stream id is the metric series id of the
// point it hangs off, and the lazily materialized identity hashes to that same id.
func TestProjectStreamIsMetricSeriesID(t *testing.T) {
	t.Parallel()

	rows, pointIDs, accepted := collect(t, corpus(2, 1, 3))
	require.Equal(t, 6, accepted)
	require.Len(t, rows, 6)
	require.Len(t, pointIDs, 3)

	// Rows are emitted point by point, in point order: 2 then 1 then 3.
	want := []signal.SeriesID{pointIDs[0], pointIDs[0], pointIDs[1], pointIDs[2], pointIDs[2], pointIDs[2]}
	for i, r := range rows {
		assert.Equal(t, want[i], r.stream, "row %d stream", i)
		assert.Equal(t, r.stream, r.identity.Hash(), "row %d identity hash", i)
	}
}

// The materialized identity carries the metric's reserved labels, so a matcher over __name__ (or
// any point attribute) selects exemplar streams through the ordinary index.
func TestProjectIdentityCarriesMetricLabels(t *testing.T) {
	t.Parallel()

	rows, _, _ := collect(t, corpus(1))
	require.Len(t, rows, 1)

	id := rows[0].identity

	name, ok := id.Attributes.Get(metric.LabelName)
	require.True(t, ok)
	assert.Equal(t, "http.server.duration", string(name.Str()))

	unit, ok := id.Attributes.Get(metric.LabelUnit)
	require.True(t, ok)
	assert.Equal(t, "ms", string(unit.Str()))

	route, ok := id.Attributes.Get([]byte("http.route"))
	require.True(t, ok)
	assert.Equal(t, "a", string(route.Str()))

	assert.Equal(t, "api", func() string {
		v, _ := id.Resource.Attributes.Get([]byte("service.name"))

		return string(v.Str())
	}())
}

func TestProjectColumns(t *testing.T) {
	t.Parallel()

	rows, _, _ := collect(t, corpus(0, 2))
	require.Len(t, rows, 2)

	assert.Equal(t, int64(2010), rows[0].ts)
	assert.InDelta(t, 1.0, rows[0].value, 0)
	assert.Equal(t, []byte{1, 0, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, rows[0].traceID)
	assert.Equal(t, []byte{1, 0, 2, 3, 4, 5, 6, 7}, rows[0].spanID)

	peer, ok := rows[0].attrs.Get([]byte("peer.service"))
	require.True(t, ok)
	assert.Equal(t, "db", string(peer.Str()))

	assert.Equal(t, int64(2011), rows[1].ts)
	assert.InDelta(t, 1.125, rows[1].value, 0)
}

func TestProjectSkipsPointsWithoutExemplars(t *testing.T) {
	t.Parallel()

	rows, pointIDs, accepted := collect(t, corpus(0, 0, 0))
	assert.Zero(t, accepted)
	assert.Empty(t, rows)
	assert.Len(t, pointIDs, 3, "the metric points themselves are still projected")
}

func TestProjectEmptyBatch(t *testing.T) {
	t.Parallel()

	rows, _, accepted := collect(t, metric.Metrics{})
	assert.Zero(t, accepted)
	assert.Empty(t, rows)
}

// Exemplars with no trace context (the producer had no active span) project as empty id cells
// rather than being dropped.
func TestProjectEmptyTraceContext(t *testing.T) {
	t.Parallel()

	var md metric.Metrics

	rm := md.AddResource()
	sm := rm.AddScope()
	mt := sm.AddMetric()
	mt.Name = []byte("m")

	p := mt.AddPoint()
	p.Ts = 1
	_ = p.AddExemplar()

	rows, _, accepted := collect(t, md)
	require.Equal(t, 1, accepted)
	require.Len(t, rows, 1)
	assert.Empty(t, rows[0].traceID)
	assert.Empty(t, rows[0].spanID)
	assert.Empty(t, rows[0].attrs)
}

func TestValueRoundTrip(t *testing.T) {
	t.Parallel()

	for _, v := range []float64{
		0, -0, 1, -1, 0.1, 1e308, -1e308, math.SmallestNonzeroFloat64,
		math.MaxFloat64, math.Inf(1), math.Inf(-1),
	} {
		assert.Equal(t, math.Float64bits(v), math.Float64bits(DecodeValue(EncodeValue(v))), "%v", v)
	}

	assert.True(t, math.IsNaN(DecodeValue(EncodeValue(math.NaN()))))
}

func FuzzValueRoundTrip(f *testing.F) {
	for _, v := range []float64{0, 1, -1, 0.1, math.MaxFloat64, math.Inf(-1)} {
		f.Add(v)
	}

	f.Fuzz(func(t *testing.T, v float64) {
		if got := DecodeValue(EncodeValue(v)); math.Float64bits(got) != math.Float64bits(v) {
			t.Fatalf("round trip: got %v want %v", got, v)
		}
	})
}

// The projector is pooled and its column buffers reused across calls; a second pass must not see
// the first pass's rows.
func TestProjectReusesBuffers(t *testing.T) {
	t.Parallel()

	first, _, _ := collect(t, corpus(3))
	require.Len(t, first, 3)

	second, _, accepted := collect(t, corpus(1))
	require.Equal(t, 1, accepted)
	require.Len(t, second, 1)
}

func BenchmarkProject(b *testing.B) {
	md := corpus(1, 1, 1, 1, 1, 1, 1, 1)

	b.ReportAllocs()

	for b.Loop() {
		metric.Project(md, func(mb *metric.Batch) {
			Project(mb, func(*recordengine.Batch) {})
		})
	}
}
