package profile

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/signal"
)

// collect runs Project and returns a clone of every emitted batch (Project reuses one Batch).
func collect(pd *Profiles) []*recordengine.Batch {
	var out []*recordengine.Batch

	Project(pd, func(b *recordengine.Batch) {
		c := &recordengine.Batch{Stream: b.Stream, Side: append([]byte(nil), b.Side...)}
		c.Ts = append(c.Ts, b.Ts...)
		for _, col := range b.Ints {
			c.Ints = append(c.Ints, append([]int64(nil), col...))
		}

		for _, col := range b.Bytes {
			dup := make([][]byte, len(col))
			for i, v := range col {
				dup[i] = append([]byte(nil), v...)
			}

			c.Bytes = append(c.Bytes, dup)
		}

		out = append(out, c)
	})

	return out
}

func svcResource(name string) signal.Resource {
	return signal.Resource{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte(name))},
	)}
}

// TestProjectFlattensSamples verifies the schema column counts, sample explosion (timestamped vs
// aggregated), denormalized profile fields, content-addressed stack ids, and a non-empty symbol
// delta.
func TestProjectFlattensSamples(t *testing.T) {
	t.Parallel()

	var pd Profiles
	d := &pd.Dictionary
	st := buildStack(d, "main", "main.go")
	cpu := ValueType{TypeStrindex: d.InternString([]byte("cpu")), UnitStrindex: d.InternString([]byte("nanoseconds"))}

	rp := pd.AddResource()
	rp.Resource = svcResource("api")
	pr := rp.AddScope().AddProfile()
	pr.SampleType = cpu
	pr.TimeNanos = 1000
	pr.Period = 7
	pr.DurationNanos = 99

	// One aggregated sample (no timestamps) and one time-series sample (two observations).
	agg := pr.AddSample()
	agg.StackIndex = st
	agg.Values = []int64{42}

	ts := pr.AddSample()
	ts.StackIndex = st
	ts.Values = []int64{5, 6}
	ts.TimestampsUnixNano = []uint64{2000, 3000}

	batches := collect(&pd)
	require.Len(t, batches, 1)
	b := batches[0]

	require.Len(t, b.Ints, 3)
	require.Len(t, b.Bytes, 5)
	require.Len(t, b.Ts, 3, "1 aggregated + 2 timestamped rows")

	assert.Equal(t, []int64{1000, 2000, 3000}, b.Ts)
	assert.Equal(t, []int64{42, 5, 6}, b.Ints[iValue])
	assert.Equal(t, []int64{7, 7, 7}, b.Ints[iPeriod], "period denormalized onto every row")
	assert.Equal(t, []int64{99, 99, 99}, b.Ints[iDuration])

	// Same stack ⇒ identical 16-byte content id on every row.
	want := newBuilder(d).stackID(st).AppendBinary(nil)
	for _, got := range b.Bytes[bStackID] {
		assert.Equal(t, want, got)
	}

	// The profile type is folded into the stream identity as reserved labels.
	series := streamSeries(svcResource("api"), signal.Scope{}, resolveType(d, pr))
	assert.Equal(t, series.Hash(), b.Stream)
	v, ok := series.Resource.Attributes.Get(LabelSampleType)
	require.True(t, ok)
	assert.Equal(t, "cpu", string(v.Str()))

	// The symbol delta is present and decodes.
	require.NotEmpty(t, b.Side)
	require.NoError(t, NewSymbolStore().Absorb(b.Side))
}

// TestProjectStreamsAndAttributes verifies one batch per Resource+Scope stream and that resolved
// per-sample attributes land in the attrs column (via the shared hash-input form).
func TestProjectStreamsAndAttributes(t *testing.T) {
	t.Parallel()

	var pd Profiles
	d := &pd.Dictionary
	st := buildStack(d, "f", "f.go")
	threadAttr := d.AddAttribute(KeyValueAndUnit{
		KeyStrindex: d.InternString([]byte("thread.name")),
		Value:       signal.StringValue([]byte("worker")),
	})

	for _, svc := range []string{"api", "web"} {
		rp := pd.AddResource()
		rp.Resource = svcResource(svc)
		pr := rp.AddScope().AddProfile()
		pr.TimeNanos = 1
		s := pr.AddSample()
		s.StackIndex = st
		s.Values = []int64{1}
		s.AttributeIndices = []int32{threadAttr}
	}

	batches := collect(&pd)
	require.Len(t, batches, 2, "one stream per service")

	want := signal.NewAttributes(
		signal.KeyValue{Key: []byte("thread.name"), Value: signal.StringValue([]byte("worker"))},
	).AppendHashInput(nil)
	assert.Equal(t, want, batches[0].Bytes[bAttrs][0])
}

// TestModelBuildersAndPool exercises the dictionary Add helpers, the pool round-trip, and Reset.
func TestModelBuildersAndPool(t *testing.T) {
	t.Parallel()

	pd := GetProfiles()
	d := &pd.Dictionary
	m := d.AddMapping(Mapping{FilenameStrindex: d.InternString([]byte("libc.so"))})
	loc := d.AddLocation(Location{MappingIndex: m, Address: 0x1000})
	st := d.AddStack(loc)
	link := d.AddLink(Link{TraceID: []byte("trace"), SpanID: []byte("span")})

	rp := pd.AddResource()
	rp.Resource = svcResource("api")
	pr := rp.AddScope().AddProfile()
	pr.TimeNanos = 1
	s := pr.AddSample()
	s.StackIndex, s.LinkIndex, s.Values = st, link, []int64{1}

	require.Len(t, collect(pd), 1)

	// Reset clears every table and the hierarchy; the pool reuses the backing arrays.
	PutProfiles(pd)
	got := GetProfiles()
	assert.Empty(t, got.Resources)
	assert.Empty(t, got.Dictionary.Strings)
	PutProfiles(got)
}

// TestProjectLinkAndMapping verifies a sample's link resolves into the trace/span id columns and a
// mapped location round-trips through the symbol delta.
func TestProjectLinkAndMapping(t *testing.T) {
	t.Parallel()

	var pd Profiles
	d := &pd.Dictionary
	m := d.AddMapping(Mapping{FilenameStrindex: d.InternString([]byte("app"))})
	loc := d.AddLocation(Location{MappingIndex: m, Lines: []Line{{FunctionIndex: d.AddFunction(Function{NameStrindex: d.InternString([]byte("f"))})}}})
	st := d.AddStack(loc)
	link := d.AddLink(Link{TraceID: []byte("T"), SpanID: []byte("S")})

	rp := pd.AddResource()
	rp.Resource = svcResource("api")
	pr := rp.AddScope().AddProfile()
	pr.TimeNanos = 1
	s := pr.AddSample()
	s.StackIndex, s.LinkIndex, s.Values = st, link, []int64{1}

	b := collect(&pd)[0]
	assert.Equal(t, [][]byte{[]byte("T")}, b.Bytes[bTraceID])
	assert.Equal(t, [][]byte{[]byte("S")}, b.Bytes[bSpanID])

	// The mapping is recorded in the symbol delta's mappings table.
	store := NewSymbolStore()
	require.NoError(t, store.Absorb(b.Side))
	mappings := map[signal.SeriesID][]byte{}
	require.NoError(t, decodeTable(mappings, store.Encode()["mappings"]))
	assert.Len(t, mappings, 1)
}

// TestProjectMalformedIndicesNoPanic feeds out-of-range dictionary indices and asserts Project still
// produces rows without panicking (defensive resolution).
func TestProjectMalformedIndicesNoPanic(t *testing.T) {
	t.Parallel()

	var pd Profiles
	rp := pd.AddResource()
	rp.Resource = svcResource("api")
	pr := rp.AddScope().AddProfile()
	pr.TimeNanos = 1
	s := pr.AddSample()
	s.StackIndex = 999 // no such stack
	s.LinkIndex = 999
	s.AttributeIndices = []int32{999}
	s.Values = []int64{1}

	batches := collect(&pd)
	require.Len(t, batches, 1)
	assert.Len(t, batches[0].Ts, 1)
}
