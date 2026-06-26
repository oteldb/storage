package trace

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/signal"
)

// spanCols collects, from a one-stream projection, each span's columns keyed by span id.
type spanRow struct {
	start, duration, kind, status int64
	parentID, nsetLeft, nsetRight int64
	name, traceID, parentSpanID   []byte
	events, links                 []byte
	attrs                         []byte
}

func project1(t *testing.T, td Traces) map[string]spanRow {
	t.Helper()

	rows := map[string]spanRow{}
	n := Project(td, func(b *recordengine.Batch) {
		for i := range b.Ts {
			id := string(b.Bytes[bSpanID][i])
			rows[id] = spanRow{
				start: b.Ts[i], duration: b.Ints[iDuration][i], kind: b.Ints[iKind][i], status: b.Ints[iStatusCode][i],
				parentID: b.Ints[iParentID][i], nsetLeft: b.Ints[iNestedLeft][i], nsetRight: b.Ints[iNestedRight][i],
				name: b.Bytes[bName][i], traceID: b.Bytes[bTraceID][i], parentSpanID: b.Bytes[bParentSpanID][i],
				events: b.Bytes[bEvents][i], links: b.Bytes[bLinks][i], attrs: b.Bytes[bAttrs][i],
			}
		}
	})
	require.Positive(t, n)

	return rows
}

// mkSpan adds a span to ss.
func mkSpan(ss *ScopeSpans, traceID, spanID, parentSpanID, name string, start, end int64) *Span {
	sp := ss.AddSpan()
	sp.TraceID, sp.SpanID, sp.ParentSpanID = []byte(traceID), []byte(spanID), []byte(parentSpanID)
	sp.Name = []byte(name)
	sp.Start, sp.End = start, end

	return sp
}

func TestProjectFillsSpanColumns(t *testing.T) {
	t.Parallel()

	var td Traces
	rs := td.AddResource()
	rs.Resource = signal.Resource{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte("api"))},
	)}
	ss := rs.AddScope()
	sp := mkSpan(ss, "trace1", "span1", "", "GET /x", 100, 250)
	sp.Kind, sp.StatusCode = 2, 1
	sp.Attributes = signal.NewAttributes(signal.KeyValue{Key: []byte("http.method"), Value: signal.StringValue([]byte("GET"))})

	rows := project1(t, td)
	r := rows["span1"]
	assert.Equal(t, int64(100), r.start)
	assert.Equal(t, int64(150), r.duration, "End-Start")
	assert.Equal(t, int64(2), r.kind)
	assert.Equal(t, int64(1), r.status)
	assert.Equal(t, []byte("GET /x"), r.name)
	assert.Equal(t, []byte("trace1"), r.traceID)

	decoded, _, err := signal.DecodeAttributes(r.attrs)
	require.NoError(t, err)
	v, ok := decoded.Get([]byte("http.method"))
	require.True(t, ok)
	assert.Equal(t, []byte("GET"), v.Str())
}

func TestNestedSetFormsValidTree(t *testing.T) {
	t.Parallel()

	// root → c1 → g ; root → c2 (c1 before c2 by start). Spans span two services (resources).
	var td Traces
	a := td.AddResource()
	a.Resource = signal.Resource{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte("a"))},
	)}
	as := a.AddScope()
	mkSpan(as, "T", "root", "", "root", 100, 500)
	mkSpan(as, "T", "c1", "root", "c1", 110, 300)

	b := td.AddResource()
	b.Resource = signal.Resource{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte("b"))},
	)}
	bs := b.AddScope()
	mkSpan(bs, "T", "g", "c1", "g", 120, 200)
	mkSpan(bs, "T", "c2", "root", "c2", 130, 400)

	rows := project1(t, td)
	root, c1, c2, g := rows["root"], rows["c1"], rows["c2"], rows["g"]

	// Validity: left < right for each.
	for id, r := range rows {
		assert.Lessf(t, r.nsetLeft, r.nsetRight, "%s left<right", id)
	}

	// Containment: descendant iff a.left<d.left && d.right<a.right.
	contains := func(anc, d spanRow) bool { return anc.nsetLeft < d.nsetLeft && d.nsetRight < anc.nsetRight }
	assert.True(t, contains(root, c1), "root ⊃ c1")
	assert.True(t, contains(root, g), "root ⊃ g (cross-service)")
	assert.True(t, contains(root, c2), "root ⊃ c2")
	assert.True(t, contains(c1, g), "c1 ⊃ g")
	assert.False(t, contains(c1, c2), "siblings are disjoint")
	assert.False(t, contains(c2, g), "c2 does not contain g")

	// parent_id is the parent's left; root's is 0.
	assert.Equal(t, int64(0), root.parentID)
	assert.Equal(t, root.nsetLeft, c1.parentID)
	assert.Equal(t, root.nsetLeft, c2.parentID)
	assert.Equal(t, c1.nsetLeft, g.parentID)
}

func TestNestedSetOrphanIsRoot(t *testing.T) {
	t.Parallel()

	// A span whose parent is absent from the batch is treated as a root (no panic, valid ids).
	var td Traces
	rs := td.AddResource()
	ss := rs.AddScope()
	mkSpan(ss, "T", "orphan", "missing-parent", "orphan", 100, 200)

	rows := project1(t, td)
	r := rows["orphan"]
	assert.Equal(t, int64(0), r.parentID, "orphan treated as a root")
	assert.Less(t, r.nsetLeft, r.nsetRight)
}

func TestEventsAndLinksRoundTrip(t *testing.T) {
	t.Parallel()

	var td Traces
	rs := td.AddResource()
	ss := rs.AddScope()
	sp := mkSpan(ss, "T", "s", "", "op", 100, 200)

	e := sp.AddEvent()
	e.Time, e.Name, e.Dropped = 150, []byte("exception"), 2
	e.Attributes = signal.NewAttributes(signal.KeyValue{Key: []byte("level"), Value: signal.StringValue([]byte("error"))})

	l := sp.AddLink()
	l.TraceID, l.SpanID, l.TraceState = []byte("other-trace"), []byte("other-span"), []byte("ts")
	l.Attributes = signal.NewAttributes(signal.KeyValue{Key: []byte("rel"), Value: signal.StringValue([]byte("follows"))})

	rows := project1(t, td)
	r := rows["s"]

	evs, err := DecodeEvents(r.events)
	require.NoError(t, err)
	require.Len(t, evs, 1)
	assert.Equal(t, int64(150), evs[0].Time)
	assert.Equal(t, []byte("exception"), evs[0].Name)
	assert.Equal(t, uint32(2), evs[0].Dropped)
	lv, ok := evs[0].Attributes.Get([]byte("level"))
	require.True(t, ok)
	assert.Equal(t, []byte("error"), lv.Str())

	links, err := DecodeLinks(r.links)
	require.NoError(t, err)
	require.Len(t, links, 1)
	assert.Equal(t, []byte("other-trace"), links[0].TraceID)
	assert.Equal(t, []byte("ts"), links[0].TraceState)
}

func TestDecodeEventsLinksTruncated(t *testing.T) {
	t.Parallel()

	_, err := DecodeEvents([]byte{0x05, 0xff})
	require.Error(t, err)
	_, err = DecodeLinks([]byte{0x05, 0xff})
	require.Error(t, err)
}

func TestGetPutTracesRecycles(t *testing.T) {
	t.Parallel()

	td := GetTraces()
	td.AddResource().AddScope().AddSpan().Name = []byte("x")
	PutTraces(td)

	td2 := GetTraces()
	assert.Empty(t, td2.Resources)
	PutTraces(td2)
}
