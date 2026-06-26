package pdataconv

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/oteldb/storage/signal/trace"
)

func TestAppendTraces(t *testing.T) {
	t.Parallel()

	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.SetSchemaUrl("https://schema/res")
	rs.Resource().Attributes().PutStr("service.name", "api")
	ss := rs.ScopeSpans().AppendEmpty()
	ss.Scope().SetName("lib")
	ss.Scope().SetVersion("v1")

	traceID := pcommon.TraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	spanID := pcommon.SpanID([8]byte{1, 2, 3, 4, 5, 6, 7, 8})
	parentID := pcommon.SpanID([8]byte{8, 7, 6, 5, 4, 3, 2, 1})

	span := ss.Spans().AppendEmpty()
	span.SetName("GET /x")
	span.SetTraceID(traceID)
	span.SetSpanID(spanID)
	span.SetParentSpanID(parentID)
	span.SetKind(ptrace.SpanKindServer)
	span.SetStartTimestamp(pcommon.Timestamp(100))
	span.SetEndTimestamp(pcommon.Timestamp(250))
	span.SetFlags(7)
	span.TraceState().FromRaw("k=v")
	span.Status().SetCode(ptrace.StatusCodeError)
	span.Status().SetMessage("boom")
	span.Attributes().PutStr("http.method", "GET")

	ev := span.Events().AppendEmpty()
	ev.SetName("exception")
	ev.SetTimestamp(pcommon.Timestamp(150))
	ev.Attributes().PutStr("k", "v")

	ln := span.Links().AppendEmpty()
	ln.SetTraceID(traceID)
	ln.SetSpanID(spanID)
	ln.TraceState().FromRaw("a=b")
	ln.Attributes().PutInt("n", 1)

	var out trace.Traces
	require.Equal(t, 0, AppendTraces(&out, td))

	require.Len(t, out.Resources, 1)
	res := out.Resources[0]
	assert.Equal(t, []byte("https://schema/res"), res.Resource.SchemaURL)
	rv, _ := res.Resource.Attributes.Get([]byte("service.name"))
	assert.Equal(t, []byte("api"), rv.Str())

	require.Len(t, res.Scopes, 1)
	scope := res.Scopes[0]
	assert.Equal(t, []byte("lib"), scope.Scope.Name)
	assert.Equal(t, []byte("v1"), scope.Scope.Version)

	require.Len(t, scope.Spans, 1)
	sp := scope.Spans[0]
	assert.Equal(t, []byte("GET /x"), sp.Name)
	assert.Equal(t, traceID[:], sp.TraceID)
	assert.Equal(t, spanID[:], sp.SpanID)
	assert.Equal(t, parentID[:], sp.ParentSpanID)
	assert.Equal(t, int32(ptrace.SpanKindServer), sp.Kind)
	assert.Equal(t, int64(100), sp.Start)
	assert.Equal(t, int64(250), sp.End)
	assert.Equal(t, uint32(7), sp.Flags)
	assert.Equal(t, []byte("k=v"), sp.TraceState)
	assert.Equal(t, int32(ptrace.StatusCodeError), sp.StatusCode)
	assert.Equal(t, []byte("boom"), sp.StatusMessage)
	av, _ := sp.Attributes.Get([]byte("http.method"))
	assert.Equal(t, []byte("GET"), av.Str())

	require.Len(t, sp.Events, 1)
	assert.Equal(t, []byte("exception"), sp.Events[0].Name)
	assert.Equal(t, int64(150), sp.Events[0].Time)

	require.Len(t, sp.Links, 1)
	assert.Equal(t, traceID[:], sp.Links[0].TraceID)
	assert.Equal(t, spanID[:], sp.Links[0].SpanID)
	assert.Equal(t, []byte("a=b"), sp.Links[0].TraceState)
}

func TestAppendTracesEmptyIDs(t *testing.T) {
	t.Parallel()

	td := ptrace.NewTraces()
	td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()

	var out trace.Traces
	require.Equal(t, 0, AppendTraces(&out, td))

	sp := out.Resources[0].Scopes[0].Spans[0]
	assert.Nil(t, sp.TraceID)
	assert.Nil(t, sp.SpanID)
	assert.Nil(t, sp.ParentSpanID)
}
