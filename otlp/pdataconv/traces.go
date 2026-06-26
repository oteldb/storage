package pdataconv

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/trace"
)

// AppendTraces converts an OTLP traces batch into dst, reusing dst's retained capacity (call
// [trace.Traces.Reset] or use [trace.GetTraces] for a recycled batch). Every span, event, and link
// is representable, so dropped is always 0; it is returned for symmetry with [AppendMetrics] and to
// leave room for future unrepresentable cases.
//
//nolint:dupl // per-signal OTLP converter; identical resource/scope walk, types differ
func AppendTraces(dst *trace.Traces, td ptrace.Traces) (dropped int) {
	rss := td.ResourceSpans()
	for i := range rss.Len() {
		srs := rss.At(i)

		rs := dst.AddResource()
		rs.Resource = signal.Resource{
			SchemaURL:  []byte(srs.SchemaUrl()),
			Attributes: convertMap(srs.Resource().Attributes()),
		}

		sss := srs.ScopeSpans()
		for j := range sss.Len() {
			sspans := sss.At(j)

			ss := rs.AddScope()
			ss.Scope = signal.Scope{
				Name:       []byte(sspans.Scope().Name()),
				Version:    []byte(sspans.Scope().Version()),
				SchemaURL:  []byte(sspans.SchemaUrl()),
				Attributes: convertMap(sspans.Scope().Attributes()),
			}

			spans := sspans.Spans()
			for k := range spans.Len() {
				appendSpan(ss, spans.At(k))
			}
		}
	}

	return dropped
}

func appendSpan(ss *trace.ScopeSpans, span ptrace.Span) {
	sp := ss.AddSpan()
	sp.Attributes = convertMap(span.Attributes())
	sp.TraceID = traceIDBytes(span.TraceID())
	sp.SpanID = spanIDBytes(span.SpanID())
	sp.ParentSpanID = spanIDBytes(span.ParentSpanID())
	sp.Name = []byte(span.Name())
	sp.StatusMessage = []byte(span.Status().Message())
	sp.TraceState = []byte(span.TraceState().AsRaw())
	sp.Start = int64(span.StartTimestamp())
	sp.End = int64(span.EndTimestamp())
	sp.Kind = int32(span.Kind())
	sp.StatusCode = int32(span.Status().Code())
	sp.Flags = span.Flags()
	sp.Dropped = span.DroppedAttributesCount()

	events := span.Events()
	for i := range events.Len() {
		ev := events.At(i)

		e := sp.AddEvent()
		e.Time = int64(ev.Timestamp())
		e.Name = []byte(ev.Name())
		e.Attributes = convertMap(ev.Attributes())
		e.Dropped = ev.DroppedAttributesCount()
	}

	links := span.Links()
	for i := range links.Len() {
		ln := links.At(i)

		l := sp.AddLink()
		l.TraceID = traceIDBytes(ln.TraceID())
		l.SpanID = spanIDBytes(ln.SpanID())
		l.TraceState = []byte(ln.TraceState().AsRaw())
		l.Attributes = convertMap(ln.Attributes())
		l.Dropped = ln.DroppedAttributesCount()
	}
}

// traceIDBytes copies a 16-byte OTLP trace id into an owned slice, returning nil when unset.
func traceIDBytes(id pcommon.TraceID) []byte {
	if id.IsEmpty() {
		return nil
	}

	return append([]byte(nil), id[:]...)
}

// spanIDBytes copies an 8-byte OTLP span id into an owned slice, returning nil when unset.
func spanIDBytes(id pcommon.SpanID) []byte {
	if id.IsEmpty() {
		return nil
	}

	return append([]byte(nil), id[:]...)
}
