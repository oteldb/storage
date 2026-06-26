// Package trace holds the traces signal's ingest model: the []byte-based, OTLP-shaped, zero-alloc
// batch accepted at the storage boundary (in place of OTel-Go ptrace.Traces), and its projection
// into the columnar span model the record engine ingests.
//
// A span is a record (mirroring signal/log): its stream identity is the producing Resource+Scope,
// and its per-record fields (name, kind, status, duration, trace/span ids, attributes, events,
// links) are columns the query filters by condition. Two trace-specific additions ride along as
// columns: the raw trace_id (with an equality bloom, so trace-by-id is a bloom-pruned equality
// fetch) and ingest-computed nested-set ids (so an embedder's TraceQL does ancestor/descendant as
// range comparisons). Events and links are serialized into byte columns.
package trace

import (
	"sync"

	"github.com/oteldb/storage/signal"
)

// Traces is the internal traces ingest batch — the OTLP Resource→Scope→Span hierarchy with all
// identity as []byte. Resettable and pool-friendly (see [GetTraces]/[PutTraces]); build it with the
// Add* helpers, which reuse retained capacity.
type Traces struct {
	Resources []ResourceSpans
}

// ResourceSpans groups the spans emitted under one [signal.Resource].
type ResourceSpans struct {
	Resource signal.Resource
	Scopes   []ScopeSpans
}

// ScopeSpans groups the spans emitted under one [signal.Scope]. A (Resource, Scope) pair is one
// span **stream**.
type ScopeSpans struct {
	Scope signal.Scope
	Spans []Span
}

// Span is a single OTLP span. Start/End are unix nanos (Start is the record's primary timestamp;
// End-Start is the stored duration). TraceID is 16 bytes, SpanID/ParentSpanID are 8 (or nil).
type Span struct {
	Attributes    signal.Attributes
	TraceID       []byte
	SpanID        []byte
	ParentSpanID  []byte
	Name          []byte
	StatusMessage []byte
	TraceState    []byte
	Start         int64
	End           int64
	Kind          int32
	StatusCode    int32
	Flags         uint32
	Dropped       uint32
	Events        []Event
	Links         []Link
}

// Event is a span event (a timestamped, named, attributed point within a span).
type Event struct {
	Attributes signal.Attributes
	Time       int64
	Name       []byte
	Dropped    uint32
}

// Link is a span link to another span (possibly in another trace).
type Link struct {
	Attributes signal.Attributes
	TraceID    []byte
	SpanID     []byte
	TraceState []byte
	Dropped    uint32
}

// Reset clears the batch for reuse, retaining backing arrays.
func (t *Traces) Reset() { t.Resources = t.Resources[:0] }

// AddResource appends a fresh [ResourceSpans] and returns a pointer to it (Resource zeroed, Scopes
// emptied with capacity retained).
func (t *Traces) AddResource() *ResourceSpans {
	t.Resources = grow(t.Resources)
	rs := &t.Resources[len(t.Resources)-1]
	rs.Resource = signal.Resource{}
	rs.Scopes = rs.Scopes[:0]

	return rs
}

// AddScope appends a fresh [ScopeSpans] under the resource.
func (rs *ResourceSpans) AddScope() *ScopeSpans {
	rs.Scopes = grow(rs.Scopes)
	ss := &rs.Scopes[len(rs.Scopes)-1]
	ss.Scope = signal.Scope{}
	ss.Spans = ss.Spans[:0]

	return ss
}

// AddSpan appends a fresh, fully-zeroed [Span] under the scope.
func (ss *ScopeSpans) AddSpan() *Span {
	ss.Spans = grow(ss.Spans)
	sp := &ss.Spans[len(ss.Spans)-1]
	*sp = Span{}

	return sp
}

// AddEvent appends a zeroed [Event] to the span.
func (sp *Span) AddEvent() *Event {
	sp.Events = grow(sp.Events)
	e := &sp.Events[len(sp.Events)-1]
	*e = Event{}

	return e
}

// AddLink appends a zeroed [Link] to the span.
func (sp *Span) AddLink() *Link {
	sp.Links = grow(sp.Links)
	l := &sp.Links[len(sp.Links)-1]
	*l = Link{}

	return l
}

// grow extends s by one element, reusing the retained backing array when len < cap.
func grow[T any](s []T) []T {
	if len(s) < cap(s) {
		return s[:len(s)+1]
	}

	var zero T

	return append(s, zero)
}

var tracesPool = sync.Pool{New: func() any { return &Traces{} }}

// GetTraces returns a reset [Traces] from a shared pool; pair with [PutTraces].
func GetTraces() *Traces {
	t, _ := tracesPool.Get().(*Traces)

	return t
}

// PutTraces resets t and returns it to the pool. Do not use t afterward.
func PutTraces(t *Traces) {
	t.Reset()
	tracesPool.Put(t)
}
