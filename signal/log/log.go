// Package log holds the logs signal's ingest model: the []byte-based, OTLP-shaped, zero-alloc
// batch accepted at the storage boundary (in place of OTel-Go plog.Logs), and its projection
// into the columnar log-record model the logs engine ingests.
//
// It mirrors signal/metric: a resettable, pool-friendly Resource→Scope→Record hierarchy holding
// all identity as []byte (never string), so an embedder decoding OTLP can build it by aliasing
// the decode buffer, and projecting copies nothing. The difference from metrics is the data
// shape: a metric is a stream of (ts, float) samples, a log is a stream of rich records (time,
// severity, body, attributes, trace context). Identity (the stream labels: Resource+Scope) is
// indexed like a metric series; the per-record fields are columns the query filters by condition.
package log

import (
	"sync"

	"github.com/oteldb/storage/signal"
)

// Logs is the internal logs ingest batch — the OTLP Resource→Scope→Record hierarchy with all
// identity as []byte. It is resettable and pool-friendly: [Logs.Reset] keeps every backing array,
// so a batch from [GetLogs] returned with [PutLogs] recycles across ingest calls with no
// allocation. Build it with the Add* helpers, which reuse the retained capacity.
type Logs struct {
	Resources []ResourceLogs
}

// ResourceLogs groups the log records emitted under one [signal.Resource].
type ResourceLogs struct {
	Resource signal.Resource
	Scopes   []ScopeLogs
}

// ScopeLogs groups the log records emitted under one [signal.Scope] (InstrumentationScope). A
// (Resource, Scope) pair is one log **stream**.
type ScopeLogs struct {
	Scope   signal.Scope
	Records []Record
}

// Record is a single OTLP log record. Body is the record body's text (a string AnyValue held
// as raw bytes; non-string bodies are rendered to text by the producer). TraceID/SpanID are the
// raw 16- and 8-byte ids (nil if unset). A record present in a [Logs] batch is well-formed by
// construction.
type Record struct {
	// Attributes are the record's attributes. They must be sorted by key (use
	// [signal.NewAttributes]); the engine serializes them in order into the attrs column.
	Attributes        signal.Attributes
	Timestamp         int64 // unix nanos; the record's event time (the part sort key)
	ObservedTimestamp int64 // unix nanos; when the collector observed it (0 if unset)
	SeverityNumber    int32 // OTLP severity number (1..24; 0 unspecified)
	SeverityText      []byte
	Body              []byte
	TraceID           []byte // 16 bytes, or nil
	SpanID            []byte // 8 bytes, or nil
	Flags             uint32
	Dropped           uint32 // dropped_attributes_count
}

// Reset clears the batch for reuse while retaining the capacity of all backing arrays.
func (l *Logs) Reset() { l.Resources = l.Resources[:0] }

// AddResource appends a fresh [ResourceLogs] and returns a pointer to it. The slot's Resource is
// zeroed and its Scopes emptied (capacity retained).
func (l *Logs) AddResource() *ResourceLogs {
	l.Resources = grow(l.Resources)
	rl := &l.Resources[len(l.Resources)-1]
	rl.Resource = signal.Resource{}
	rl.Scopes = rl.Scopes[:0]

	return rl
}

// AddScope appends a fresh [ScopeLogs] under the resource and returns a pointer to it. The slot's
// Scope is zeroed and its Records emptied (capacity retained).
func (rl *ResourceLogs) AddScope() *ScopeLogs {
	rl.Scopes = grow(rl.Scopes)
	sl := &rl.Scopes[len(rl.Scopes)-1]
	sl.Scope = signal.Scope{}
	sl.Records = sl.Records[:0]

	return sl
}

// AddRecord appends a fresh [Record] under the scope and returns a pointer to it for the caller
// to populate. The slot is fully zeroed.
func (sl *ScopeLogs) AddRecord() *Record {
	sl.Records = grow(sl.Records)
	r := &sl.Records[len(sl.Records)-1]
	*r = Record{}

	return r
}

// grow extends s by one element, reusing the retained backing array when len < cap (the
// resettable-arena trick) and only allocating when the slice is at capacity.
func grow[T any](s []T) []T {
	if len(s) < cap(s) {
		return s[:len(s)+1]
	}

	var zero T

	return append(s, zero)
}

var logsPool = sync.Pool{New: func() any { return &Logs{} }}

// GetLogs returns a reset [Logs] from a shared pool. Pair it with [PutLogs] to recycle the batch
// (and its backing arrays) across ingest calls.
func GetLogs() *Logs {
	l, _ := logsPool.Get().(*Logs)

	return l
}

// PutLogs resets l and returns it to the pool. Do not use l after this call. The byte payloads l
// references are not owned by the pool; the caller must ensure they are no longer aliased.
func PutLogs(l *Logs) {
	l.Reset()
	logsPool.Put(l)
}
