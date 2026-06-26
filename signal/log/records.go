package log

import (
	"sync"

	"github.com/oteldb/storage/signal"
)

// Identity is a log stream's identity: its [signal.Resource] and the [signal.Scope] that produced
// it. Unlike a metric series, a stream carries no data-point attributes — the per-record
// attributes vary within the stream and are stored as a column, not folded into identity. Two
// streams with equal resource+scope are the same stream (and the same [signal.SeriesID]).
type Identity struct {
	Resource signal.Resource
	Scope    signal.Scope
}

// ToSeries returns the stream's [signal.Series] — the value stored, indexed (resource attributes
// and scope name/version become queryable labels), and returned in fetch batches. Its data-point
// attributes are empty.
func (id Identity) ToSeries() signal.Series {
	return signal.Series{Resource: id.Resource, Scope: id.Scope}
}

// StreamID is the content-addressed id of the stream identity ([ToSeries] hashed).
func (id Identity) StreamID() signal.SeriesID { return id.ToSeries().Hash() }

// Batch is the projection of one log stream: its [signal.SeriesID], the identity needed to
// materialize a full [signal.Series] lazily (only for a stream the engine has not seen), and the
// stream's records (aliasing the source [Logs]). Emitting a whole stream at once lets the engine
// take its lock and register the stream once, then append every record under it.
//
// A Batch is reused across streams within one [Project] pass: its fields and the data they alias
// are valid only for the duration of the emit call. Do not retain it.
type Batch struct {
	StreamID signal.SeriesID
	base     Identity
	records  []Record // the stream's records (aliases the source Logs)
	buf      []byte   // reused stream-id hash-input scratch (persists via the pool)
}

var batchPool = sync.Pool{New: func() any { return &Batch{} }}

// Len returns the number of records in the batch.
func (b *Batch) Len() int { return len(b.records) }

// Resource is the batch's source resource (for tenant routing).
func (b *Batch) Resource() signal.Resource { return b.base.Resource }

// Scope is the batch's source scope (for tenant routing).
func (b *Batch) Scope() signal.Scope { return b.base.Scope }

// Series materializes the stream's full [signal.Series] (resource+scope). It is the lazy
// materializer the engine calls only when registering a newly-seen stream.
func (b *Batch) Series() signal.Series { return b.base.ToSeries() }

// At returns the i-th record. The returned value aliases the source batch (zero-copy).
func (b *Batch) At(i int) Record { return b.records[i] }

// Records returns the batch's records (aliasing the source). Read-only.
func (b *Batch) Records() []Record { return b.records }

// Project iterates a [Logs] batch and calls emit once per stream (each Resource+Scope group) with
// a [Batch] of that stream's records. It returns how many records were emitted. Every record in a
// [Logs] batch is well-formed by construction, so projection rejects nothing; out-of-order
// rejection is the engine's concern downstream.
//
// The stream id is computed without per-record work: it is the resource+scope identity hashed
// once per group into the batch's reused buffer, so a steady-state [Project] allocates nothing.
func Project(ld Logs, emit func(*Batch)) (accepted int) {
	b, _ := batchPool.Get().(*Batch)
	defer func() {
		b.base = Identity{}
		b.records = nil
		batchPool.Put(b) // keeps b.buf for the next pass
	}()

	for ri := range ld.Resources {
		rl := &ld.Resources[ri]
		b.base.Resource = rl.Resource

		for si := range rl.Scopes {
			sl := &rl.Scopes[si]
			if len(sl.Records) == 0 {
				continue
			}

			b.base.Scope = sl.Scope
			b.buf = b.base.ToSeries().AppendHashInput(b.buf[:0])
			b.StreamID = signal.HashBytes(b.buf)
			b.records = sl.Records
			emit(b)
			accepted += len(sl.Records)
		}
	}

	return accepted
}
