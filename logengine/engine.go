// Package logengine is the single-node logs storage engine: the logs analog of package engine. It
// ties the identity index (streams), immutable columnar parts, and the WAL into an ingest+query
// path, reusing the signal-neutral substrate (block, index, wal, backend) but with the log record
// shape — a stream of rich records rather than a series of float samples.
//
// It implements [fetch.Fetcher]: label matchers resolve streams over the postings index, and a
// fetched [fetch.Batch] carries the per-record columns (timestamps + severity/body/attrs/…). The
// columnar Conditions of the fetch contract are applied here in a later milestone; M8a serves the
// full column set filtered only by stream labels and time.
package logengine

import (
	"context"
	"sync"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/log"
	"github.com/oteldb/storage/wal"
)

// Config configures an [Engine].
type Config struct {
	// OOOWindow rejects records older than newest-OOOWindow (nanoseconds). 0 disables (logs are
	// commonly accepted out of order).
	OOOWindow int64
	// WAL, when non-nil, durably logs streams and records for crash recovery. nil is the
	// ephemeral in-memory engine.
	WAL *wal.SegmentWriter
	// Backend stores flushed parts. Required for [Engine.Flush]; nil is a head-only engine.
	Backend backend.Backend
	// Prefix is the backend key prefix under which this engine's parts are written
	// (typically "{tenant}/logs").
	Prefix string
}

// Engine is a single tenant's logs storage engine. Safe for concurrent use.
type Engine struct {
	cfg     Config
	mu      sync.RWMutex
	head    *head
	parts   []*part
	nextSeq int
}

var _ fetch.Fetcher = (*Engine)(nil)

// New returns a logs engine with an empty head.
func New(cfg Config) *Engine {
	return &Engine{cfg: cfg, head: newHead()}
}

// fromLogRecord converts a model record to the engine's internal rec, serializing attributes once
// (the reversible form decoded later for attribute conditions).
func fromLogRecord(r log.Record) rec {
	return rec{
		ts: r.Timestamp, observed: r.ObservedTimestamp, severity: int64(r.SeverityNumber),
		flags: int64(r.Flags), dropped: int64(r.Dropped),
		sevText: r.SeverityText, body: r.Body, traceID: r.TraceID, spanID: r.SpanID,
		attrs: r.Attributes.AppendHashInput(nil),
	}
}

// toWALRecord converts an internal rec to the WAL wire form.
func toWALRecord(r rec) wal.LogRecord {
	return wal.LogRecord{
		Timestamp: r.ts, ObservedTimestamp: r.observed, SeverityNumber: int32(r.severity),
		Flags: uint32(r.flags), Dropped: uint32(r.dropped),
		SeverityText: r.sevText, Body: r.body, TraceID: r.traceID, SpanID: r.spanID, Attrs: r.attrs,
	}
}

// fromWALRecord converts a WAL record to the engine's internal rec.
func fromWALRecord(w wal.LogRecord) rec {
	return rec{
		ts: w.Timestamp, observed: w.ObservedTimestamp, severity: int64(w.SeverityNumber),
		flags: int64(w.Flags), dropped: int64(w.Dropped),
		sevText: w.SeverityText, body: w.Body, traceID: w.TraceID, spanID: w.SpanID, attrs: w.Attrs,
	}
}

// AppendBatch ingests one stream's records (a [log.Batch] from [log.Project]): it registers the
// stream on first sight, appends each record through the OOO check, and logs accepted records to
// the WAL. It returns how many records were accepted (the rest were out-of-order beyond the
// window). The whole stream is appended under a single lock. Safe for concurrent use.
func (e *Engine) AppendBatch(b *log.Batch) (accepted int, err error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	id := b.StreamID
	isNew := e.head.ensureStream(id, b.Series)

	var walRecs []wal.LogRecord

	for i := range b.Records() {
		r := fromLogRecord(b.At(i))
		if !e.head.appendRecord(id, r, e.cfg.OOOWindow) {
			continue
		}

		accepted++

		if e.cfg.WAL != nil {
			walRecs = append(walRecs, toWALRecord(r))
		}
	}

	if e.cfg.WAL != nil && accepted > 0 {
		if isNew {
			if err := e.cfg.WAL.WriteSeries(id, b.Series()); err != nil {
				return accepted, err
			}
		}

		if err := e.cfg.WAL.WriteLogRecords(id, walRecs); err != nil {
			return accepted, err
		}
	}

	return accepted, nil
}

// Fetch implements [fetch.Fetcher] over head ∪ flushed parts: it resolves the request's matchers
// to streams, then returns one batch per stream with its records in the window (head ∪ every
// part), sorted by timestamp.
func (e *Engine) Fetch(ctx context.Context, r fetch.Request) (fetch.Iterator, error) {
	// The label index sorts lazily on first read after a write; do that one-time sort under the
	// exclusive lock so concurrent fetches never race on it (see engine.Engine.Fetch).
	e.mu.RLock()
	for !e.head.indexSorted() {
		e.mu.RUnlock()
		e.mu.Lock()
		e.head.ensureIndexSorted()
		e.mu.Unlock()
		e.mu.RLock()
	}

	defer e.mu.RUnlock()

	ids := e.head.resolve(r.Matchers)

	var batches []*fetch.Batch

	for _, id := range ids {
		acc := &recordCols{}

		// Parts oldest → newest, then the head, then sort the whole window by timestamp.
		for _, p := range e.parts {
			if p.maxTime < r.Start || p.minTime > r.End {
				continue // time-prune via the part's bounds
			}

			// Bloom prune: when conditions are applied (AllConditions), skip a part whose body or
			// attribute bloom proves a required full-text token or equality value absent (no
			// false negatives; surviving parts are still re-checked per row).
			if r.AllConditions && !p.mayContain(r.Conditions) {
				continue
			}

			if err := p.appendWindow(ctx, id, acc, r.Start, r.End); err != nil {
				return nil, err
			}
		}

		e.head.appendWindow(id, acc, r.Start, r.End)

		// Column conditions (AND) filter records within the stream. Per the contract, the engine
		// applies them conjunctively only when AllConditions is set; otherwise it returns the
		// window superset and the language layer filters.
		if r.AllConditions && len(r.Conditions) > 0 {
			acc = acc.filtered(r.Conditions)
		}

		if acc.len() == 0 {
			continue
		}

		acc.sortByTs()

		s, _ := e.head.series.Get(id)
		b := acc.toBatch(id, s, r.Projection)

		// SecondPass is an optional engine-side post-filter (e.g. a cross-column predicate); a
		// false verdict drops the whole stream batch.
		if r.SecondPass != nil && !r.SecondPass(b) {
			continue
		}

		batches = append(batches, b)
	}

	return fetch.NewSliceIterator(batches), nil
}

// Flush writes the head's buffered records to a new immutable part and clears the buffers (the
// stream index is retained). No-op if the head holds no records. Requires a [Config.Backend].
func (e *Engine) Flush(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.flushLocked(ctx)
}

// Reset discards all data (head + parts), returning the engine to its empty state without
// reallocating it, and deletes this engine's part objects from the backend. Meant for the
// ephemeral in-memory engine in tests/benchmarks. Safe for concurrent use.
func (e *Engine) Reset(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.head = newHead()
	e.parts = nil
	e.nextSeq = 0

	if e.cfg.Backend == nil {
		return nil
	}

	keys, err := e.cfg.Backend.List(ctx, e.cfg.Prefix+"/")
	if err != nil {
		return errors.Wrap(err, "list parts")
	}

	for _, k := range keys {
		if err := e.cfg.Backend.Delete(ctx, k); err != nil && !errors.Is(err, backend.ErrNotExist) {
			return errors.Wrapf(err, "delete %q", k)
		}
	}

	return nil
}

// Replay rebuilds the head from the WAL segments in dir (durable restart).
func (e *Engine) Replay(dir string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	return wal.ReplayDir(dir, wal.Handlers{
		OnSeries: func(_ signal.SeriesID, s signal.Series) error {
			e.head.registerStream(s)

			return nil
		},
		OnLogRecords: func(id signal.SeriesID, recs []wal.LogRecord) error {
			e.head.replayRecords(id, toRecs(recs))

			return nil
		},
	})
}

// toRecs converts WAL records to internal recs.
func toRecs(ws []wal.LogRecord) []rec {
	out := make([]rec, len(ws))
	for i := range ws {
		out[i] = fromWALRecord(ws[i])
	}

	return out
}

// PartCount returns the number of flushed parts (testing/introspection).
func (e *Engine) PartCount() int {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return len(e.parts)
}

// StreamCount returns the number of distinct streams in the head.
func (e *Engine) StreamCount() int {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return e.head.series.Len()
}

// HeadRecordCount returns the number of records currently buffered in the head (across all
// streams) — for introspection and tests (e.g. to observe replica head trimming).
func (e *Engine) HeadRecordCount() int {
	e.mu.RLock()
	defer e.mu.RUnlock()

	n := 0
	for _, buf := range e.head.records {
		n += buf.len()
	}

	return n
}

func (e *Engine) flushLocked(ctx context.Context) error {
	f := e.head.drainHead()
	if f == nil {
		return nil
	}

	prefix := e.partPrefix(e.nextSeq)
	if err := writePart(ctx, e.cfg.Backend, prefix, f); err != nil {
		return err
	}

	p, err := openPart(ctx, e.cfg.Backend, prefix)
	if err != nil {
		return err
	}

	p.minTime, p.maxTime = colsTimeRange(f)
	e.parts = append(e.parts, p)
	e.nextSeq++

	if err := e.updateIndexLocked(ctx); err != nil {
		return err
	}

	return e.writeStreamIndexLocked(ctx)
}
