package recordengine

import (
	"context"
	"sync"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"
)

// Config configures an [Engine].
type Config struct {
	// Schema is the per-record column set this engine stores (required; the signal supplies it).
	Schema *Schema
	// OOOWindow rejects records older than newest-OOOWindow (nanoseconds). 0 disables.
	OOOWindow int64
	// WAL, when non-nil, durably logs streams and records for crash recovery. nil ⇒ ephemeral.
	WAL *wal.SegmentWriter
	// Backend stores flushed parts. Required for [Engine.Flush]; nil ⇒ head-only.
	Backend backend.Backend
	// Prefix is the backend key prefix under which this engine's parts are written.
	Prefix string
	// SideStore, when non-nil, is a signal-supplied content-addressed auxiliary store (e.g. the
	// profiles symbol store) that the engine persists as part sidecars on flush and unions on merge.
	// nil ⇒ no side data (logs, traces).
	SideStore SideStore
}

// Engine is one tenant's record store for a signal. Safe for concurrent use.
type Engine struct {
	cfg     Config
	mu      sync.RWMutex
	head    *head
	parts   []*part
	nextSeq int
}

var _ fetch.Fetcher = (*Engine)(nil)

// New returns an engine with an empty head over cfg.Schema.
func New(cfg Config) *Engine {
	return &Engine{cfg: cfg, head: newHead(cfg.Schema)}
}

// Batch is one stream's projected records in the engine's column layout: the primary timestamps
// plus the int and byte column vectors in the schema's per-kind order. The signal package builds
// it; the engine treats the columns opaquely. Byte slices may alias the source batch (the head
// clones them on append).
type Batch struct {
	Stream   signal.SeriesID
	Identity func() signal.Series // materialized only when the stream is newly seen
	Ts       []int64
	Ints     [][]int64  // len == schema int count; Ints[k][row]
	Bytes    [][][]byte // len == schema byte count; Bytes[k][row]
	// Side is an optional encoded side-store delta (the content-addressed symbols this batch's
	// records reference) absorbed by [Config.SideStore]. nil when the engine has no side store.
	Side []byte
}

// Len returns the number of records in the batch.
func (b *Batch) Len() int { return len(b.Ts) }

// AppendBatch ingests one stream's records: it registers the stream on first sight, appends each
// record through the OOO check, and logs accepted records to the WAL. It returns how many records
// were accepted (the rest were out-of-order beyond the window). Safe for concurrent use.
func (e *Engine) AppendBatch(b *Batch) (accepted int, err error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	isNew := e.head.ensureStream(b.Stream, b.Identity)

	scratch := rec{ints: make([]int64, len(b.Ints)), bytes: make([][]byte, len(b.Bytes))}

	var walRecs []rec

	for i := range b.Ts {
		scratch.ts = b.Ts[i]
		for k := range b.Ints {
			scratch.ints[k] = b.Ints[k][i]
		}

		for k := range b.Bytes {
			scratch.bytes[k] = b.Bytes[k][i]
		}

		if !e.head.appendRecord(b.Stream, scratch, e.cfg.OOOWindow) {
			continue
		}

		accepted++

		if e.cfg.WAL != nil {
			walRecs = append(walRecs, cloneRec(scratch))
		}
	}

	if e.cfg.WAL != nil && accepted > 0 {
		if isNew {
			if err := e.cfg.WAL.WriteSeries(b.Stream, b.Identity()); err != nil {
				return accepted, err
			}
		}

		if err := e.cfg.WAL.WriteRecords(b.Stream, encodeRecs(walRecs)); err != nil {
			return accepted, err
		}
	}

	if e.cfg.SideStore != nil && accepted > 0 && len(b.Side) > 0 {
		if err := e.cfg.SideStore.Absorb(b.Side); err != nil {
			return accepted, errors.Wrap(err, "absorb side delta")
		}
	}

	return accepted, nil
}

// Fetch implements [fetch.Fetcher] over head ∪ flushed parts: it resolves matchers to streams,
// gathers each stream's in-window records (decoding only the referenced columns), applies the
// column conditions and projection, and returns one batch per stream sorted by timestamp.
func (e *Engine) Fetch(ctx context.Context, r fetch.Request) (fetch.Iterator, error) {
	// The label index sorts lazily on first read after a write; do that one-time sort under the
	// exclusive lock so concurrent fetches never race on it.
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
	if len(ids) == 0 {
		return fetch.NewSliceIterator(nil), nil
	}

	accs, err := e.accumulate(ctx, ids, r)
	if err != nil {
		return nil, err
	}

	var batches []*fetch.Batch

	for _, id := range ids {
		acc := accs[id]

		if r.AllConditions && len(r.Conditions) > 0 {
			acc.filterInPlace(r.Conditions)
		}

		if acc.len() == 0 {
			continue
		}

		acc.sortByTs()

		s, _ := e.head.series.Get(id)
		b := acc.toBatch(id, s, r.Projection)

		if r.SecondPass != nil && !r.SecondPass(b) {
			continue
		}

		batches = append(batches, b)
	}

	return fetch.NewSliceIterator(batches), nil
}

// appendWindowRows appends rows of cols in [rng.start, rng.end) whose timestamp is in [start, end]
// to acc, bulk-appending the whole range when it falls entirely in the window.
func appendWindowRows(acc, cols *recordCols, rng rowRange, start, end int64) {
	if rng.start >= rng.end {
		return
	}

	if cols.ts[rng.start] >= start && cols.ts[rng.end-1] <= end {
		acc.appendRange(cols, rng.start, rng.end)

		return
	}

	for i := rng.start; i < rng.end; i++ {
		if cols.ts[i] >= start && cols.ts[i] <= end {
			acc.appendRow(cols, i)
		}
	}
}

// Flush writes the head's buffered records to a new immutable part and clears the buffers. No-op if
// the head is empty. Requires a [Config.Backend].
func (e *Engine) Flush(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.flushLocked(ctx)
}

// Reset discards all data (head + parts) and deletes this engine's part objects, returning it to
// the empty state without reallocating. Safe for concurrent use.
func (e *Engine) Reset(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.head = newHead(e.cfg.Schema)
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
		OnRecords: func(id signal.SeriesID, blob []byte) error {
			recs, err := decodeRecs(blob, e.cfg.Schema.numInts(), e.cfg.Schema.numBytes())
			if err != nil {
				return err
			}

			e.head.replayRecords(id, recs)

			return nil
		},
	})
}

// PartCount returns the number of flushed parts (introspection).
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

// HeadRecordCount returns the number of records buffered in the head across all streams.
func (e *Engine) HeadRecordCount() int {
	e.mu.RLock()
	defer e.mu.RUnlock()

	n := 0
	for _, buf := range e.head.records {
		n += buf.len()
	}

	return n
}

// accumulate gathers each requested stream's in-window records (head ∪ live parts) into a
// pre-sized accumulator, decoding only the columns the request references (lazy decode) and each
// live part exactly once.
func (e *Engine) accumulate(ctx context.Context, ids []signal.SeriesID, r fetch.Request) (map[signal.SeriesID]*recordCols, error) {
	sel := selectColumns(e.cfg.Schema, r)

	live := make([]*part, 0, len(e.parts))
	for _, p := range e.parts {
		switch {
		case p.maxTime < r.Start || p.minTime > r.End: // time-prune
		case r.AllConditions && !p.mayContain(r.Conditions): // bloom-prune
		case p.holdsAny(ids):
			live = append(live, p)
		}
	}

	accs := make(map[signal.SeriesID]*recordCols, len(ids))
	for _, id := range ids {
		n := e.head.recordCount(id)
		for _, p := range live {
			if rng, ok := p.ranges[id]; ok {
				n += rng.end - rng.start
			}
		}

		accs[id] = newRecordCols(e.cfg.Schema, n, sel)
	}

	for _, p := range live {
		cols, err := p.readCols(ctx, sel)
		if err != nil {
			return nil, err
		}

		for _, id := range ids {
			if rng, ok := p.ranges[id]; ok {
				appendWindowRows(accs[id], cols, rng, r.Start, r.End)
			}
		}
	}

	for _, id := range ids {
		e.head.appendWindow(id, accs[id], r.Start, r.End)
	}

	return accs, nil
}

func (e *Engine) flushLocked(ctx context.Context) error {
	f := e.head.drainHead()
	if f == nil {
		return nil
	}

	prefix := e.partPrefix(e.nextSeq)
	if err := writePart(ctx, e.cfg.Backend, e.cfg.Schema, prefix, f); err != nil {
		return err
	}

	p, err := openPart(ctx, e.cfg.Backend, e.cfg.Schema, prefix)
	if err != nil {
		return err
	}

	p.minTime, p.maxTime = colsTimeRange(f)
	e.parts = append(e.parts, p)
	e.nextSeq++

	if e.cfg.SideStore != nil {
		if err := writeSidecars(ctx, e.cfg.Backend, prefix, e.cfg.SideStore.Encode()); err != nil {
			return err
		}

		e.cfg.SideStore.Reset()
	}

	if err := e.updateIndexLocked(ctx); err != nil {
		return err
	}

	return e.writeStreamIndexLocked(ctx)
}
