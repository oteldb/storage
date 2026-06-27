package recordengine

import (
	"context"
	"sync"
	"time"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/internal/obs"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/query/profile"
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
	// Obs is the observability handle (spans + metrics). nil ⇒ a no-op handle.
	Obs *obs.Obs
	// Signal is the signal label for this engine's metrics ("log"/"trace"/"profile"); the facade
	// sets it per signal. Empty ⇒ "record".
	Signal string
}

// Engine is one tenant's record store for a signal. Safe for concurrent use.
type Engine struct {
	cfg     Config
	mu      sync.RWMutex
	head    *head
	parts   []*part
	nextSeq int
	// retiring holds parts removed from the live set by flush/merge, pending backend deletion once
	// their in-flight fetch readers drain (deferred reclamation; see reclaim.go).
	retiring []*part
	// flushing holds the record buffers detached from the head by an in-progress flush, kept readable
	// by fetch until the flushed part is published (then cleared, atomically with adding the part). It
	// closes the visibility gap a flush would otherwise open between draining the head and the part
	// becoming live. nil when no flush is in flight.
	flushing map[signal.SeriesID]*recordCols
	// flushedEpoch is the WAL flush watermark: the generation of the most recently flushed head
	// (persisted in the bucket index). Current head records are written to the WAL at flushedEpoch+1,
	// so on recovery the engine replays only WAL segments past flushedEpoch — exactly-once.
	flushedEpoch uint64
}

var _ fetch.Fetcher = (*Engine)(nil)

// New returns an engine with an empty head over cfg.Schema.
func New(cfg Config) *Engine {
	if cfg.Obs == nil {
		cfg.Obs = obs.NewNop()
	}

	if cfg.Signal == "" {
		cfg.Signal = "record"
	}

	e := &Engine{cfg: cfg, head: newHead(cfg.Schema)}
	if cfg.WAL != nil {
		cfg.WAL.SetEpoch(e.flushedEpoch + 1) // first head generation
	}

	return e
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

// ByteSize returns the in-flight memory the batch's records occupy (timestamps, int columns, and
// the lengths of the byte columns) — the size a caller charges against an ingest-rate budget.
func (b *Batch) ByteSize() int64 {
	n := int64(8 * len(b.Ts))
	for k := range b.Ints {
		n += int64(8 * len(b.Ints[k]))
	}

	for k := range b.Bytes {
		for _, v := range b.Bytes[k] {
			n += int64(len(v))
		}
	}

	return n
}

// AppendBatch ingests one stream's records: it registers the stream on first sight, appends each
// record through the admission limits (OOO window, cardinality, in-flight bytes), and logs accepted
// records to the WAL. It returns an [AppendResult] breaking accepted/rejected down by reason, so the
// caller can report an exact OTLP partial-success. Safe for concurrent use.
func (e *Engine) AppendBatch(b *Batch, limits AppendLimits) (AppendResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Cardinality is a per-batch decision (a batch is one stream): if this stream is new and the
	// head is at the cap, the whole batch is rejected and the stream is not registered.
	isNew, ok := e.head.ensureStream(b.Stream, b.Identity, limits.MaxSeries)
	if !ok {
		return AppendResult{RejectedCardinality: len(b.Ts)}, nil
	}

	scratch := rec{ints: make([]int64, len(b.Ints)), bytes: make([][]byte, len(b.Bytes))}

	var (
		res     AppendResult
		walRecs []rec
	)

	for i := range b.Ts {
		scratch.ts = b.Ts[i]
		for k := range b.Ints {
			scratch.ints[k] = b.Ints[k][i]
		}

		for k := range b.Bytes {
			scratch.bytes[k] = b.Bytes[k][i]
		}

		switch e.head.appendRecord(b.Stream, scratch, e.cfg.OOOWindow, limits.MaxInFlightBytes) {
		case admitted:
			res.Accepted++
		case rejectOOO:
			res.RejectedOOO++

			continue
		case rejectBytes:
			res.RejectedBytes++

			continue
		}

		if e.cfg.WAL != nil {
			walRecs = append(walRecs, cloneRec(scratch))
		}
	}

	if e.cfg.WAL != nil && res.Accepted > 0 {
		if err := e.logWAL(b, walRecs, isNew); err != nil {
			return res, err
		}
	}

	if e.cfg.SideStore != nil && res.Accepted > 0 && len(b.Side) > 0 {
		if err := e.cfg.SideStore.Absorb(b.Side); err != nil {
			return res, errors.Wrap(err, "absorb side delta")
		}
	}

	return res, nil
}

// HeadBytes returns the head's current buffered record bytes — the in-flight memory measure for
// [AppendLimits.MaxInFlightBytes].
func (e *Engine) HeadBytes() int64 {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return e.head.bytes
}

// Stats is an in-memory snapshot of a record engine's state for introspection (no backend I/O).
type Stats struct {
	Streams     int64 // distinct streams ever seen (index span: head ∪ flushed)
	HeadRecords int64 // records currently buffered in the head (unflushed)
	HeadBytes   int64 // head's buffered record bytes (the in-flight memory measure)
	Parts       int   // flushed immutable parts
	MinTime     int64 // oldest flushed record time (unix ns); 0 when no parts
	MaxTime     int64 // newest record time across parts and the head (unix ns); 0 when empty
}

// Stats returns an in-memory snapshot of the engine's state under a single read lock (no backend
// I/O, no decode), safe to poll at dashboard cadence. Part byte sizes are not included.
func (e *Engine) Stats() Stats {
	e.mu.RLock()
	defer e.mu.RUnlock()

	s := Stats{
		Streams:   int64(e.head.series.Len()),
		HeadBytes: e.head.bytes,
		Parts:     len(e.parts),
		MaxTime:   e.head.newest,
	}

	for _, buf := range e.head.records {
		s.HeadRecords += int64(buf.len())
	}

	for i, p := range e.parts {
		if i == 0 || p.minTime < s.MinTime {
			s.MinTime = p.minTime
		}

		if p.maxTime > s.MaxTime {
			s.MaxTime = p.maxTime
		}
	}

	return s
}

// Fetch implements [fetch.Fetcher] over head ∪ flushed parts: it resolves matchers to streams,
// gathers each stream's in-window records (decoding only the referenced columns), applies the
// column conditions and projection, and returns one batch per stream sorted by timestamp.
func (e *Engine) Fetch(ctx context.Context, r fetch.Request) (fetch.Iterator, error) {
	ctx = e.cfg.Obs.Base(ctx)
	ctx, span := e.cfg.Obs.Tracer.Start(ctx, "recordengine.fetch",
		trace.WithAttributes(attribute.String("storage.prefix", e.cfg.Prefix)))
	defer span.End()

	ctx, pf := profile.Begin(ctx, "recordengine.fetch")
	defer pf.End()

	startNs := time.Now()
	log := zctx.From(ctx)
	log.Debug("fetch start",
		zap.String("signal", e.cfg.Signal), zap.String("prefix", e.cfg.Prefix),
		zap.Int("matchers", len(r.Matchers)), zap.Int("conditions", len(r.Conditions)))

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

	_, rpf := profile.Begin(ctx, "resolve-matchers")
	ids := e.head.resolve(r.Matchers)
	rpf.Add("series_matched", int64(len(ids)))
	rpf.End()

	partsScanned := len(e.parts)

	record := func(rows int) {
		pf.Add("series_matched", int64(len(ids)))
		pf.Add("parts_scanned", int64(partsScanned))
		pf.Add("rows", int64(rows))
		span.SetAttributes(
			attribute.Int("storage.series_matched", len(ids)),
			attribute.Int("storage.parts_scanned", partsScanned),
			attribute.Int("storage.rows", rows),
		)
		e.cfg.Obs.Fetch.Record(ctx, e.cfg.Signal, time.Since(startNs), int64(len(ids)), int64(partsScanned), int64(rows))
		log.Debug("fetch done",
			zap.String("signal", e.cfg.Signal), zap.String("prefix", e.cfg.Prefix),
			zap.Int("series_matched", len(ids)), zap.Int("parts_scanned", partsScanned),
			zap.Int("rows", rows), zap.Duration("took", time.Since(startNs)))
	}

	if len(ids) == 0 {
		e.mu.RUnlock()
		record(0)

		return fetch.NewSliceIterator(nil), nil
	}

	// Plan under the read lock: seed each stream's accumulator from the head, select and acquire the
	// live parts to read, and capture stream identities. Releasing the lock before the backend reads
	// lets appends and flush/merge proceed concurrently — the acquired parts can't be reclaimed until
	// we release them, so the lock-free reads never race a delete.
	plan := e.planFetch(ids, r)
	e.mu.RUnlock()

	defer plan.releaseParts()

	if err := plan.readParts(ctx); err != nil {
		span.RecordError(err)

		return nil, err
	}

	var (
		batches []*fetch.Batch
		rows    int
	)

	for _, id := range ids {
		acc := plan.accs[id]

		if r.AllConditions && len(r.Conditions) > 0 {
			acc.filterInPlace(r.Conditions)
		}

		if acc.len() == 0 {
			continue
		}

		acc.sortByTs()

		b := acc.toBatch(id, plan.series[id], r.Projection)

		if r.SecondPass != nil && !r.SecondPass(b) {
			continue
		}

		batches = append(batches, b)
		rows += len(b.Timestamps)
	}

	record(rows)

	return fetch.NewSliceIterator(batches), nil
}

// Series returns the identities of the streams matching matchers that hold at least one record in
// [start, end] — the enumeration primitive behind profile-type / label listing. A zero start AND
// end disables the time filter (return every matching stream). The time filter is part-overlap
// granular (a returned stream is guaranteed to match the matchers; its in-window records are a
// superset check). Safe for concurrent use.
func (e *Engine) Series(matchers []fetch.Matcher, start, end int64) []signal.Series {
	e.mu.RLock()
	for !e.head.indexSorted() {
		e.mu.RUnlock()
		e.mu.Lock()
		e.head.ensureIndexSorted()
		e.mu.Unlock()
		e.mu.RLock()
	}

	defer e.mu.RUnlock()

	ids := e.head.resolve(matchers)

	out := make([]signal.Series, 0, len(ids))
	for _, id := range ids {
		if !e.streamInRangeLocked(id, start, end) {
			continue
		}

		if s, ok := e.head.series.Get(id); ok {
			out = append(out, s)
		}
	}

	return out
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
	ctx = e.cfg.Obs.Base(ctx)
	ctx, span := e.cfg.Obs.Tracer.Start(ctx, "recordengine.flush",
		trace.WithAttributes(attribute.String("storage.prefix", e.cfg.Prefix)))
	defer span.End()

	startNs := time.Now()
	log := zctx.From(ctx)
	log.Debug("flush requested", zap.String("signal", e.cfg.Signal), zap.String("prefix", e.cfg.Prefix))

	rows, err := e.flush(ctx)
	if err != nil {
		span.RecordError(err)
		log.Error("flush failed",
			zap.String("signal", e.cfg.Signal), zap.String("prefix", e.cfg.Prefix), zap.Error(err))

		return err
	}

	if rows > 0 {
		span.SetAttributes(attribute.Int("storage.rows", rows))
		e.cfg.Obs.Flush.Record(ctx, e.cfg.Signal, time.Since(startNs), int64(rows))
		log.Debug("flushed head to part",
			zap.String("signal", e.cfg.Signal), zap.String("prefix", e.cfg.Prefix),
			zap.Int("rows", rows), zap.Duration("took", time.Since(startNs)))
	} else {
		log.Debug("flush no-op (empty head)",
			zap.String("signal", e.cfg.Signal), zap.String("prefix", e.cfg.Prefix))
	}

	return nil
}

// Reset discards all data (head + parts) and deletes this engine's part objects, returning it to
// the empty state without reallocating. Safe for concurrent use.
func (e *Engine) Reset(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.head = newHead(e.cfg.Schema)
	e.parts = nil
	e.retiring = nil // the List+Delete below removes their objects too
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

// Replay rebuilds the head (and side store) from the WAL segments in dir (durable restart). It skips
// segments at or below the flush watermark recovered by [Engine.LoadParts] (call LoadParts first), so
// records already in a flushed part are not re-applied — exactly-once recovery.
func (e *Engine) Replay(dir string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	return wal.ReplayDirFrom(dir, e.flushedEpoch, e.replayHandlers())
}

// SideSnapshot returns the engine's full side-store tables — the live head accumulator unioned with
// every flushed part's sidecars — as named payloads, for a signal to build a resolver over (e.g. the
// profiles symbol store). nil when the engine has no side store. Safe for concurrent use.
func (e *Engine) SideSnapshot(ctx context.Context) (map[string][]byte, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.cfg.SideStore == nil {
		return map[string][]byte{}, nil
	}

	parts := make([]map[string][]byte, 0, len(e.parts)+1)
	parts = append(parts, e.cfg.SideStore.Encode()) // unflushed head symbols

	for _, p := range e.parts {
		m, err := loadSidecars(ctx, e.cfg.Backend, p.prefix, e.cfg.SideStore.Names())
		if err != nil {
			return nil, err
		}

		parts = append(parts, m)
	}

	return e.cfg.SideStore.Union(parts)
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

// logWAL durably logs a batch's series (on first sight), its accepted records, and — when present —
// its side-store delta, so a WAL replay reconstructs the head and the symbols those records reference.
func (e *Engine) logWAL(b *Batch, walRecs []rec, isNew bool) error {
	if isNew {
		if err := e.cfg.WAL.WriteSeries(b.Stream, b.Identity()); err != nil {
			return err
		}
	}

	if err := e.cfg.WAL.WriteRecords(b.Stream, encodeRecs(walRecs)); err != nil {
		return err
	}

	if len(b.Side) > 0 {
		return e.cfg.WAL.WriteSide(b.Side)
	}

	return nil
}

// replayHandlers builds the WAL handlers that rebuild the head and side store from a record log
// verbatim (no OOO re-check). Shared by [Engine.Replay] (durable restart) and
// [Engine.ApplyReplicated] (cluster secondary). The caller holds the lock.
func (e *Engine) replayHandlers() wal.Handlers {
	return wal.Handlers{
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
		OnSide: func(payload []byte) error {
			if e.cfg.SideStore == nil {
				return nil
			}

			return e.cfg.SideStore.Absorb(payload)
		},
	}
}

// streamInRangeLocked reports whether stream id has any record in [start, end] (head ∪ in-flight
// flush buffers ∪ parts). A zero start AND end disables the filter. Caller holds the lock.
func (e *Engine) streamInRangeLocked(id signal.SeriesID, start, end int64) bool {
	if start == 0 && end == 0 {
		return true
	}

	if bufInRange(e.head.records[id], start, end) || bufInRange(e.flushing[id], start, end) {
		return true
	}

	for _, p := range e.parts {
		if rng, ok := p.ranges[id]; ok && rng.start < rng.end && p.maxTime >= start && p.minTime <= end {
			return true
		}
	}

	return false
}

// fetchPlan is the lock-free-readable plan a fetch builds under the engine read lock: per-stream
// accumulators already seeded from the head, the acquired (ref-held) live parts still to read, and the
// captured stream identities. Its [fetchPlan.readParts] does the backend I/O off the lock.
type fetchPlan struct {
	sel        colSel
	ids        []signal.SeriesID
	accs       map[signal.SeriesID]*recordCols
	series     map[signal.SeriesID]signal.Series
	liveParts  []*part
	start, end int64
}

// planFetch builds the fetch plan: it selects and acquires the live parts that may hold a requested
// stream in-window (time + bloom + stream prune), seeds each stream's accumulator from the head
// buffer, and captures stream identities — all under the lock, so the subsequent part reads run
// lock-free. Caller holds e.mu (read lock). The acquired parts must be released with releaseParts.
func (e *Engine) planFetch(ids []signal.SeriesID, r fetch.Request) *fetchPlan {
	p := &fetchPlan{
		sel:    selectColumns(e.cfg.Schema, r),
		ids:    ids,
		accs:   make(map[signal.SeriesID]*recordCols, len(ids)),
		series: make(map[signal.SeriesID]signal.Series, len(ids)),
		start:  r.Start,
		end:    r.End,
	}

	for _, part := range e.parts {
		switch {
		case part.maxTime < r.Start || part.minTime > r.End: // time-prune
		case r.AllConditions && !part.mayContain(r.Conditions): // bloom-prune
		case part.holdsAny(ids):
			part.acquire()
			p.liveParts = append(p.liveParts, part)
		}
	}

	for _, id := range ids {
		n := e.head.recordCount(id)
		if buf := e.flushing[id]; buf != nil {
			n += buf.len()
		}

		for _, part := range p.liveParts {
			if rng, ok := part.ranges[id]; ok {
				n += rng.end - rng.start
			}
		}

		acc := newRecordCols(e.cfg.Schema, n, p.sel)
		e.head.appendWindow(id, acc, r.Start, r.End) // seed from the live head under the lock
		if buf := e.flushing[id]; buf != nil {
			appendColsWindow(buf, acc, r.Start, r.End) // …and from records mid-flush (not yet a part)
		}

		p.accs[id] = acc

		if s, ok := e.head.series.Get(id); ok {
			p.series[id] = s
		}
	}

	return p
}

// readParts decodes each acquired part (only the referenced columns — lazy decode) and appends its
// in-window rows to the per-stream accumulators. Runs lock-free: the parts are immutable and ref-held.
func (p *fetchPlan) readParts(ctx context.Context) error {
	for _, part := range p.liveParts {
		cols, err := part.readCols(ctx, p.sel)
		if err != nil {
			return err
		}

		for _, id := range p.ids {
			if rng, ok := part.ranges[id]; ok {
				appendWindowRows(p.accs[id], cols, rng, p.start, p.end)
			}
		}
	}

	return nil
}

// releaseParts releases the fetch's hold on its acquired parts, letting a retired part be reclaimed.
func (p *fetchPlan) releaseParts() {
	for _, part := range p.liveParts {
		part.release()
	}
}

// flush drains the head to a new immutable part, returning the number of records flushed (0 ⇒ empty
// head). It is phased so the part's column/bloom/footer write and read-back happen off the engine lock
// — appends and fetches proceed concurrently — while the head drain, the side-store snapshot, and the
// metadata publish run under it. Only the background maintenance task (or Close) calls flush, so the
// parts mutation has a single writer.
func (e *Engine) flush(ctx context.Context) (int, error) {
	// Plan (under lock): detach the head's record buffers (keeping them readable via e.flushing so a
	// concurrent fetch never loses them), snapshot the side-store delta atomically with the detach (so
	// a concurrent append's symbols aren't lost by the Reset), and reserve the part sequence.
	e.mu.Lock()
	detached := e.head.detach()
	if detached == nil {
		e.mu.Unlock()
		e.reclaimRetired(ctx) // nothing to flush, but still sweep pending deletions

		return 0, nil
	}

	e.flushing = detached
	seq := e.nextSeq

	var side map[string][]byte
	if e.cfg.SideStore != nil {
		side = e.cfg.SideStore.Encode()
		e.cfg.SideStore.Reset()
	}

	e.mu.Unlock()

	// Build (lock-free): lay out the detached buffers as part columns, write the part (columns +
	// blooms + record-key footer), read it back, and write the side-store sidecars. These are the
	// large I/Os that previously blocked the whole engine. The detached buffers are immutable here.
	f := buildFlushColumns(e.cfg.Schema, detached)
	rows := len(f.stream)
	prefix := e.partPrefix(seq)

	if err := writePart(ctx, e.cfg.Backend, e.cfg.Schema, prefix, f); err != nil {
		return 0, err
	}

	p, err := openPart(ctx, e.cfg.Backend, e.cfg.Schema, prefix)
	if err != nil {
		return 0, err
	}

	p.minTime, p.maxTime = colsTimeRange(f)

	if side != nil {
		if err := writeSidecars(ctx, e.cfg.Backend, prefix, side); err != nil {
			return rows, err
		}
	}

	// Publish (under lock): add the part copy-on-write and clear e.flushing in the same critical
	// section — so a fetch sees the records either in e.flushing or in the part, never neither (no gap)
	// and never both (no double count). Bump the sequence and the flush watermark, then persist the
	// index/stream-index and checkpoint the WAL — small metadata writes kept under the lock so the
	// parts swap and the durable watermark commit stay atomic, preserving the exactly-once
	// crash-consistency ordering (index commits the watermark, then the WAL checkpoint discards the
	// now-obsolete segments).
	e.mu.Lock()
	e.parts = appendPart(e.parts, p)
	e.flushing = nil
	e.nextSeq = seq + 1
	e.flushedEpoch++
	err = e.publishLocked(ctx)
	e.mu.Unlock()

	if err != nil {
		return rows, err
	}

	e.reclaimRetired(ctx)

	return rows, nil
}

// publishLocked persists the engine's part set (bucket index + stream identity index) and, for a
// flush, checkpoints the WAL to the advanced watermark. Caller holds e.mu.
func (e *Engine) publishLocked(ctx context.Context) error {
	if err := e.updateIndexLocked(ctx); err != nil {
		return err
	}

	if err := e.writeStreamIndexLocked(ctx); err != nil {
		return err
	}

	if e.cfg.WAL != nil {
		e.cfg.WAL.SetEpoch(e.flushedEpoch + 1)

		return e.cfg.WAL.Checkpoint()
	}

	return nil
}
