package recordengine

import (
	"math"
	"time"

	"github.com/oteldb/storage/index/postings"
	"github.com/oteldb/storage/index/series"
	"github.com/oteldb/storage/index/symbols"
	"github.com/oteldb/storage/internal/memsize"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// Reserved label keys: a stream's scope identity is indexed as labels so a query can match it.
var (
	labelScopeName    = []byte(signal.LabelScopeName)
	labelScopeVersion = []byte(signal.LabelScopeVersion)
)

// head is the engine's in-memory write buffer: the identity index (symbols + series + postings
// over stream labels) plus per-stream full-column record buffers in arrival order (sorted on
// read). Not safe for concurrent use; the [Engine] holds the lock.
type head struct {
	schema  *Schema
	sym     *symbols.Table
	series  *series.Index
	post    *postings.MemPostings
	records map[signal.SeriesID]*recordCols
	newest  int64 // newest record timestamp across every stream (reported as Inspect.MaxTime)

	// streamNewest is each stream's own newest admitted timestamp: the OOO window is a per-stream
	// lateness bound, not a cross-stream one, so a high-rate (or clock-skewed-ahead) stream cannot
	// drag the watermark forward for the low-rate streams sharing the head. It outlives a flush —
	// [head.detach] replaces the record buffers but a stream's lateness bound is not reset by its
	// records becoming durable — and is cleared only with the head itself.
	streamNewest map[signal.SeriesID]int64

	bytes int64 // buffered record bytes
	// detachedBytes is the byte count [head.detach] moved aside for an in-flight flush. Those buffers
	// stay fully resident (and the flush builds a second copy from them) until the part is published,
	// so they belong to the in-flight measure — see [head.inFlightBytes].
	detachedBytes int64
	// since is when the head took its first bytes after the last flush (zero while empty). It is the
	// only wall-clock the head keeps: record timestamps are data time, which backfill makes unusable
	// as a measure of how long data has been sitting unflushed.
	since time.Time
}

// grow accounts n newly buffered bytes, stamping the head's start when it takes its first bytes
// since the last flush.
func (h *head) grow(n int64) {
	if h.bytes == 0 {
		h.since = time.Now()
	}

	h.bytes += n
}

// age is how long the head has been accumulating since its last flush. 0 is reserved for an empty
// head, so a live one floors at a tick: a coarse clock (Windows moves in ~15ms steps) otherwise
// reports a head that has just taken its first bytes as having none.
func (h *head) age() time.Duration {
	if h.bytes == 0 || h.since.IsZero() {
		return 0
	}

	return max(time.Since(h.since), time.Nanosecond)
}

func newHead(schema *Schema) *head {
	return &head{
		schema:       schema,
		sym:          symbols.New(),
		series:       series.New(),
		post:         postings.NewMemPostings(),
		records:      make(map[signal.SeriesID]*recordCols),
		streamNewest: make(map[signal.SeriesID]int64),
	}
}

// admitStream reports whether the head has yet to see the stream (isNew) and whether registering it
// is allowed: a new stream is refused when minting it would exceed the cardinality cap. Existing
// streams are never blocked, so a query keeps returning what is already admitted. It does not
// mutate, so a caller can make the stream's identity durable before [head.ensureStream] commits it.
func (h *head) admitStream(id signal.SeriesID, maxSeries int64) (isNew, ok bool) {
	if h.series.Has(id) {
		return false, true
	}

	return true, maxSeries <= 0 || int64(h.series.Len()) < maxSeries
}

// rebuildIdentity returns fresh symbol, series and postings structures holding only the identities
// of snap that are in live — the identity prune (see prune.go). It is a rebuild rather than a
// deletion because symbol ids are dense and referenced by the postings lists, so removing one
// symbol would renumber every id above it.
//
// It reads nothing but its arguments, so it runs **off the engine lock**: snap is the series index's
// append-only entry log, which registration only extends. The caller installs the result and
// replays whatever was registered meanwhile ([head.swapIdentity]).
func rebuildIdentity(schema *Schema, snap []series.Entry, live map[signal.SeriesID]struct{}) *head {
	out := &head{schema: schema, sym: symbols.New(), series: series.New(), post: postings.NewMemPostings()}

	for i := range snap {
		if _, ok := live[snap[i].ID]; !ok {
			continue
		}

		out.register(snap[i].ID, snap[i].Series)
	}

	// Sort now, while the index is still private: a reader would otherwise trigger the lazy in-place
	// sort under the engine's read lock.
	out.post.EnsureSorted()

	return out
}

// swapIdentity installs the rebuilt identity structures, and is where the prune becomes atomic
// against ingest. Two sets of identities must survive a rebuild that decided without them:
//
//   - tail — registered after the snapshot was taken, so they are past its end in the same
//     append-only log and are live by construction.
//   - revived — snapshot entries the prune found dead that hold buffered records again. A stream
//     whose identity the *old* index still had is not re-registered when a record arrives, so it
//     leaves no tail entry; dropping it would strand records the next flush would then write into a
//     part with no identity naming them.
//
// Every other dead stream loses its out-of-order watermark here: it means nothing without an
// identity, while a live stream must keep its own or a late record slips back in. The record
// buffers and the byte measures are untouched — they describe live data. Caller holds the engine lock.
func (h *head) swapIdentity(rebuilt *head, snap, tail []series.Entry, dead []int32) {
	for i := range tail {
		rebuilt.register(tail[i].ID, tail[i].Series)
	}

	for _, pos := range dead {
		e := snap[pos]
		if _, revived := h.records[e.ID]; revived {
			rebuilt.register(e.ID, e.Series)

			continue
		}

		delete(h.streamNewest, e.ID)
	}

	h.sym, h.series, h.post = rebuilt.sym, rebuilt.series, rebuilt.post
}

// register records and indexes an identity known not to be present.
func (h *head) register(id signal.SeriesID, s signal.Series) {
	h.series.Add(s)
	h.indexLabels(id, s)
}

// needsStreamRecord reports whether the WAL must be told about this stream again. A flush
// checkpoints the log — every segment written before it is discarded — and detaches every record
// buffer, so a stream record logged when the identity was first seen is gone after the next flush
// while the identity itself survives in the resident index. Logging records again for a stream that
// is starting a fresh buffer keeps the log self-contained, which matters because identity is scoped
// to the parts: one whose parts retention has dropped exists nowhere durable but the log.
//
// A buffer is created once per stream per flush window, so this costs one stream record per
// actively-appending stream per flush, and nothing on the repeat-record path.
func (h *head) needsStreamRecord(id signal.SeriesID) bool { return h.records[id] == nil }

// ensureStream registers and indexes the stream on first sight and makes sure its (full-column)
// record buffer exists. materialize is called only when the stream identity is newly seen. It
// returns whether the stream is admitted, i.e. [head.admitStream]'s cardinality verdict.
func (h *head) ensureStream(id signal.SeriesID, materialize func() signal.Series, maxSeries int64) bool {
	isNew, ok := h.admitStream(id, maxSeries)
	if !ok {
		return false
	}

	if isNew {
		s := materialize()
		h.series.Add(s)
		h.indexLabels(id, s)
	}

	if h.records[id] == nil {
		h.records[id] = newRecordCols(h.schema, 0, fullSel(h.schema))
	}

	return true
}

// headByteCap is the hard ceiling on the head's record bytes, live plus detached, enforced regardless
// of [AppendLimits.MaxInFlightBytes] (which may be unset). A flush concatenates every stream's cells
// into one blob per byte column, indexed by the int32 offsets of [byteCol] — so a column blob cannot
// exceed 2 GiB, and the byte count (which counts every column's bytes) bounds any single one of them.
// Overflowing would be silent corruption: negative offsets written into a part. Reaching it takes a
// raised or disabled FlushThresholdBytes; past it records are rejected as [rejectBytes], the same
// memory-backpressure reason the configurable cap uses.
const headByteCap = math.MaxInt32

// appendRecord appends r to stream id's buffer (already ensured, or created on demand for the
// replica apply path), rejecting it as out-of-order when older than oooWindow behind *that stream's*
// newest admitted record (oooWindow > 0). A stream's first record is therefore never out of order,
// however far behind its neighbors it is. It returns whether the record was accepted.
func (h *head) appendRecord(id signal.SeriesID, r rec, oooWindow, maxBytes int64) admitOutcome {
	streamNewest, seen := h.streamNewest[id]
	if seen && oooWindow > 0 && r.ts < streamNewest-oooWindow {
		return rejectOOO
	}

	// headByteCap is a format bound on the *next* part's column blobs, and a failed flush folds the
	// detached buffers back into the live ones ([head.reattach]) — so the bound covers both sides of
	// the in-flight measure, not just the live half, which would let a reattach restore ~2× the cap.
	// MaxInFlightBytes is memory backpressure, and covers everything resident for its own reason.
	if h.inFlightBytes() >= headByteCap || (maxBytes > 0 && h.inFlightBytes() >= maxBytes) {
		return rejectBytes
	}

	buf := h.records[id]
	if buf == nil {
		buf = newRecordCols(h.schema, 0, fullSel(h.schema))
		h.records[id] = buf
	}

	buf.appendClone(r)
	h.grow(recByteSize(r))

	if !seen || r.ts > streamNewest {
		h.streamNewest[id] = r.ts
	}

	if r.ts > h.newest {
		h.newest = r.ts
	}

	return admitted
}

// registerStream records and indexes a stream identity without records (WAL replay / load).
func (h *head) registerStream(s signal.Series) {
	id := s.Hash()
	if !h.series.Has(id) {
		h.series.Add(s)
		h.indexLabels(id, s)
	}
}

// replayRecords appends records to an already-registered stream verbatim (WAL replay / replica
// apply; no OOO rejection — logged/replicated records are authoritative).
func (h *head) replayRecords(id signal.SeriesID, recs []rec) {
	if !h.series.Has(id) {
		return // stream record missing; ignore (defensive)
	}

	for i := range recs {
		// Replay/replica records are authoritative: no admission limits. [headByteCap] still applies —
		// it is a format bound, not a policy, and past it the head cannot be flushed at all.
		h.appendRecord(id, recs[i], 0, 0)
	}
}

// indexLabels interns and registers every queryable label of the stream — resource and scope
// attributes plus the scope name/version — into the postings index under id.
func (h *head) indexLabels(id signal.SeriesID, s signal.Series) {
	// Register the series in the all-set so it is resolvable even when it carries no labels at all
	// (e.g. a log stream whose resource and scope are empty); otherwise resolve(nil) would skip it.
	h.post.AddSeries(id)

	for i := range s.Resource.Attributes {
		h.addLabel(id, s.Resource.Attributes[i].Key, s.Resource.Attributes[i].Value)
	}

	for i := range s.Scope.Attributes {
		h.addLabel(id, s.Scope.Attributes[i].Key, s.Scope.Attributes[i].Value)
	}

	if len(s.Scope.Name) > 0 {
		h.addLabel(id, labelScopeName, signal.StringValue(s.Scope.Name))
	}

	if len(s.Scope.Version) > 0 {
		h.addLabel(id, labelScopeVersion, signal.StringValue(s.Scope.Version))
	}
}

func (h *head) addLabel(id signal.SeriesID, name []byte, v signal.Value) {
	nameID := uint32(h.sym.Intern(name))
	valueID := uint32(h.sym.Intern(signal.AppendValue(nil, v)))
	h.post.Add(id, nameID, valueID)
}

// indexSorted / ensureIndexSorted let the engine perform the postings' one-time lazy sort under
// the exclusive lock, so concurrent reads never trigger the in-place mutation.
func (h *head) indexSorted() bool  { return h.post.Sorted() }
func (h *head) ensureIndexSorted() { h.post.EnsureSorted() }

// resolve returns the stream ids matching all matchers (their intersection), lowering each
// callback matcher to a postings value scan over the typed value.
//
// A stream that does not carry the label at all is offered [signal.EmptyValue], the same contract
// [fetch.Condition] has per row: the matcher is operator-free, so only the language knows whether
// absence satisfies it (a negation or an is-unset matcher does). Such a matcher unions the value
// scan with the streams lacking the label ([postings.MemPostings.WithoutName]).
func (h *head) resolve(matchers []fetch.Matcher) []signal.SeriesID {
	if len(matchers) == 0 {
		return drain(h.post.All())
	}

	its := make([]postings.Postings, len(matchers))
	for i := range matchers {
		match := matchers[i].Match
		absent := match(signal.EmptyValue())

		nameID, ok := h.sym.Lookup(matchers[i].Name)
		if !ok {
			// No stream carries the label, so every stream is an absent one.
			if !absent {
				return nil
			}

			its[i] = h.post.All()

			continue
		}

		its[i] = h.post.Select(uint32(nameID), func(valueID uint32) bool {
			raw, ok := h.sym.Get(symbols.ID(valueID))
			if !ok {
				return false
			}

			v, _, err := signal.DecodeValue(raw)
			if err != nil {
				return false
			}

			return match(v)
		})

		if absent {
			its[i] = postings.Merge(its[i], h.post.WithoutName(uint32(nameID)))
		}
	}

	return drain(postings.Intersect(its...))
}

func drain(p postings.Postings) []signal.SeriesID {
	out, _ := postings.ToSlice(p)

	return out
}

// recordCount returns the number of records buffered for stream id (an upper bound used to
// pre-size a fetch accumulator).
func (h *head) recordCount(id signal.SeriesID) int {
	if buf := h.records[id]; buf != nil {
		return buf.len()
	}

	return 0
}

// appendWindow appends stream id's buffered records whose timestamp is in [start, end] to acc.
func (h *head) appendWindow(id signal.SeriesID, acc *recordCols, start, end int64) error {
	return appendColsWindow(h.records[id], acc, start, end)
}

// appendColsWindow appends buf's rows whose timestamp is in [start, end] to acc. No-op when buf is
// nil. When the window fully covers the buffer (the common wide-window case) every row is appended in
// one bulk [recordCols.appendRange] — one blob copy per byte column instead of a per-row append that
// re-grows the accumulator's blobs. The buffer is unsorted (arrival order), so the fast path keys off
// the tracked tsMin/tsMax bounds, not the endpoint rows.
func appendColsWindow(buf, acc *recordCols, start, end int64) error {
	if buf == nil || buf.len() == 0 {
		return nil
	}

	// Appending every row bounds any subset of them, so this one check leaves the row loop below
	// unguarded whenever the buffer cannot overflow the accumulator however the window falls.
	name, over := acc.appendRangeOverflows(buf, 0, buf.len())

	if buf.tsMin >= start && buf.tsMax <= end {
		if over {
			return errColumnTooLarge(name)
		}

		acc.appendRange(buf, 0, buf.len())

		return nil
	}

	for i := range buf.ts {
		if buf.ts[i] < start || buf.ts[i] > end {
			continue
		}

		if over {
			if n, rowOver := acc.appendRangeOverflows(buf, i, i+1); rowOver {
				return errColumnTooLarge(n)
			}
		}

		acc.appendRow(buf, i)
	}

	return nil
}

// bufInRange reports whether buf holds any record with timestamp in [start, end]. No-op (false) when
// buf is nil.
func bufInRange(buf *recordCols, start, end int64) bool {
	if buf == nil {
		return false
	}

	for _, t := range buf.ts {
		if t >= start && t <= end {
			return true
		}
	}

	return false
}

// trimBelowCovered drops every buffered record with timestamp ≤ t (now durable in a flushed
// part) for the streams in covered (nil ⇒ every stream), bounding a replica's head to the
// still-unflushed window; each buffer is compacted in place. See the metric head's
// trimBelowCovered for why the replica refresh must not trim a stream absent from the flushed
// parts.
func (h *head) trimBelowCovered(t int64, covered map[signal.SeriesID]struct{}) {
	for id, buf := range h.records {
		if covered != nil {
			if _, ok := covered[id]; !ok {
				continue
			}
		}
		idx := buf.rowScratch[:0]
		for i := range buf.ts {
			if buf.ts[i] > t {
				idx = append(idx, i)
			}
		}

		buf.rowScratch = idx
		if len(idx) != len(buf.ts) {
			buf.gatherRows(idx)
		}
	}

	h.recountBytes()
}

// inFlightBytes is the engine's resident record bytes: the live head plus whatever a flush has
// detached but not yet published. It is what [AppendLimits.MaxInFlightBytes] and the flush-pressure
// trigger meter — dropping the detached part would let a whole second head in on top of the one
// still being written out.
func (h *head) inFlightBytes() int64 { return h.bytes + h.detachedBytes }

// identityBytes is the resident footprint of the head's identity state — symbols, the stream
// index, the postings lists and the per-stream OOO watermarks. It is deliberately **not** part of
// [head.inFlightBytes]: a flush drains buffered records but not identities, so folding the two
// would make a size-triggered flush chase a number it cannot lower. Identity is reported on its
// own ([Stats.IdentityBytes]) because nothing else counts it, and it only grows.
func (h *head) identityBytes() int64 {
	return h.sym.SizeBytes() + h.series.SizeBytes() + h.post.SizeBytes() +
		int64(len(h.streamNewest))*watermarkEntryBytes
}

// watermarkEntryBytes is one streamNewest map entry (stream id → timestamp).
var watermarkEntryBytes = memsize.MapEntry[signal.SeriesID, int64]()

// recountBytes resets the in-flight byte measure from the current buffers (used after a bulk
// mutation like trimBelow that does not track per-record deltas).
func (h *head) recountBytes() {
	var n int64
	for _, buf := range h.records {
		n += buf.byteSize()
	}

	h.bytes = n
	if h.bytes == 0 {
		h.since = time.Time{}
	}
}
