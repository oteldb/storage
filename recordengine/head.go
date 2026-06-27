package recordengine

import (
	"github.com/oteldb/storage/index/postings"
	"github.com/oteldb/storage/index/series"
	"github.com/oteldb/storage/index/symbols"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// Reserved label keys: a stream's scope identity is indexed as labels so a query can match it.
var (
	labelScopeName    = []byte("otel.scope.name")
	labelScopeVersion = []byte("otel.scope.version")
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
	newest  int64 // newest record timestamp, for the OOO window
	bytes   int64 // buffered record bytes; the in-flight memory measure
}

func newHead(schema *Schema) *head {
	return &head{
		schema:  schema,
		sym:     symbols.New(),
		series:  series.New(),
		post:    postings.NewMemPostings(),
		records: make(map[signal.SeriesID]*recordCols),
	}
}

// ensureStream registers and indexes the stream on first sight and makes sure its (full-column)
// record buffer exists. materialize is called only when the stream identity is newly seen. It
// returns whether the identity was newly registered (so the caller logs a stream record to the WAL).
func (h *head) ensureStream(id signal.SeriesID, materialize func() signal.Series, maxSeries int64) (isNew, ok bool) {
	if !h.series.Has(id) {
		// A new stream: reject it if minting it would exceed the cardinality cap. Existing streams
		// are never blocked, so a query keeps returning what is already admitted.
		if maxSeries > 0 && int64(h.series.Len()) >= maxSeries {
			return false, false
		}

		isNew = true
		s := materialize()
		h.series.Add(s)
		h.indexLabels(id, s)
	}

	if h.records[id] == nil {
		h.records[id] = newRecordCols(h.schema, 0, fullSel(h.schema))
	}

	return isNew, true
}

// appendRecord appends r to stream id's buffer (already ensured, or created on demand for the
// replica apply path), rejecting it as out-of-order when older than newest-oooWindow
// (oooWindow > 0). It returns whether the record was accepted.
func (h *head) appendRecord(id signal.SeriesID, r rec, oooWindow, maxBytes int64) admitOutcome {
	if h.newest != 0 && oooWindow > 0 && r.ts < h.newest-oooWindow {
		return rejectOOO
	}

	if maxBytes > 0 && h.bytes >= maxBytes {
		return rejectBytes
	}

	buf := h.records[id]
	if buf == nil {
		buf = newRecordCols(h.schema, 0, fullSel(h.schema))
		h.records[id] = buf
	}

	buf.appendClone(r)
	h.bytes += recByteSize(r)

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
		h.appendRecord(id, recs[i], 0, 0) // replay/replica: authoritative, no admission limits
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
func (h *head) resolve(matchers []fetch.Matcher) []signal.SeriesID {
	if len(matchers) == 0 {
		return drain(h.post.All())
	}

	its := make([]postings.Postings, len(matchers))
	for i := range matchers {
		nameID, ok := h.sym.Lookup(matchers[i].Name)
		if !ok {
			return nil
		}

		match := matchers[i].Match
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
func (h *head) appendWindow(id signal.SeriesID, acc *recordCols, start, end int64) {
	appendColsWindow(h.records[id], acc, start, end)
}

// appendColsWindow appends buf's rows whose timestamp is in [start, end] to acc. No-op when buf is nil.
func appendColsWindow(buf *recordCols, acc *recordCols, start, end int64) {
	if buf == nil {
		return
	}

	for i := range buf.ts {
		if buf.ts[i] >= start && buf.ts[i] <= end {
			acc.appendRow(buf, i)
		}
	}
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

// trimBelow drops every buffered record with timestamp ≤ t (now durable in a flushed part),
// bounding a replica's head to the still-unflushed window. Each buffer is compacted in place.
func (h *head) trimBelow(t int64) {
	for _, buf := range h.records {
		w := 0
		for i := range buf.ts {
			if buf.ts[i] > t {
				if w != i {
					buf.moveRow(i, w)
				}

				w++
			}
		}

		buf.truncate(w)
	}

	h.recountBytes()
}

// recountBytes resets the in-flight byte measure from the current buffers (used after a bulk
// mutation like trimBelow that does not track per-record deltas).
func (h *head) recountBytes() {
	var n int64
	for _, buf := range h.records {
		n += buf.byteSize()
	}

	h.bytes = n
}
