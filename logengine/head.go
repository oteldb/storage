package logengine

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

// head is the logs engine's in-memory write buffer: the identity index (symbols + series +
// postings over stream labels) plus per-stream record buffers in arrival order (sorted on read).
// It is not safe for concurrent use; the [Engine] holds the lock.
type head struct {
	sym     *symbols.Table
	series  *series.Index
	post    *postings.MemPostings
	records map[signal.SeriesID]*recordCols
	newest  int64 // newest record timestamp, for the OOO window
}

func newHead() *head {
	return &head{
		sym:     symbols.New(),
		series:  series.New(),
		post:    postings.NewMemPostings(),
		records: make(map[signal.SeriesID]*recordCols),
	}
}

// ensureStream registers and indexes the stream on first sight and makes sure its record buffer
// exists. materialize is called only when the stream identity is newly seen. It returns whether
// the identity was newly registered (so the caller logs a stream record to the WAL).
func (h *head) ensureStream(id signal.SeriesID, materialize func() signal.Series) (isNew bool) {
	if !h.series.Has(id) {
		isNew = true
		s := materialize()
		h.series.Add(s)
		h.indexLabels(id, s)
	}

	if h.records[id] == nil {
		h.records[id] = &recordCols{}
	}

	return isNew
}

// appendRecord appends r to stream id's buffer (already ensured), rejecting it as out-of-order
// when older than newest-oooWindow (oooWindow > 0). It returns whether the record was accepted.
func (h *head) appendRecord(id signal.SeriesID, r rec, oooWindow int64) bool {
	if h.newest != 0 && oooWindow > 0 && r.ts < h.newest-oooWindow {
		return false
	}

	buf := h.records[id]
	if buf == nil { // tolerate a registered-but-unbuffered stream (replica apply path)
		buf = &recordCols{}
		h.records[id] = buf
	}

	buf.appendClone(r)

	if r.ts > h.newest {
		h.newest = r.ts
	}

	return true
}

// registerStream records and indexes a stream identity without records (WAL replay / load, where
// the stream record precedes its log records).
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

	if h.records[id] == nil {
		h.records[id] = &recordCols{}
	}

	for i := range recs {
		h.records[id].appendClone(recs[i])
		if recs[i].ts > h.newest {
			h.newest = recs[i].ts
		}
	}
}

// indexLabels interns and registers every queryable label of the stream — resource and scope
// attributes plus the scope name/version — into the postings index under id.
func (h *head) indexLabels(id signal.SeriesID, s signal.Series) {
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
// the exclusive lock, so concurrent reads never trigger the in-place mutation (see Engine.Fetch).
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
			return nil // no stream carries this label
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
	out, _ := postings.ToSlice(p) // memory postings never errors

	return out
}

// appendWindow appends stream id's buffered records whose timestamp is in [start, end] to acc.
func (h *head) appendWindow(id signal.SeriesID, acc *recordCols, start, end int64) {
	buf := h.records[id]
	if buf == nil {
		return
	}

	for i := range buf.ts {
		if buf.ts[i] >= start && buf.ts[i] <= end {
			acc.appendRow(buf, i)
		}
	}
}

// trimBelow drops every buffered record with timestamp ≤ t (now durable in a flushed part),
// bounding a replica's head to the still-unflushed window. Each buffer is compacted in place.
func (h *head) trimBelow(t int64) {
	for _, buf := range h.records {
		kept := buf.ts[:0]
		keepIdx := make([]int, 0, len(buf.ts))

		for i := range buf.ts {
			if buf.ts[i] > t {
				keepIdx = append(keepIdx, i)
				kept = append(kept, buf.ts[i])
			}
		}

		buf.ts = kept
		buf.observed = compact(buf.observed, keepIdx)
		buf.severity = compact(buf.severity, keepIdx)
		buf.flags = compact(buf.flags, keepIdx)
		buf.dropped = compact(buf.dropped, keepIdx)
		buf.sevText = compact(buf.sevText, keepIdx)
		buf.body = compact(buf.body, keepIdx)
		buf.traceID = compact(buf.traceID, keepIdx)
		buf.spanID = compact(buf.spanID, keepIdx)
		buf.attrs = compact(buf.attrs, keepIdx)
	}
}

// compact rewrites s in place to keep only the rows in keepIdx (ascending).
func compact[T any](s []T, keepIdx []int) []T {
	out := s[:0]
	for _, i := range keepIdx {
		out = append(out, s[i])
	}

	return out
}
