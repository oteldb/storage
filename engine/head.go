package engine

import (
	"cmp"
	"slices"

	"github.com/oteldb/storage/index/postings"
	"github.com/oteldb/storage/index/series"
	"github.com/oteldb/storage/index/symbols"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// Reserved label keys: scope identity fields are indexed as labels so a query can match
// them (the metric name/unit/etc. are already folded into the series attributes by
// signal/metric).
var (
	labelScopeName    = []byte("otel.scope.name")
	labelScopeVersion = []byte("otel.scope.version")
)

// head is the in-memory, mutable write buffer: the index (symbols + series + postings)
// plus per-series sample buffers in arrival order (sorted on read). It is not safe for
// concurrent use; the [Engine] holds the lock.
type head struct {
	sym     *symbols.Table
	series  *series.Index
	post    *postings.MemPostings
	samples map[signal.SeriesID]*sampleBuf
	newest  int64 // newest timestamp seen, for the OOO window
}

type sampleBuf struct {
	ts     []int64
	values []float64
}

func newHead() *head {
	return &head{
		sym:     symbols.New(),
		series:  series.New(),
		post:    postings.NewMemPostings(),
		samples: make(map[signal.SeriesID]*sampleBuf),
	}
}

// append adds one sample for series s, registering and indexing it on first sight. A
// sample older than newest-oooWindow (when oooWindow > 0) is rejected. It returns the
// series id, whether the sample was accepted, and whether the series was newly seen (so
// the caller logs a series record to the WAL).
func (h *head) append(s signal.Series, ts int64, value float64, oooWindow int64) (id signal.SeriesID, accepted, isNew bool) {
	id = s.Hash()
	if h.newest != 0 && oooWindow > 0 && ts < h.newest-oooWindow {
		return id, false, false
	}

	if !h.series.Has(id) {
		isNew = true

		h.series.Add(s)
		h.indexLabels(id, s)
	}

	buf := h.bufFor(id)
	buf.ts = append(buf.ts, ts)
	buf.values = append(buf.values, value)

	if ts > h.newest {
		h.newest = ts
	}

	return id, true, isNew
}

// appendByID adds one sample for the series whose content id is already computed. The full
// identity is materialized lazily — materialize() is called only on first sight, when the
// series must be registered and indexed — so the hot path for a repeat series never builds
// or hashes a [signal.Series]. It returns whether the sample was accepted (false ⇒ out of
// the OOO window), whether the series was newly seen, and, when new, the materialized
// identity (for the caller's WAL series record).
func (h *head) appendByID(
	id signal.SeriesID, ts int64, value float64, oooWindow int64, materialize func() signal.Series,
) (accepted, isNew bool, s signal.Series) {
	if h.newest != 0 && oooWindow > 0 && ts < h.newest-oooWindow {
		return false, false, signal.Series{}
	}

	// One map probe on the hot path: a present sample buffer means the series is already
	// known, so a repeat append never touches the series index. Only when the buffer is
	// absent do we consult the series index (authoritative — WAL replay can register a
	// series before its first live sample) to decide whether to materialize and index it.
	buf := h.samples[id]
	if buf == nil {
		if !h.series.Has(id) {
			isNew = true
			s = materialize()
			h.series.Add(s)
			h.indexLabels(id, s)
		}

		buf = &sampleBuf{}
		h.samples[id] = buf
	}

	buf.ts = append(buf.ts, ts)
	buf.values = append(buf.values, value)

	if ts > h.newest {
		h.newest = ts
	}

	return true, isNew, s
}

// bufFor returns the (created-on-demand) sample buffer for an already-registered series.
func (h *head) bufFor(id signal.SeriesID) *sampleBuf {
	buf := h.samples[id]
	if buf == nil {
		buf = &sampleBuf{}
		h.samples[id] = buf
	}

	return buf
}

// registerSeries records and indexes a series identity without samples (used by WAL
// replay, where the series record precedes its sample records).
func (h *head) registerSeries(s signal.Series) {
	id := s.Hash()
	if !h.series.Has(id) {
		h.series.Add(s)
		h.indexLabels(id, s)
	}
}

// replaySamples appends a run of samples to an already-registered series (WAL replay; no
// OOO rejection — logged samples are authoritative).
func (h *head) replaySamples(id signal.SeriesID, ts []int64, values []float64) {
	if !h.series.Has(id) {
		return // series record missing; ignore (defensive)
	}

	buf := h.bufFor(id)
	buf.ts = append(buf.ts, ts...)
	buf.values = append(buf.values, values...)

	for _, t := range ts {
		if t > h.newest {
			h.newest = t
		}
	}
}

// indexLabels interns and registers every queryable label of the series — resource and
// scope attributes, the scope name/version, and the (folded) point attributes — into the
// postings index under id.
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

	for i := range s.Attributes {
		h.addLabel(id, s.Attributes[i].Key, s.Attributes[i].Value)
	}
}

func (h *head) addLabel(id signal.SeriesID, name []byte, v signal.Value) {
	nameID := uint32(h.sym.Intern(name))
	valueID := uint32(h.sym.Intern(signal.AppendValue(nil, v)))
	h.post.Add(id, nameID, valueID)
}

// resolve returns the SeriesIDs matching all matchers (their intersection), lowering each
// callback matcher to a postings value scan over the typed value.
func (h *head) resolve(matchers []fetch.Matcher) []signal.SeriesID {
	if len(matchers) == 0 {
		return drain(h.post.All())
	}

	its := make([]postings.Postings, len(matchers))
	for i := range matchers {
		nameID, ok := h.sym.Lookup(matchers[i].Name)
		if !ok {
			return nil // no series carries this label
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

// batch builds a fetch batch for series id within [start, end], or nil if it has no
// samples in the window.
func (h *head) batch(id signal.SeriesID, start, end int64) *fetch.Batch {
	buf := h.samples[id]
	if buf == nil {
		return nil
	}

	ts, values := sortedWindow(buf, start, end)
	if len(ts) == 0 {
		return nil
	}

	s, _ := h.series.Get(id)

	return &fetch.Batch{ID: id, Series: s, Timestamps: ts, Values: values}
}

type tsv struct {
	ts  int64
	val float64
}

// sortedWindow returns the buffer's samples within [start, end], sorted by timestamp.
func sortedWindow(buf *sampleBuf, start, end int64) ([]int64, []float64) {
	pairs := make([]tsv, len(buf.ts))
	for i := range buf.ts {
		pairs[i] = tsv{buf.ts[i], buf.values[i]}
	}

	slices.SortFunc(pairs, func(a, b tsv) int { return cmp.Compare(a.ts, b.ts) })

	var (
		ts     []int64
		values []float64
	)

	for _, p := range pairs {
		if p.ts < start || p.ts > end {
			continue
		}

		ts = append(ts, p.ts)
		values = append(values, p.val)
	}

	return ts, values
}
