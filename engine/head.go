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
	bytes   int64 // buffered sample bytes (SampleBytes each); the in-flight memory measure
}

type sampleBuf struct {
	ts     []int64
	values []float64
	sf     []float64 // lossy-sampling weights; nil ⇒ every weight is 1, else len == len(ts)
}

// appendSample appends one (ts, value, sf). The sf slice stays nil — and the common path
// allocation-free — until the first non-unit weight arrives, at which point prior weights are
// backfilled to 1.
func (b *sampleBuf) appendSample(ts int64, value, sf float64) {
	b.ts = append(b.ts, ts)
	b.values = append(b.values, value)

	switch {
	case b.sf != nil:
		b.sf = append(b.sf, sf)
	case sf != 1:
		b.sf = make([]float64, len(b.ts)-1, len(b.ts))
		for i := range b.sf {
			b.sf[i] = 1
		}

		b.sf = append(b.sf, sf)
	}
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
	buf.appendSample(ts, value, 1)
	h.bytes += SampleBytes

	if ts > h.newest {
		h.newest = ts
	}

	return id, true, isNew
}

// appendByID adds one sample for the series whose content id is already computed. The full
// identity is materialized lazily — materialize() is called only on first sight, when the
// series must be registered and indexed — so the hot path for a repeat series never builds
// or hashes a [signal.Series]. It enforces limits (OOO window, in-flight bytes, cardinality,
// and the optional soft-budget overflow routing) and returns the per-sample outcome, the
// *effective* id the sample landed under (the original id, or the overflow series' id), whether
// that series was newly seen, and, when new, its materialized identity (for the caller's WAL).
func (h *head) appendByID(
	id signal.SeriesID, ts int64, value, sf float64, oooWindow int64, limits AppendLimits, materialize func() signal.Series,
) (out admitOutcome, effID signal.SeriesID, isNew bool, s signal.Series) {
	if h.newest != 0 && oooWindow > 0 && ts < h.newest-oooWindow {
		return rejectOOO, id, false, signal.Series{}
	}

	// Memory backpressure applies to every sample (known or new): once the head is at the
	// in-flight cap, shed until a flush drains it.
	if limits.MaxInFlightBytes > 0 && h.bytes >= limits.MaxInFlightBytes {
		return rejectBytes, id, false, signal.Series{}
	}

	// One map probe on the hot path: a present sample buffer means the series is already
	// known, so a repeat append never touches the series index. Only when the buffer is
	// absent do we consult the series index (authoritative — WAL replay can register a
	// series before its first live sample) to decide whether to materialize and index it.
	buf := h.samples[id]
	if buf == nil {
		// A new identity (not yet indexed) goes through the cardinality decision; an already-indexed
		// one (WAL replay registered it before its first live sample) just needs a buffer.
		if !h.series.Has(id) {
			var done bool
			if out, effID, isNew, s, done = h.admitNew(id, ts, value, sf, limits, materialize); done {
				return out, effID, isNew, s
			}
		}

		buf = &sampleBuf{}
		h.samples[id] = buf
	}

	buf.appendSample(ts, value, sf)
	h.bytes += SampleBytes

	if ts > h.newest {
		h.newest = ts
	}

	return admitted, id, isNew, s
}

// admitNew decides admission for a series not yet in the index. done=true means the returned outcome
// is final — a hard cardinality reject, or a soft-budget overflow that already appended the sample;
// done=false means the series was registered normally and the caller should buffer the sample under
// id (isNew and s are then set).
func (h *head) admitNew(
	id signal.SeriesID, ts int64, value, sf float64, limits AppendLimits, materialize func() signal.Series,
) (out admitOutcome, effID signal.SeriesID, isNew bool, s signal.Series, done bool) {
	cardinality := int64(h.series.Len())

	// Hard ceiling: reject a new series that would exceed MaxSeries. Existing series are never
	// blocked, so a query keeps returning what is already admitted.
	if limits.MaxSeries > 0 && cardinality >= limits.MaxSeries {
		return rejectCardinality, id, false, signal.Series{}, true
	}

	// Soft budget: between MaxSeriesSoft and MaxSeries a new series is routed to a caller-built
	// overflow series instead of registered, bounding cardinality while keeping the tenant's
	// aggregates approximately right. Only on the degraded path — no steady-state hot-path cost.
	if limits.Overflow != nil && limits.MaxSeriesSoft > 0 && cardinality >= limits.MaxSeriesSoft {
		o, oid, oNew, oS := h.appendOverflow(limits.Overflow(materialize()), ts, value, sf)

		return o, oid, oNew, oS, true
	}

	s = materialize()
	h.series.Add(s)
	h.indexLabels(id, s)

	return admitted, id, true, s, false
}

// appendOverflow appends a sample to the overflow series ov (the soft-budget redirect target). The
// overflow series is exempt from the cardinality cap — there are few of them (one per metric name) —
// so it is registered on first sight regardless. Returns the overflow id so the caller logs the WAL
// under the overflow identity, matching the head.
func (h *head) appendOverflow(ov signal.Series, ts int64, value, sf float64) (admitOutcome, signal.SeriesID, bool, signal.Series) {
	oid := ov.Hash()

	isNew := false

	buf := h.samples[oid]
	if buf == nil {
		if !h.series.Has(oid) {
			isNew = true
			h.series.Add(ov)
			h.indexLabels(oid, ov)
		}

		buf = &sampleBuf{}
		h.samples[oid] = buf
	}

	buf.appendSample(ts, value, sf)
	h.bytes += SampleBytes

	if ts > h.newest {
		h.newest = ts
	}

	return admittedOverflow, oid, isNew, ov
}

// trimBelow drops every buffered sample with timestamp ≤ t (now durable in a flushed part),
// bounding a replica's head to the still-unflushed window. Each buffer is compacted in place.
func (h *head) trimBelow(t int64) {
	for _, buf := range h.samples {
		ts := buf.ts[:0]
		vs := buf.values[:0]

		for i := range buf.ts {
			if buf.ts[i] > t {
				ts = append(ts, buf.ts[i])
				vs = append(vs, buf.values[i])
			}
		}

		buf.ts, buf.values = ts, vs
	}

	h.recountBytes()
}

// recountBytes resets the in-flight byte measure from the current buffers (used after a bulk
// mutation like trimBelow that does not track per-sample deltas).
func (h *head) recountBytes() {
	var n int64
	for _, buf := range h.samples {
		n += int64(len(buf.ts))
	}

	h.bytes = n * SampleBytes
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
	// The WAL does not yet carry scale factors, so replayed samples take weight 1; keep the sf
	// slice aligned if it was already materialized.
	if buf.sf != nil {
		for range ts {
			buf.sf = append(buf.sf, 1)
		}
	}

	h.bytes += int64(len(ts)) * SampleBytes

	for _, t := range ts {
		if t > h.newest {
			h.newest = t
		}
	}
}

// replaySamplesSF restores samples that carried lossy-sampling scale factors (WAL recovery), so a
// crash recovers unflushed *sampled* data at its representative weight rather than weight 1. It
// appends per sample through appendSample, which materializes the buffer's sf slice on the first
// non-unit weight.
func (h *head) replaySamplesSF(id signal.SeriesID, ts []int64, values, sf []float64) {
	if !h.series.Has(id) {
		return // series record missing; ignore (defensive)
	}

	buf := h.bufFor(id)
	for i := range ts {
		buf.appendSample(ts[i], values[i], sf[i])

		if ts[i] > h.newest {
			h.newest = ts[i]
		}
	}

	h.bytes += int64(len(ts)) * SampleBytes
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

// indexSorted reports whether the label index is sorted (so a read will not mutate it).
func (h *head) indexSorted() bool { return h.post.Sorted() }

// ensureIndexSorted sorts the label index in place. The engine calls it while holding the
// exclusive lock so that later concurrent [head.resolve] reads never trigger the lazy sort.
func (h *head) ensureIndexSorted() { h.post.EnsureSorted() }

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
	s, _ := h.series.Get(id)

	return bufBatch(h.samples[id], id, s, start, end)
}

// bufBatch builds a fetch batch from one sample buffer within [start, end] (with the given identity),
// or nil if buf is nil or has no samples in the window. Used for both the live head and the buffers a
// flush has detached but not yet published as a part.
func bufBatch(buf *sampleBuf, id signal.SeriesID, s signal.Series, start, end int64) *fetch.Batch {
	if buf == nil {
		return nil
	}

	ts, values, sf := sortedWindow(buf, start, end)
	if len(ts) == 0 {
		return nil
	}

	return &fetch.Batch{ID: id, Series: s, Timestamps: ts, Values: values, ScaleFactors: sf}
}

// detach moves the head's sample buffers aside for a flush and installs fresh empty buffers, so new
// appends are unaffected, returning the detached buffers (nil if no series holds a sample). The series
// index is retained — identities outlive a flush. The caller keeps the detached buffers readable until
// the flushed part is published, so a concurrent fetch never loses sight of the samples mid-flush.
func (h *head) detach() map[signal.SeriesID]*sampleBuf {
	hasRows := false
	for _, buf := range h.samples {
		if len(buf.ts) > 0 {
			hasRows = true

			break
		}
	}

	if !hasRows {
		return nil
	}

	detached := h.samples
	h.samples = make(map[signal.SeriesID]*sampleBuf)
	h.bytes = 0

	return detached
}

type tsv struct {
	ts  int64
	val float64
	sf  float64
}

// sortedWindow returns the buffer's samples within [start, end], sorted by timestamp. The
// returned sf slice is nil when every weight is 1 (the unsampled common case), else len == len(ts).
func sortedWindow(buf *sampleBuf, start, end int64) ([]int64, []float64, []float64) {
	// Fast path: head samples are usually ingested in order, so the buffer is already ascending —
	// skip the sortable scratch and the sort, window-copying directly.
	if ascendingInt64(buf.ts) {
		return windowCopy(buf, start, end)
	}

	pairs := make([]tsv, len(buf.ts))
	for i := range buf.ts {
		pairs[i] = tsv{ts: buf.ts[i], val: buf.values[i], sf: bufSF(buf, i)}
	}

	slices.SortFunc(pairs, func(a, b tsv) int { return cmp.Compare(a.ts, b.ts) })

	var (
		ts     []int64
		values []float64
		sf     []float64
	)

	for _, p := range pairs {
		if p.ts < start || p.ts > end {
			continue
		}

		ts = append(ts, p.ts)
		values = append(values, p.val)

		if p.sf != 1 && sf == nil {
			sf = make([]float64, len(ts)-1, len(pairs))
			for i := range sf {
				sf[i] = 1
			}
		}

		if sf != nil {
			sf = append(sf, p.sf)
		}
	}

	return ts, values, sf
}

// bufSF returns sample i's weight from a sample buffer, defaulting to 1 when the buffer carries
// no weights.
func bufSF(buf *sampleBuf, i int) float64 {
	if buf.sf == nil {
		return 1
	}

	return buf.sf[i]
}

// ascendingInt64 reports whether s is non-decreasing — the common case for a head buffer ingested
// in time order, letting sortedWindow skip its sortable scratch and sort.
func ascendingInt64(s []int64) bool {
	for i := 1; i < len(s); i++ {
		if s[i] < s[i-1] {
			return false
		}
	}

	return true
}

// windowCopy copies the buffer's [start, end] samples in order (the buffer is already ascending),
// pre-sizing the result to the buffer length. The sf slice stays nil until the first non-unit weight.
func windowCopy(buf *sampleBuf, start, end int64) ([]int64, []float64, []float64) {
	ts := make([]int64, 0, len(buf.ts))
	values := make([]float64, 0, len(buf.ts))

	var sf []float64

	for i := range buf.ts {
		t := buf.ts[i]
		if t < start || t > end {
			continue
		}

		ts = append(ts, t)
		values = append(values, buf.values[i])
		sf = appendWeight(sf, bufSF(buf, i), len(ts), len(buf.ts))
	}

	return ts, values, sf
}
