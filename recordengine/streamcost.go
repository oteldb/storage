package recordengine

import (
	"cmp"
	"context"
	"fmt"
	"slices"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/block"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/index/bloom"
	"github.com/oteldb/storage/signal"
)

// StreamCostOptions selects what [Engine.StreamCost] attributes and how much of it to estimate.
type StreamCostOptions struct {
	// GroupBy is the stream label (a resource or scope attribute name, e.g. "service.name") whose
	// value keys the report. Empty groups by raw stream id. Grouping by label is the useful form:
	// what an operator can act on is a service, and stream identity is a field policy that can
	// change under them.
	GroupBy string

	// Columns restricts the byte columns that are decoded and attributed (nil ⇒ every byte column).
	// The int columns and the timestamp are always accounted — they need no decode — so a narrowed
	// request still reports complete row counts, and RawBytes/DiskBytes cover exactly the columns
	// listed in each group's Columns.
	Columns []string

	// TopN keeps only the N costliest groups by RawBytes (≤0 ⇒ every group).
	TopN int

	// MaxSketchGroups bounds how many groups carry distinct estimates, which cost 8 KiB of sketch
	// each for the duration of one column's pass (≤0 ⇒ [DefaultStreamCostSketchGroups]). The budget
	// goes to the groups with the most rows; the rest report DistinctEstimated false.
	MaxSketchGroups int
}

// DefaultStreamCostSketchGroups is [StreamCostOptions.MaxSketchGroups]'s default: 4096 groups, i.e.
// 32 MiB of transient sketch state. It is set to cover a real store's group count (a corpus grouped
// by a pod-suffixed service.name produced 2256) rather than to ration the sketches — 8 KiB per group
// is negligible against the column decode the pass is already doing. The cap exists for the
// pathological case, grouping a million-stream store by raw stream id.
const DefaultStreamCostSketchGroups = 4096

// StreamCostStat is one group's (one label value's, or one stream's) share of the engine's flushed
// parts. The head is not included — it holds no compressed bytes to attribute.
type StreamCostStat struct {
	Key     string // the GroupBy label's value, or the stream id; empty ⇒ the label is absent
	Streams int    // distinct streams folded into this group
	Rows    int64

	// RawBytes is the decoded footprint of the group's rows over the accounted columns.
	RawBytes int64

	// DiskBytes is APPROXIMATE. Compression is per column per frame and a frame spans whatever
	// streams its rows fall in, so a group's compressed footprint is not directly measurable: each
	// frame's compressed size is apportioned across the groups holding its rows by their raw-byte
	// share. Rows are (stream, ts)-ordered, so most frames hold one stream and the estimate is
	// close; a group narrower than a frame is the case where it is not.
	DiskBytes int64

	// DistinctEstimated reports whether the distinct counts below were computed for this group — see
	// [StreamCostOptions.MaxSketchGroups]. False ⇒ Distinct/DistinctNormalized are 0, not zero.
	DistinctEstimated bool

	Columns []ColumnCostStat
}

// ColumnCostStat is one column's share of a group's cost.
type ColumnCostStat struct {
	Name      string
	RawBytes  int64
	DiskBytes int64 // approximate, as [StreamCostStat.DiskBytes]

	// Distinct estimates the distinct values the group's rows hold in this column (HyperLogLog,
	// ~1.6% standard error). 0 for an int column and for a group outside the sketch budget.
	Distinct int64

	// DistinctNormalized is Distinct over values with every run of ASCII digits collapsed to '#'.
	// It is what separates a genuinely high-entropy column from a templated one carrying an embedded
	// timestamp or id: a group whose Distinct is large and whose DistinctNormalized is tiny is not a
	// storage problem but a parsing one — the same line, repeated, never turned into fields.
	DistinctNormalized int64
}

// costCol is one accounted column: its name, how a row's raw bytes are measured, and whether the
// pass has to decode it.
type costCol struct {
	name  string
	width int // fixed raw bytes per row; 0 for a byte column, whose rows are measured
	bytes bool
}

// costGroup accumulates one group across every part and column.
type costGroup struct {
	key      string
	streams  int
	rows     int64
	sketched bool
	cols     []costColumnAcc
}

type costColumnAcc struct {
	raw  int64
	disk float64
	// distinct/normalized are the current column's estimates, folded in when its pass ends.
	distinct, normalized int64
}

// streamSpan is a stream's row range in a part, tagged with the group it belongs to.
type streamSpan struct {
	rowRange

	group int
}

// StreamCost attributes the engine's flushed parts to streams (or, with
// [StreamCostOptions.GroupBy], to a label's values): rows, decoded bytes, an approximate compressed
// share, and per-column distinct estimates.
//
// It answers "which service is costing me, and why" — the diagnosis the label-cardinality and
// per-part views cannot give, since neither attributes bytes to a stream nor says anything about the
// cardinality of the values inside the columns, which is what drives the cost.
//
// It is the heaviest introspection call in the engine: every accounted byte column of every live
// part is read and decoded once (int columns and the timestamp are accounted arithmetically, with no
// decode). Run it as an operator drill-down, not on a schedule, and narrow it with
// [StreamCostOptions.Columns] when only one column is in question. Each part is ref-held for the
// duration, so a concurrent merge cannot reclaim it mid-read.
func (e *Engine) StreamCost(ctx context.Context, opts StreamCostOptions) ([]StreamCostStat, error) {
	parts, spans, groups := e.streamCostPlan(opts)
	defer func() {
		for _, p := range parts {
			p.release()
		}
	}()

	if len(groups) == 0 {
		return nil, nil
	}

	cols := e.costColumns(opts.Columns)
	for i := range groups {
		groups[i].cols = make([]costColumnAcc, len(cols))
	}

	budgetSketches(groups, opts.MaxSketchGroups)

	sk := newSketchSet(groups)

	for ci := range cols {
		for pi, p := range parts {
			if err := accountColumn(ctx, p, &cols[ci], ci, spans[pi], groups, sk); err != nil {
				return nil, errors.Wrapf(err, "part %q column %q", p.prefix, cols[ci].name)
			}
		}

		sk.fold(groups, ci)
	}

	return finishStreamCost(groups, cols, opts.TopN), nil
}

// streamCostPlan snapshots the live parts (ref-held) and resolves every stream they hold to its
// group. Row counts come from the parts' in-memory row-range index, so the group set and its row
// counts are known before anything is decoded — which is what lets the sketch budget go to the
// groups that matter.
//
// Resolution takes the read lock once per part rather than once for the whole plan: the work is
// proportional to the store's total stream count, so a single lock would stall the maintenance loop
// for as long as that takes (102 ms at 337k streams, and it grows linearly). Sorting happens off
// lock. An identity pruned between two parts falls back to the stream id, which
// [Engine.groupKeyLocked] already does for identities retention has dropped.
func (e *Engine) streamCostPlan(opts StreamCostOptions) (parts []*part, spans [][]streamSpan, groups []costGroup) {
	parts = e.acquireParts()

	pl := costPlanner{e: e, byKey: make(map[string]int), groupBy: []byte(opts.GroupBy)}
	spans = make([][]streamSpan, len(parts))

	for pi, p := range parts {
		ss := pl.planPart(p)

		slices.SortFunc(ss, func(a, b streamSpan) int { return cmp.Compare(a.start, b.start) })
		spans[pi] = ss
	}

	return parts, spans, pl.groups
}

// acquireParts snapshots the live parts and ref-holds each, so a concurrent merge cannot reclaim one
// mid-read.
func (e *Engine) acquireParts() []*part {
	e.mu.RLock()
	defer e.mu.RUnlock()

	parts := make([]*part, len(e.parts))
	copy(parts, e.parts)

	for _, p := range parts {
		p.acquire()
	}

	return parts
}

// costPlanner carries the group set across per-part resolutions.
type costPlanner struct {
	e       *Engine
	byKey   map[string]int
	groups  []costGroup
	keyBuf  []byte
	groupBy []byte
}

func (pl *costPlanner) planPart(p *part) []streamSpan {
	pl.e.mu.RLock()
	defer pl.e.mu.RUnlock()

	ss := make([]streamSpan, 0, len(p.ranges))

	for _, sr := range p.ranges {
		pl.keyBuf = pl.e.groupKeyLocked(pl.keyBuf[:0], sr.id, pl.groupBy)

		gi, ok := pl.byKey[string(pl.keyBuf)]
		if !ok {
			gi = len(pl.groups)
			pl.byKey[string(pl.keyBuf)] = gi
			pl.groups = append(pl.groups, costGroup{key: string(pl.keyBuf)})
		}

		pl.groups[gi].streams++
		pl.groups[gi].rows += int64(sr.end - sr.start)

		ss = append(ss, streamSpan{rowRange: sr.rowRange, group: gi})
	}

	return ss
}

// groupKeyLocked renders the stream's grouping key into dst: the named resource/scope attribute's
// value, or the stream id when no label was requested (or the identity is no longer resident, which
// retention's identity prune can leave behind). Caller holds e.mu.
func (e *Engine) groupKeyLocked(dst []byte, id signal.SeriesID, name []byte) []byte {
	if len(name) == 0 {
		return fmt.Appendf(dst, "%016x%016x", id.Hi, id.Lo)
	}

	s, ok := e.head.series.Get(id)
	if !ok {
		return fmt.Appendf(dst, "%016x%016x", id.Hi, id.Lo)
	}

	if v, ok := s.Resource.Attributes.Get(name); ok {
		return v.AppendText(dst)
	}

	if v, ok := s.Scope.Attributes.Get(name); ok {
		return v.AppendText(dst)
	}

	return dst
}

// costColumns lists the columns the pass accounts: the implicit stream id and timestamp, every int
// column, and the byte columns the request selected.
func (e *Engine) costColumns(want []string) []costCol {
	schema := e.cfg.Schema

	cols := []costCol{
		{name: colStream, width: 16},
		{name: colTs, width: 8},
	}

	for k := range schema.intCols {
		cols = append(cols, costCol{name: schema.intColumn(k).Name, width: 8})
	}

	for k := range schema.byteCols {
		name := schema.byteColumn(k).Name
		if want != nil && !slices.Contains(want, name) {
			continue
		}

		cols = append(cols, costCol{name: name, bytes: true})
	}

	return cols
}

// budgetSketches marks the groups that get distinct estimates: the ones with the most rows, capped
// by the budget. Row counts are free (they come from the row-range index), so the ranking costs
// nothing and is a good stand-in for the byte ranking the pass has not computed yet.
func budgetSketches(groups []costGroup, maxGroups int) {
	if maxGroups <= 0 {
		maxGroups = DefaultStreamCostSketchGroups
	}

	order := make([]int, len(groups))
	for i := range order {
		order[i] = i
	}

	slices.SortFunc(order, func(a, b int) int {
		if groups[a].rows != groups[b].rows {
			return cmp.Compare(groups[b].rows, groups[a].rows)
		}

		return cmp.Compare(groups[a].key, groups[b].key)
	})

	for _, gi := range order[:min(maxGroups, len(order))] {
		groups[gi].sketched = true
	}
}

// sketchSet holds one distinct/normalized sketch pair per sketched group, re-armed per column — so
// the sketch state is bounded by the budget rather than by (groups × columns).
type sketchSet struct {
	slot       []int // group index → slot, -1 for an unsketched group
	distinct   []bloom.Sketch
	normalized []bloom.Sketch
	scratch    []byte
}

func newSketchSet(groups []costGroup) *sketchSet {
	s := &sketchSet{slot: make([]int, len(groups))}

	n := 0

	for i := range groups {
		if !groups[i].sketched {
			s.slot[i] = -1

			continue
		}

		s.slot[i] = n
		n++
	}

	s.distinct = make([]bloom.Sketch, n)
	s.normalized = make([]bloom.Sketch, n)

	return s
}

func (s *sketchSet) add(group int, v []byte) {
	slot := s.slot[group]
	if slot < 0 {
		return
	}

	s.distinct[slot].Add(v)

	s.scratch = appendCollapseDigits(s.scratch[:0], v)
	s.normalized[slot].Add(s.scratch)
}

// fold moves the current column's estimates into the groups and re-arms the sketches for the next.
func (s *sketchSet) fold(groups []costGroup, col int) {
	for gi := range groups {
		slot := s.slot[gi]
		if slot < 0 {
			continue
		}

		groups[gi].cols[col].distinct = int64(s.distinct[slot].Estimate())
		groups[gi].cols[col].normalized = int64(s.normalized[slot].Estimate())

		s.distinct[slot].Reset()
		s.normalized[slot].Reset()
	}
}

// appendCollapseDigits appends v to dst with every maximal run of ASCII digits replaced by a single
// '#'. Callers reuse dst, so the collapse allocates nothing per value.
func appendCollapseDigits(dst, v []byte) []byte {
	for i := 0; i < len(v); {
		c := v[i]
		if c < '0' || c > '9' {
			dst = append(dst, c)
			i++

			continue
		}

		for i < len(v) && v[i] >= '0' && v[i] <= '9' {
			i++
		}

		dst = append(dst, '#')
	}

	return dst
}

// accountColumn attributes one column of one part. A byte column is decoded once and measured per
// row; an int column's rows are a fixed width, so its share follows from the row spans alone.
func accountColumn(
	ctx context.Context, p *part, col *costCol, ci int, spans []streamSpan, groups []costGroup, sk *sketchSet,
) error {
	if len(spans) == 0 {
		return nil
	}

	cr, err := p.reader.Column(ctx, col.name)
	if err != nil {
		return err
	}

	frames, err := cr.Frames()
	if err != nil {
		return err
	}

	if len(frames) == 0 {
		return nil
	}

	var dc *chunk.DictColumn

	if col.bytes {
		if dc, err = cr.Bytes(); err != nil {
			return err
		}
	}

	// Raw bytes are accumulated per (group, frame) so the frame's compressed size can be split by
	// the share of its raw bytes each group holds — the finest split compression permits.
	frameRaw := make([]int64, len(frames))
	pairRaw := make(map[int64]int64, len(spans))

	eachOverlap(spans, frames, func(gi, fi, lo, hi int) {
		var raw int64

		if col.bytes {
			for row := lo; row < hi; row++ {
				v := dc.At(row)
				raw += int64(len(v))
				sk.add(gi, v)
			}
		} else {
			raw = int64(hi-lo) * int64(col.width)
		}

		groups[gi].cols[ci].raw += raw
		frameRaw[fi] += raw
		pairRaw[int64(gi)<<32|int64(fi)] += raw
	})

	apportion(cr.ObjectBytes(), frames, frameRaw, pairRaw, groups, ci)

	return nil
}

// eachOverlap calls fn for each (stream span × frame) overlap, in ascending row order. Both inputs
// tile [0, rows) in order — a part's rows are (stream, ts)-sorted, so each stream is one contiguous
// run — so one merged walk covers them.
func eachOverlap(spans []streamSpan, frames []block.FrameExtent, fn func(group, frame, lo, hi int)) {
	fi := 0

	for _, s := range spans {
		for row := s.start; row < s.end; {
			for fi < len(frames) && frames[fi].EndRow <= row {
				fi++
			}

			if fi == len(frames) {
				return
			}

			hi := min(s.end, frames[fi].EndRow)
			if hi > row {
				fn(s.group, fi, row, hi)
			}

			row = hi
		}
	}
}

// apportion splits the column object's compressed bytes across the groups. The object is split
// between frames by their compressed sizes, and each frame between its groups by their raw-byte
// share — the estimate [StreamCostStat.DiskBytes] documents. The object (rather than the summed
// frame sizes) is the numerator so a group's shares still add up to what the column occupies,
// directory and shared dictionary included.
func apportion(objectBytes int64, frames []block.FrameExtent, frameRaw []int64, pairRaw map[int64]int64, groups []costGroup, ci int) {
	var totalFrame int64
	for _, f := range frames {
		totalFrame += f.Bytes
	}

	if totalFrame <= 0 || objectBytes <= 0 {
		return
	}

	scale := float64(objectBytes) / float64(totalFrame)

	for pair, raw := range pairRaw {
		gi, fi := int(pair>>32), int(pair&0xffffffff)
		if frameRaw[fi] <= 0 {
			continue
		}

		share := float64(raw) / float64(frameRaw[fi])
		groups[gi].cols[ci].disk += float64(frames[fi].Bytes) * scale * share
	}
}

// finishStreamCost renders the accumulators into the public shape, sorted by RawBytes descending.
func finishStreamCost(groups []costGroup, cols []costCol, topN int) []StreamCostStat {
	out := make([]StreamCostStat, 0, len(groups))

	for gi := range groups {
		g := &groups[gi]

		st := StreamCostStat{
			Key: g.key, Streams: g.streams, Rows: g.rows, DistinctEstimated: g.sketched,
			Columns: make([]ColumnCostStat, 0, len(cols)),
		}

		for ci := range cols {
			acc := g.cols[ci]
			disk := int64(acc.disk + 0.5)

			st.RawBytes += acc.raw
			st.DiskBytes += disk
			st.Columns = append(st.Columns, ColumnCostStat{
				Name: cols[ci].name, RawBytes: acc.raw, DiskBytes: disk,
				Distinct: acc.distinct, DistinctNormalized: acc.normalized,
			})
		}

		out = append(out, st)
	}

	slices.SortFunc(out, func(a, b StreamCostStat) int {
		if a.RawBytes != b.RawBytes {
			return cmp.Compare(b.RawBytes, a.RawBytes)
		}

		return cmp.Compare(a.Key, b.Key)
	})

	if topN > 0 && len(out) > topN {
		out = out[:topN]
	}

	return out
}
