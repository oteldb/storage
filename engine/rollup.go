package engine

import (
	"context"
	"math"
	"slices"

	"github.com/oteldb/storage/block"
	"github.com/oteldb/storage/encoding/compress"
	"github.com/oteldb/storage/internal/mergestream"
	"github.com/oteldb/storage/signal"
)

// rollupMarker is the canonical marker for a layout: its active tiers, widest first, with a tier
// dropped where a same-(Interval, Agg) tier with a later Before already covers it.
func rollupMarker(tiers []DownsampleTier) block.Rollup {
	var r block.Rollup

	for _, t := range compactLayout(tiers) {
		r.Tiers = append(r.Tiers, block.RollupTier{Before: t.Before, Interval: t.Interval, Agg: uint8(t.Agg)})
	}

	return r
}

// compactLayout returns the active tiers of layout in [widestFirst] order, keeping only the latest
// Before of each (Interval, Agg) whose Interval no other Agg shares. Only then is the dropped tier
// redundant: [tierAt] ranks equal Intervals by Before, so with a second Agg in play the earlier
// Before decides which Agg wins.
func compactLayout(layout []DownsampleTier) []DownsampleTier {
	var out []DownsampleTier

	for _, t := range layout {
		if t.Interval <= 0 {
			continue
		}

		mixed, later := false, false

		for _, o := range layout {
			if o.Interval != t.Interval {
				continue
			}

			mixed = mixed || o.Agg != t.Agg
			later = later || o.Agg == t.Agg && o.Before > t.Before
		}

		if (!later || mixed) && !slices.Contains(out, t) {
			out = append(out, t)
		}
	}

	slices.SortStableFunc(out, widestFirst)

	return out
}

// appliedRollup reads a part's marker; known is false for a part without one.
func appliedRollup(r *block.Rollup) (tiers []DownsampleTier, known bool) {
	if r == nil {
		return nil, false
	}

	tiers = make([]DownsampleTier, 0, len(r.Tiers))
	for _, t := range r.Tiers {
		tiers = append(tiers, DownsampleTier{Before: t.Before, Interval: t.Interval, Agg: signal.Aggregation(t.Agg)})
	}

	return tiers, true
}

// downsamplePending reports whether rewriting p under tiers would roll some of its samples wider
// than they are. A part without a marker falls back to its age.
func downsamplePending(p *part, tiers []DownsampleTier) bool {
	switch {
	case !activeTiers(tiers):
		return false
	case !p.rollupKnown:
		return downsampleApplies(tiers, p.minTime)
	default:
		return rollupPending(p.rollup, tiers, p.minTime, p.maxTime)
	}
}

func activeTiers(tiers []DownsampleTier) bool {
	return slices.ContainsFunc(tiers, func(t DownsampleTier) bool { return t.Interval > 0 })
}

// rollupPending reports whether tiers assign some timestamp in [lo, hi] a wider Interval than
// recorded does. A recorded layout wider than tiers is not pending: rolled data cannot be un-rolled.
func rollupPending(recorded, tiers []DownsampleTier, lo, hi int64) bool {
	return anyBreakpoint(recorded, tiers, lo, hi, func(rec, cur DownsampleTier, okRec, okCur bool) bool {
		return okCur && (!okRec || cur.Interval > rec.Interval)
	})
}

// layoutsDiffer reports whether a and b assign some timestamp in [lo, hi] a different (Interval, Agg).
func layoutsDiffer(a, b []DownsampleTier, lo, hi int64) bool {
	return anyBreakpoint(a, b, lo, hi, func(ta, tb DownsampleTier, okA, okB bool) bool {
		return okA != okB || okA && (ta.Interval != tb.Interval || ta.Agg != tb.Agg)
	})
}

// anyBreakpoint reports whether f holds for the tiers a and b assign some timestamp in [lo, hi]. An
// assignment changes only at a Before, so evaluating at lo and at each Before of either layout inside
// (lo, hi] covers the whole span.
func anyBreakpoint(a, b []DownsampleTier, lo, hi int64, f func(ta, tb DownsampleTier, okA, okB bool) bool) bool {
	at := func(ts int64) bool {
		ta, okA := tierAt(a, ts)
		tb, okB := tierAt(b, ts)

		return f(ta, tb, okA, okB)
	}

	if at(lo) {
		return true
	}

	for _, layout := range [...][]DownsampleTier{a, b} {
		for _, t := range layout {
			if t.Interval > 0 && lo < t.Before && t.Before <= hi && at(t.Before) {
				return true
			}
		}
	}

	return false
}

// tierAt is [pickTier] over tiers in any order: the first a sample at ts qualifies for, in
// [widestFirst] order.
func tierAt(tiers []DownsampleTier, ts int64) (DownsampleTier, bool) {
	var (
		best DownsampleTier
		ok   bool
	)

	for _, t := range tiers {
		if t.Interval <= 0 || ts >= t.Before {
			continue
		}

		if !ok || widestFirst(t, best) < 0 {
			best, ok = t, true
		}
	}

	return best, ok
}

// partOptions is the layout every metric part is written with; rollup nil leaves the marker unknown.
func partOptions(blockRows int, comp compressProfile, rollup *block.Rollup) []block.PartOption {
	opts := []block.PartOption{block.WithSortKey(colTs), block.WithGranuleSize(blockRows)}
	if comp.Algorithm != compress.AlgorithmNone {
		opts = append(opts, block.WithCompression(comp.Algorithm), block.WithCompressionLevel(comp.Level))
	}

	if rollup != nil {
		opts = append(opts, block.WithRollup(*rollup))
	}

	return opts
}

// resolvePolicy is the tiers a merge over the live parts src applies: the tiers those parts record,
// then the policy's tiers that nest with them ([nestedTiers]), which it also returns as dropped when
// they do not. Recorded tiers go first, so a range already rolled keeps its recorded Agg and width
// for data rolled later, even when that data never meets the part that recorded them: otherwise a late
// part rolled alone under a changed Agg would hold representatives that no single marker describes
// once it meets the older part.
func resolvePolicy(src []*part, tiers []DownsampleTier) (applied, dropped []DownsampleTier) {
	kept, dropped := nestedTiers(src, tiers)

	var recorded []DownsampleTier

	for _, p := range src {
		for _, t := range p.rollup {
			if !slices.Contains(recorded, t) {
				recorded = append(recorded, t)
			}
		}
	}

	if len(recorded) == 0 {
		return kept, dropped
	}

	return compactLayout(append(recorded, kept...)), dropped
}

// nestedTiers splits tiers into those whose Interval nests with every Interval a part of src records
// and those that do not. Coarsening a representative into a bucket that does not hold its whole
// bucket cannot be exact, and a marker cannot confine a tier to part of its range, so a non-nesting
// tier is not applied at all until retention drops the last part recording the tier it conflicts
// with. The policy validates only against itself; this is the same check against the history.
func nestedTiers(src []*part, tiers []DownsampleTier) (kept, dropped []DownsampleTier) {
	if !activeTiers(tiers) {
		return tiers, nil
	}

	var recorded []int64

	for _, p := range src {
		for _, t := range p.rollup {
			if !slices.Contains(recorded, t.Interval) {
				recorded = append(recorded, t.Interval)
			}
		}
	}

	nests := func(t DownsampleTier) bool {
		return t.Interval <= 0 || !slices.ContainsFunc(recorded, func(iv int64) bool {
			return t.Interval%iv != 0 && iv%t.Interval != 0
		})
	}

	if !slices.ContainsFunc(tiers, func(t DownsampleTier) bool { return !nests(t) }) {
		return tiers, nil
	}

	for _, t := range tiers {
		if nests(t) {
			kept = append(kept, t)
		} else {
			dropped = append(dropped, t)
		}
	}

	return kept, dropped
}

// rollupPlan is the downsampling a merge applies and the marker its outputs record.
type rollupPlan struct {
	tiers  []DownsampleTier
	marker *block.Rollup
}

// planRollup decides a merge's downsampling. A merge rolls every sample up to the layout it records:
// the tiers its known sources record, which rolled data keeps whatever the policy says now, and the
// current tiers where some source is pending. Where every source already holds that layout the rollup
// only folds representatives sharing a bucket, so tiers stays nil and a ladder merge of rolled parts
// takes the fast path. A lone pending part is rewritten verbatim, its data then holding the layout as
// if applied, when rolling it would change no timestamp, value or weight. A part without a marker is
// raw to that test as to every fold: were timestamps alone enough, raw samples on bucket starts would
// be stamped as representatives of a count they never had.
func (e *Engine) planRollup(ctx context.Context, src []*part, start int64, tiers []DownsampleTier) (rollupPlan, error) {
	if !slices.ContainsFunc(src, func(p *part) bool { return downsamplePending(p, tiers) }) {
		layout := mergeLayout(src, nil)
		plan := rollupPlan{marker: unionRollup(src, layout, false)}

		if !holdsLayout(src, layout) {
			plan.tiers = layout
		}

		return plan, nil
	}

	layout := mergeLayout(src, tiers)

	if len(src) == 1 {
		changes, err := e.rollupChanges(ctx, src[0], start, layout)
		if err != nil {
			return rollupPlan{}, err
		}

		if !changes {
			return rollupPlan{marker: unionRollup(src, layout, true)}, nil
		}
	}

	return rollupPlan{tiers: layout, marker: unionRollup(src, layout, false)}, nil
}

// mergeLayout is the layout a merge of src applying tiers rolls its samples up to: every known
// source's recorded tiers, then tiers. Recorded tiers go first: where a recorded and a current tier
// tie on Interval and Before but differ in Agg, the rolled data keeps its recorded Agg, and [tierAt]
// and [pickTier] keep the first of a tie.
func mergeLayout(src []*part, tiers []DownsampleTier) []DownsampleTier {
	var layout []DownsampleTier

	for _, p := range src {
		layout = append(layout, p.rollup...)
	}

	return compactLayout(append(layout, tiers...))
}

// holdsLayout reports whether rolling src up to layout can only fold representatives sharing a
// bucket: every known source records layout over its own span, and no unknown source holds a sample
// layout would roll.
func holdsLayout(src []*part, layout []DownsampleTier) bool {
	for _, p := range src {
		if !p.rollupKnown {
			if _, ok := tierAt(layout, p.minTime); ok {
				return false
			}

			continue
		}

		if layoutsDiffer(p.rollup, layout, p.minTime, p.maxTime) {
			return false
		}
	}

	return true
}

// unionRollup is the marker of a merge of src that rolled every sample up to layout: per timestamp,
// the widest layout the merged data there has had applied. A source without a marker counts as
// rolled only when layout covers its whole span, or when checked is set because the caller verified
// its samples against layout: past the layout its samples may be legacy representatives the marker
// would call raw, so the result is unknown instead. A merge of known parts is therefore always known.
func unionRollup(src []*part, layout []DownsampleTier, checked bool) *block.Rollup {
	for _, p := range src {
		if !p.rollupKnown && !checked && !coversSpan(layout, p.maxTime) {
			return nil
		}
	}

	m := rollupMarker(layout)

	return &m
}

// coversSpan reports whether tiers roll every sample up to hi.
func coversSpan(tiers []DownsampleTier, hi int64) bool {
	_, ok := tierAt(tiers, hi)

	return ok
}

// rollupChanges reports whether rolling p up under tiers changes any series: its timestamps, values
// or weights. It streams p one series range at a time and stops at the
// first series that changes, so its footprint is a merge source's read window, released before the
// rewrite that follows allocates.
func (e *Engine) rollupChanges(ctx context.Context, p *part, start int64, tiers []DownsampleTier) (bool, error) {
	src := []*part{p}

	var keys mergestream.Keys
	if err := mergeKeys(ctx, src, &keys); err != nil {
		return false, err
	}

	s, err := newPartStream(ctx, p, e.mergeReadWindow)
	if err != nil {
		return false, err
	}

	var (
		streams = []*partStream{s}
		scratch = make([]rangeBuf, 1)
	)

	for keys.Next() {
		m, err := mergeStreamedSeries(ctx, src, streams, scratch, keys.Key(), start)
		if err != nil {
			return false, err
		}

		ts, vals, sf, tags := m.collectTagged(tiers)

		rolledTs, rolledVals, rolledSF, _ := downsampleCovering(ts, vals, sf, tags, tiers)
		if !slices.Equal(rolledTs, ts) {
			return true, nil
		}

		if !sameBits(rolledVals, vals) || !sameWeights(rolledSF, sf, len(ts)) {
			return true, nil
		}
	}

	return false, nil
}

// sameBits compares floats by bit pattern, so a NaN sample equals itself.
func sameBits(a, b []float64) bool {
	return slices.EqualFunc(a, b, func(x, y float64) bool { return math.Float64bits(x) == math.Float64bits(y) })
}

// sameWeights compares two weight vectors of n samples, nil meaning every weight is 1.
func sameWeights(a, b []float64, n int) bool {
	for i := range n {
		wa, wb := 1.0, 1.0
		if a != nil {
			wa = a[i]
		}

		if b != nil {
			wb = b[i]
		}

		if math.Float64bits(wa) != math.Float64bits(wb) {
			return false
		}
	}

	return true
}
