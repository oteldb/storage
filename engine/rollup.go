package engine

import (
	"cmp"
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

// resolvePolicy is the tiers a merge over one cohort applies: the tiers history, the cohort's
// readable parts, records, then the policy's tiers compatible with every readable part
// ([compatibleTiers]), which it also returns as dropped when they are not. Recorded tiers keep
// applying with their recorded Agg, so data landing later in a range already rolled is rolled like
// the rest of it, even when it never meets the part that recorded the range.
func resolvePolicy(history, readable []*part, tiers []DownsampleTier) (applied, dropped []DownsampleTier) {
	kept, dropped := compatibleTiers(readable, tiers)

	var recorded []DownsampleTier

	for _, p := range history {
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

// compatibleTiers splits tiers into those a merge over src can apply exactly and those it cannot:
// a tier whose Interval does not nest with one a part of src records, or whose Agg differs from one a
// part records. The first would coarsen a representative into a bucket that does not hold its whole
// bucket. The second would re-aggregate representatives by an Agg they were not rolled with, whatever
// the tier's width (a 1h Count over 1m Sum representatives counts minutes, not samples), and a marker
// records one Agg per range. Either tier is not applied at all until retention drops the last part
// recording the tier it conflicts with, since a marker cannot confine a tier to part of its range.
// The policy validates only against itself; this is the same check against the history.
func compatibleTiers(src []*part, tiers []DownsampleTier) (kept, dropped []DownsampleTier) {
	if !activeTiers(tiers) {
		return tiers, nil
	}

	var recorded []DownsampleTier
	for _, p := range src {
		recorded = append(recorded, p.rollup...)
	}

	fits := func(t DownsampleTier) bool { return t.Interval <= 0 || fitsLayout(recorded, t) }

	if !slices.ContainsFunc(tiers, func(t DownsampleTier) bool { return !fits(t) }) {
		return tiers, nil
	}

	for _, t := range tiers {
		if fits(t) {
			kept = append(kept, t)
		} else {
			dropped = append(dropped, t)
		}
	}

	return kept, dropped
}

// tiersNest reports whether two active tiers can share one layout: they roll up by one Agg and one
// Interval divides the other. It is the one definition of compatible layouts: the policy is checked
// against the history by it ([compatibleTiers]), cohorts are formed by it ([mergeCohorts]), and a
// commit's rebase guard checks adopted parts by it.
func tiersNest(a, b DownsampleTier) bool {
	return a.Agg == b.Agg && (a.Interval%b.Interval == 0 || b.Interval%a.Interval == 0)
}

// fitsLayout reports whether active tier t nests with every active tier of layout.
func fitsLayout(layout []DownsampleTier, t DownsampleTier) bool {
	return !slices.ContainsFunc(layout, func(o DownsampleTier) bool { return o.Interval > 0 && !tiersNest(o, t) })
}

// layoutsNest reports whether every active tier of b nests with every one of a and of b itself.
func layoutsNest(a, b []DownsampleTier) bool {
	for _, t := range b {
		if t.Interval > 0 && (!fitsLayout(a, t) || !fitsLayout(b, t)) {
			return false
		}
	}

	return true
}

// mergeCohort is a set of this engine's parts a merge may combine: those whose recorded layouts nest
// with each other ([tiersNest]), with history every readable part in the cohort, adopted ones
// included, and layout the tiers they record.
type mergeCohort struct {
	layout  []DownsampleTier
	parts   []*part
	history []*part
}

// mergeCohorts partitions own, the parts this engine may merge, into sets whose recorded layouts nest.
// One writer never records a layout that does not nest with a readable part's ([compatibleTiers], and
// the commit guard in [Engine.merge]), so there is normally one cohort. Parts that disagree anyway,
// written by writers running different policies for one tenant, cannot be merged: a second Agg
// would re-aggregate one side by the other's, and a grid that does not nest (7m beside 1h) would
// coarsen a 7m bucket straddling an hour into one hour. Each cohort merges only with itself, every
// output records one nesting layout, and every cohort is still compacted.
//
// Marked readable parts are assigned oldest first (by minTime, then prefix), each to the first cohort
// it nests with, so the assignment depends on the parts alone. Raw and unmarked parts join the
// primary cohort, the oldest part's: a late raw sample then rolls like the data that was rolled
// first, whichever writer wrote it. The primary cohort comes first, the rest in the order formed.
func mergeCohorts(own, readable []*part) []mergeCohort {
	marked := make([]*part, 0, len(readable))

	for _, p := range readable {
		if activeTiers(p.rollup) {
			marked = append(marked, p)
		}
	}

	slices.SortFunc(marked, func(a, b *part) int {
		if c := cmp.Compare(a.minTime, b.minTime); c != 0 {
			return c
		}

		return cmp.Compare(a.prefix, b.prefix)
	})

	var cohorts []mergeCohort

	of := make(map[*part]int, len(marked))

	for _, p := range marked {
		i := slices.IndexFunc(cohorts, func(c mergeCohort) bool { return layoutsNest(c.layout, p.rollup) })
		if i < 0 {
			i = len(cohorts)
			cohorts = append(cohorts, mergeCohort{})
		}

		c := &cohorts[i]
		for _, t := range p.rollup {
			if t.Interval > 0 && !slices.Contains(c.layout, t) {
				c.layout = append(c.layout, t)
			}
		}

		c.history = append(c.history, p)
		of[p] = i
	}

	if len(cohorts) == 0 {
		cohorts = append(cohorts, mergeCohort{})
	}

	for _, p := range own {
		i, ok := of[p]
		if !ok {
			i = 0
		}

		cohorts[i].parts = append(cohorts[i].parts, p)
	}

	return slices.DeleteFunc(cohorts, func(c mergeCohort) bool { return len(c.parts) == 0 })
}

// cohortRun is the selection one merge makes: the first cohort, from start round-robin, whose own
// selection is not empty, with the tiers it applies and the policy tiers it drops. Round-robin keeps
// a busy cohort from starving the others. The ladder's still-filling bucket is the one holding the
// newest readable sample, adopted parts included: a writer that stopped ingesting while a rival goes
// on would otherwise hold its last bucket open for ever.
func cohortRun(
	cohorts []mergeCohort, readable []*part, opts MergeOptions, capBytes int64, idle int, start uint64,
) (selected []*part, tiers, dropped []DownsampleTier) {
	newest := newestSample(readable)

	for i := range cohorts {
		c := &cohorts[(start+uint64(i))%uint64(len(cohorts))]

		o := opts
		o.Downsample, dropped = resolvePolicy(c.history, readable, opts.Downsample)

		if sel := selectMergePartsBefore(c.parts, o, capBytes, idle, newest); len(sel) > 0 {
			return sel, o.Downsample, dropped
		}
	}

	return nil, nil, dropped
}

// rollupPlan is the downsampling a merge applies and the marker its outputs record.
type rollupPlan struct {
	tiers  []DownsampleTier
	marker *block.Rollup
}

// rolledBefore is the latest cutoff below which the merge's output holds representatives, minInt64
// when it rolls nothing.
func (p rollupPlan) rolledBefore() int64 {
	cutoff := minInt64

	for _, t := range p.tiers {
		if t.Interval > 0 {
			cutoff = max(cutoff, t.Before)
		}
	}

	if p.marker != nil {
		for _, t := range p.marker.Tiers {
			cutoff = max(cutoff, t.Before)
		}
	}

	return cutoff
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
// source's recorded tiers, then tiers. The sources and tiers share one Agg ([mergeCohorts],
// [resolvePolicy]).
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

		ts, vals, sf, tags := m.collectTagged()

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
