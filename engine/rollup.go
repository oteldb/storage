package engine

import (
	"context"
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

// rollupPlan is the downsampling a merge applies and the marker its outputs record.
type rollupPlan struct {
	tiers  []DownsampleTier
	marker *block.Rollup
}

// planRollup decides a merge's downsampling: tiers when some source is pending, else none, so
// representatives already at least as wide are not re-rolled. A lone pending part is rolled only if
// that moves some sample; one already at its bucket starts is rewritten verbatim, since re-rolling is
// not idempotent for every aggregation, and its data then holds the tiers as if applied.
func (e *Engine) planRollup(ctx context.Context, src []*part, start int64, tiers []DownsampleTier) (rollupPlan, error) {
	if !slices.ContainsFunc(src, func(p *part) bool { return downsamplePending(p, tiers) }) {
		return rollupPlan{marker: unionRollup(src, nil, false)}, nil
	}

	if len(src) == 1 {
		moves, err := e.rollupMoves(ctx, src[0], start, tiers)
		if err != nil {
			return rollupPlan{}, err
		}

		if !moves {
			return rollupPlan{marker: unionRollup(src, tiers, true)}, nil
		}
	}

	return rollupPlan{tiers: tiers, marker: unionRollup(src, tiers, false)}, nil
}

// unionRollup is the marker of a merge of src that applied tiers: per timestamp, the widest layout
// the merged data there has had applied. It is nil (unknown) when that cannot be expressed as one
// layout over every source's span: sources disagree on a range they share, or one source's rolled
// range reaches into another's raw data. A source without a marker counts as having had tiers
// applied only when they cover its whole span, or when checked is set because the caller verified
// its samples against tiers; otherwise the result is unknown too.
func unionRollup(src []*part, tiers []DownsampleTier, checked bool) *block.Rollup {
	union := slices.Clone(tiers)

	for _, p := range src {
		union = append(union, p.rollup...)
	}

	union = compactLayout(union)

	for _, p := range src {
		if !p.rollupKnown && !checked && !coversSpan(tiers, p.maxTime) {
			return nil
		}

		held := append(slices.Clone(tiers), p.rollup...)
		if layoutsDiffer(held, union, p.minTime, p.maxTime) {
			return nil
		}
	}

	m := rollupMarker(union)

	return &m
}

// coversSpan reports whether tiers roll every sample up to hi.
func coversSpan(tiers []DownsampleTier, hi int64) bool {
	_, ok := tierAt(tiers, hi)

	return ok
}

// rollupMoves reports whether rolling p up under tiers changes any series' timestamps. It streams
// p one series range at a time and stops at the first series that changes, so its footprint is a
// merge source's read window, released before the rewrite that follows allocates.
func (e *Engine) rollupMoves(ctx context.Context, p *part, start int64, tiers []DownsampleTier) (bool, error) {
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
		tsBuf   []int64
		valBuf  []float64
	)

	for keys.Next() {
		m, err := mergeStreamedSeries(ctx, src, streams, scratch, keys.Key(), start)
		if err != nil {
			return false, err
		}

		var sf []float64

		tsBuf, valBuf, sf = m.collect(tsBuf, valBuf)

		if rolled, _, _ := downsample(tsBuf, valBuf, sf, tiers); !slices.Equal(rolled, tsBuf) {
			return true, nil
		}
	}

	return false, nil
}
