package engine

import (
	"slices"

	"github.com/oteldb/storage/block"
)

func closeRollupSelection(seed, own, readable []*part, opts MergeOptions, maxInputs int) ([]*part, []DownsampleTier, bool) {
	tiers := opts.Downsample
	if !slices.ContainsFunc(seed, func(p *part) bool { return downsamplePending(p, tiers) }) {
		return finishRollupSelection(seed, readable, tiers, opts, false)
	}
	selected := slices.Clone(seed)
	for len(selected) < maxInputs {
		n := len(selected)
		for _, candidate := range own {
			if slices.Contains(selected, candidate) || !rollupOverlaps(selected, candidate, tiers) {
				continue
			}
			selected = append(selected, candidate)
			if len(selected) == maxInputs {
				break
			}
		}
		if len(selected) == n {
			break
		}
	}
	// Keep source order: the freshest raw copy must win before any samples are rolled.
	selected = slices.DeleteFunc(slices.Clone(own), func(p *part) bool { return !slices.Contains(selected, p) })
	closed := closedRollupTiers(selected, readable, tiers)
	inherited := mergeLayout(selected, closed)
	if !slices.Equal(inherited, closedRollupTiers(selected, readable, inherited)) {
		recorded := mergeLayout(selected, nil)
		unchanged := slices.DeleteFunc(slices.Clone(selected), func(p *part) bool { return !holdsLayout([]*part{p}, recorded) })
		if len(unchanged) >= minMergeParts {
			return finishRollupSelection(unchanged, readable, nil, opts, true)
		}
		selected = slices.DeleteFunc(slices.Clone(own), func(p *part) bool {
			return downsampleApplies(p.rollup, p.minTime) || !rollupOverlaps(selected, p, tiers)
		})
		selected = selected[:min(len(selected), maxInputs)]
		closed = closedRollupTiers(selected, readable, tiers)
	}
	return finishRollupSelection(selected, readable, closed, opts, true)
}

func finishRollupSelection(
	selected, readable []*part, tiers []DownsampleTier, opts MergeOptions, closure bool,
) ([]*part, []DownsampleTier, bool) {
	if len(selected) == 0 {
		return nil, nil, closure
	}
	layout := mergeLayout(selected, tiers)
	if !holdsLayout(selected, layout) && !slices.Equal(layout, closedRollupTiers(selected, readable, layout)) {
		return nil, nil, closure
	}
	if len(selected) == 1 {
		opts.Downsample = tiers
		if !forcedRewrite(selected[0], opts) && !splitsOutput(selected, minInt64) {
			return nil, nil, closure
		}
	}
	return selected, tiers, closure
}

func closedRollupTiers(selected, readable []*part, tiers []DownsampleTier) []DownsampleTier {
	closed := slices.Clone(tiers)
	for i, tier := range tiers {
		if tier.Interval <= 0 {
			continue
		}
		for _, other := range readable {
			if selectedPart(selected, other) {
				continue
			}
			for _, p := range selected {
				lo, hi, ok := rollupSpan(p, tier)
				if !ok || other.maxTime < lo || other.minTime > hi {
					continue
				}
				closed[i].Before = min(closed[i].Before, rollupBucketStart(max(lo, other.minTime), tier.Interval))
			}
		}
	}
	lo, _ := spanOf(selected)
	return compactLayout(slices.DeleteFunc(closed, func(t DownsampleTier) bool { return t.Before <= lo }))
}

func selectedPart(selected []*part, p *part) bool {
	return slices.ContainsFunc(selected, func(s *part) bool { return s == p || s.prefix != "" && s.prefix == p.prefix })
}

func rollupOverlaps(selected []*part, candidate *part, tiers []DownsampleTier) bool {
	for _, p := range selected {
		for _, t := range tiers {
			lo, hi, ok := rollupSpan(p, t)
			if ok && candidate.minTime <= hi && candidate.maxTime >= lo {
				return true
			}
		}
	}
	return false
}

func rollupSpan(p *part, tier DownsampleTier) (int64, int64, bool) {
	if tier.Interval <= 0 || p.minTime >= tier.Before {
		return 0, 0, false
	}
	last := min(p.maxTime, tier.Before-1)
	lo := rollupBucketStart(p.minTime, tier.Interval)
	offset := last % tier.Interval
	if offset < 0 {
		offset += tier.Interval
	}
	tail := tier.Interval - 1 - offset
	hi := maxInt64
	if last <= maxInt64-tail {
		hi = last + tail
	}
	return lo, hi, true
}

func rollupBucketStart(ts, interval int64) int64 {
	offset := ts % interval
	if offset < 0 {
		offset += interval
	}
	if ts < minInt64+offset {
		return minInt64
	}
	return ts - offset
}

func rollupClosureGuard(selected, readable []*part, marker *block.Rollup, e *Engine) func() error {
	compatible := rollupGuard(readable, marker, e)
	tiers, _ := appliedRollup(marker)
	if !activeTiers(tiers) {
		return nil
	}
	return func() error {
		if compatible != nil {
			if err := compatible(); err != nil {
				return err
			}
		}
		changed := func(p *part) bool {
			known := slices.ContainsFunc(readable, func(previous *part) bool {
				return previous.prefix == p.prefix && previous.minTime == p.minTime && previous.maxTime == p.maxTime
			})
			return !known && rollupOverlaps(selected, p, tiers)
		}
		if slices.ContainsFunc(e.parts, changed) || slices.ContainsFunc(e.unopenedLayoutsLocked(), changed) {
			return errRollupConflict
		}
		for i := range e.foreign {
			ent := &e.foreign[i]
			if changed(&part{prefix: ent.Prefix, minTime: ent.MinTime, maxTime: ent.MaxTime}) {
				return errRollupConflict
			}
		}
		return nil
	}
}
