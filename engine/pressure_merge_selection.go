package engine

import (
	"cmp"
	"slices"

	"github.com/oteldb/storage/internal/buildbudget"
)

func (e *Engine) pressureMergePlan(
	cohorts []mergeCohort, readable []*part, opts MergeOptions, capBytes int64, idle int,
	turn uint64, maxInputs int, anchor *part,
) boundedMergeSelection {
	ordinary := cohortRunPlanLimited(cohorts, readable, opts, capBytes, idle, turn, maxInputs)
	if !e.twoPressureGenerations() {
		return ordinary
	}
	if anchor == nil && (len(ordinary.parts) == 0 || e.pressureMergeSelectionAllowed(ordinary.parts)) {
		ordinary.orderAnchor = newestMergePart(ordinary.parts)
		return ordinary
	}
	search := pressureMergeSearch{engine: e, readable: readable, opts: opts, capBytes: capBytes,
		idle: idle, newest: newestSample(readable), maxInputs: maxInputs, anchor: anchor,
		remaining: maxMergeParts * maxMergeParts}
	for i := range cohorts {
		c := &cohorts[(turn+uint64(i))%uint64(len(cohorts))]
		if selected := search.cohort(c); len(selected.parts) > 0 {
			return selected
		}
		if search.remaining == 0 {
			break
		}
	}
	if anchor != nil {
		// Memory repricing must not silently switch back to the vetoed older seed.
		return boundedMergeSelection{capBytes: ordinary.capBytes, orderAnchor: anchor}
	}
	// Preserve the normal order deferral when pending generations prevent every candidate.
	return ordinary
}

type pressureMergeSearch struct {
	engine           *Engine
	readable         []*part
	opts             MergeOptions
	capBytes, newest int64
	idle, maxInputs  int
	remaining        int
	anchor           *part
}

func (s *pressureMergeSearch) cohort(c *mergeCohort) boundedMergeSelection {
	if s.anchor != nil && !slices.Contains(c.parts, s.anchor) {
		return boundedMergeSelection{}
	}
	opts := s.opts
	var dropped []DownsampleTier
	opts.Downsample, dropped = resolvePolicy(c.history, s.readable, s.opts.Downsample)
	for _, level := range mergeLadder {
		_, groups := partitionGroupsBefore(c.parts, level, s.newest)
		for _, group := range slices.Backward(groups) {
			if selected := s.group(group, c.parts, opts, dropped); len(selected.parts) > 0 {
				return selected
			}
			if s.remaining == 0 {
				return boundedMergeSelection{}
			}
		}
	}
	return boundedMergeSelection{}
}

func (s *pressureMergeSearch) group(group, own []*part, opts MergeOptions, dropped []DownsampleTier) boundedMergeSelection {
	group = slices.Clone(group)
	slices.SortFunc(group, func(a, b *part) int { return cmp.Compare(a.prefix, b.prefix) })
	for end := len(group); end >= minMergeParts; end-- {
		if s.anchor != nil && group[end-1] != s.anchor {
			continue
		}
		for n := min(s.maxInputs, end); n >= minMergeParts; n-- {
			// Bound candidate count independently of the backlog size.
			if s.remaining == 0 {
				return boundedMergeSelection{}
			}
			s.remaining--
			run := group[end-n : end]
			if !pressureMergeSuffixQualifies(run, s.capBytes, s.idle) {
				continue
			}
			selected, tiers, closure := closeRollupSelection(run, own, s.readable, opts, s.maxInputs)
			if len(selected) < minMergeParts || !slices.Contains(selected, group[end-1]) || !s.engine.pressureMergeSelectionAllowed(selected) {
				continue
			}
			outputCap := s.capBytes
			if closure {
				outputCap = 0
			}
			return boundedMergeSelection{parts: slices.Clone(selected), tiers: tiers, conflicting: dropped,
				capBytes: outputCap, orderAnchor: newestMergePart(selected)}
		}
	}
	return boundedMergeSelection{}
}

func newestMergePart(parts []*part) (newest *part) {
	for _, p := range parts {
		if newest == nil || p.prefix > newest.prefix {
			newest = p
		}
	}
	return newest
}

func (e *Engine) pressureMergeSelectionAllowed(parts []*part) bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.pressureMergeOrderLocked(parts, nil) == nil
}

func pressureMergeSuffixQualifies(parts []*part, capBytes int64, idle int) bool {
	var total, largest int64
	smallest := parts[0].sizeBytes()
	for _, p := range parts {
		if sealed(p, capBytes) {
			return false
		}
		bytes := p.sizeBytes()
		total = buildbudget.Add(total, bytes)
		smallest, largest = min(smallest, bytes), max(largest, bytes)
	}
	if capBytes > 0 && total > capBytes {
		return false
	}
	if idle >= mergeIdleRounds || total <= smallRunBytes {
		return true
	}
	return buildbudget.Mul(smallest, int64(len(parts))) >= largest && float64(total)/float64(largest) >= minMergeMultiplier
}
