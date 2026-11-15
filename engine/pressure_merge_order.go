package engine

import (
	"cmp"
	"context"
	"slices"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/signal"
)

var errPressureMergeOrder = errors.New("merge deferred: newer overlapping pressure data must be included")

func partIDBounds(p *part) (first, last signal.SeriesID, known bool) {
	if index := p.index.paged; index != nil {
		return index.first, index.last, index.bounded
	}
	if len(p.index.ids) == 0 {
		return first, last, false
	}
	return p.index.ids[0], p.index.ids[len(p.index.ids)-1], true
}

func partIDsDisjoint(a, b *part) bool {
	alo, ahi, aok := partIDBounds(a)
	blo, bhi, bok := partIDBounds(b)
	return aok && bok && (ahi.Compare(blo) < 0 || bhi.Compare(alo) < 0)
}

// pressureMergeOrderLocked requires the newest overlapping sources, without holding a lease across encoding.
func (e *Engine) pressureMergeOrderLocked(selected, outputs []*part) error {
	if !e.twoPressureGenerations() || len(selected) == 0 {
		return nil
	}
	var err error
	outputs, err = e.pressureMergeOutputs(selected, outputs)
	if err != nil {
		return err
	}
	oldest := selected[0].prefix
	for _, p := range selected {
		oldest = min(oldest, p.prefix)
	}
	for _, p := range e.readablePartsLocked() {
		if p.prefix <= oldest || slices.Contains(selected, p) {
			continue
		}
		if mergeExcludedOverlap(selected, outputs, p) {
			return errPressureMergeOrder
		}
	}
	for i := range e.foreign {
		entry := &e.foreign[i]
		if entry.Prefix <= oldest {
			continue
		}
		if _, ok := e.foreignParts[entry.Prefix]; ok {
			continue
		}
		if mergeExcludedOverlap(selected, outputs, &part{prefix: entry.Prefix, minTime: entry.MinTime, maxTime: entry.MaxTime}) {
			return errPressureMergeOrder
		}
	}
	for _, g := range e.pressureGenerations {
		lo := minInt64
		if g.head.bufferedKnown {
			lo = g.head.bufferedMin
		}
		pending := &part{prefix: g.prefixFloor, minTime: lo, maxTime: g.head.newest}
		// Representatives combine with raw rows, so prefix order cannot replace rollup closure.
		if slices.ContainsFunc(outputs, func(p *part) bool { return rollupOverlaps(selected, pending, p.rollup) }) {
			return errPressureMergeOrder
		}
		if mergeExcludedOverlap(selected, outputs, pending) {
			return errPressureMergeOrder
		}
	}
	return nil
}

func mergeExcludedOverlap(selected, outputs []*part, other *part) bool {
	if len(outputs) > 0 && !slices.ContainsFunc(outputs, func(p *part) bool { return p.prefix > other.prefix }) {
		return false
	}

	for _, source := range selected {
		if partIDsDisjoint(source, other) {
			continue
		}
		if source.minTime <= other.maxTime && other.minTime <= source.maxTime {
			return true
		}
		for _, output := range outputs {
			if output.minTime <= other.maxTime && other.minTime <= output.maxTime {
				return true
			}
		}
	}
	return false
}

func (e *Engine) pressureMergeGuard(selected, outputs []*part, prior func() error) func() error {
	if !e.twoPressureGenerations() {
		return prior
	}
	return func() error {
		if prior != nil {
			if err := prior(); err != nil {
				return err
			}
		}
		return e.pressureMergeOrderLocked(selected, outputs)
	}
}

func (e *Engine) sortPressureParts(parts []*part) {
	if e.twoPressureGenerations() {
		slices.SortFunc(parts, func(a, b *part) int { return cmp.Compare(a.prefix, b.prefix) })
	}
}

func (e *Engine) admitPressureMerge(ctx context.Context, selected []*part, background bool) (bool, error) {
	if !e.twoPressureGenerations() {
		return false, nil
	}
	e.mu.RLock()
	err := e.pressureMergeOrderLocked(selected, nil)
	e.mu.RUnlock()
	if err == nil {
		return false, nil
	}
	e.mergeDeferred.Store(true)
	e.cfg.Obs.Merge.OrderDeferred(ctx, metricSignal)
	if background {
		err = nil
	}
	return true, err
}

func (e *Engine) pressureMergeFailure(
	ctx context.Context, err error, background bool, dropped, selected int, bytesIn, bytesOut int64,
) (mergeResult, error) {
	if !errors.Is(err, errPressureMergeOrder) {
		return mergeResult{parts: dropped + selected, bytesIn: bytesIn, bytesOut: bytesOut}, err
	}
	e.mergeDeferred.Store(true)
	e.cfg.Obs.Merge.OrderDeferred(ctx, metricSignal)
	if background {
		err = nil
	}
	return mergeResult{deferred: true, parts: dropped}, err
}
