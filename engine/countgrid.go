package engine

import (
	"context"
	"slices"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/otel/attribute"

	"github.com/oteldb/storage/internal/buildbudget"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

var _ fetch.GridCounter = (*Engine)(nil)

// CountGrid counts distinct present series at every window, optionally grouped by one label.
// It reads timestamps only when head or part bounds cannot establish presence.
func (e *Engine) CountGrid(ctx context.Context, r fetch.Request, grid fetch.WindowGrid, groupBy []byte) (map[string][]int, error) {
	ctx, span := e.startAggregateSpan(e.cfg.Obs.Base(ctx), "engine.countGrid")
	defer span.End()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	presence, err := newGridPresence(grid)
	if err != nil {
		return nil, err
	}
	cp, err := e.snapshotCountGrid(ctx, r, presence, groupBy != nil)
	if cp != nil {
		defer cp.plan.releaseParts()
	}
	if err != nil {
		return nil, err
	}
	p := cp.plan
	span.SetAttributes(attribute.Int("storage.series_matched", len(p.ids)),
		attribute.Int("storage.parts_scanned", len(p.liveParts)), attribute.Int("storage.grid_steps", grid.NumSteps))
	if len(p.ids) == 0 {
		return map[string][]int{}, nil
	}

	for _, pt := range p.liveParts {
		if !slices.Contains(p.memActive, false) {
			break
		}
		proof := presence.covered(pt.minTime, pt.maxTime)
		if pt.minTime >= r.Start && pt.maxTime <= r.End && proof.lo == 0 && int(proof.hi) == grid.NumSteps {
			if err := pt.index.intersectMark(ctx, p.ids, p.memActive); err != nil {
				return nil, err
			}
		}
	}

	groupOf, names, groupBytes := cp.groups(groupBy, grid.NumSteps)
	estimate := buildbudget.Add(cp.workspace(presence), groupBytes)
	raw := slices.Contains(p.memActive, false)
	if raw {
		for _, pt := range p.liveParts {
			p.prepareTimestampReader(pt)
		}
		bytes, bounded := p.countStreamingEstimate()
		if !bounded {
			var err error
			bytes, err = p.decodeEstimate(ctx, colNeed{})
			if err != nil {
				return nil, err
			}
		}
		estimate = buildbudget.Add(estimate, bytes)
	}
	if err := p.reserveDecodeBudget(ctx, r, estimate); err != nil {
		return nil, err
	}

	return cp.collect(ctx, presence, groupOf, names, groupBy != nil)
}

func (cp *countGridPlan) collect(
	ctx context.Context, presence *gridPresence, groupOf []uint32, names []string, grouped bool,
) (map[string][]int, error) {
	p := cp.plan
	grid := presence.grid
	out := make(map[string][]int)
	var spans []gridSpan
	for i, id := range p.ids {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		presence.reset()
		if p.memActive[i] {
			presence.add(gridSpan{0, uint32(grid.NumSteps)})
		} else {
			for _, s := range cp.heads[cp.starts[i]:cp.starts[i+1]] {
				presence.add(s)
			}
			if err := p.gridSeries(ctx, id, presence); err != nil {
				return nil, err
			}
		}
		p.releaseSeriesPins()
		if presence.count == 0 {
			continue
		}
		name := names[0]
		if grouped {
			name = names[groupOf[i]]
		}
		diff := out[name]
		if diff == nil {
			diff = make([]int, grid.NumSteps+1)
			out[name] = diff
		}
		spans = presence.spans(spans[:0])
		for _, s := range spans {
			diff[s.lo]++
			diff[s.hi]--
		}
	}
	for name, diff := range out {
		for i := 1; i < grid.NumSteps; i++ {
			diff[i] += diff[i-1]
		}
		out[name] = diff[:grid.NumSteps]
	}
	return out, nil
}

func (p *enginePlan) gridSeries(ctx context.Context, id signal.SeriesID, presence *gridPresence) error {
	if err := p.proveGridSeries(ctx, id, presence); err != nil {
		return err
	}
	for _, pt := range p.liveParts {
		if presence.complete() {
			return nil
		}
		rng, found, err := p.lookupRange(ctx, pt, id)
		if err != nil {
			return err
		}
		if !found {
			continue
		}
		if err := p.gridPart(ctx, pt, rng, presence); err != nil {
			return err
		}
	}
	return nil
}

func (p *enginePlan) proveGridSeries(ctx context.Context, id signal.SeriesID, presence *gridPresence) error {
	// Existence proofs precede timestamp reads, regardless of part publication order.
	for _, pt := range p.liveParts {
		if presence.complete() {
			return nil
		}
		if pt.minTime < p.start || pt.maxTime > p.end {
			continue
		}
		proof := presence.covered(pt.minTime, pt.maxTime)
		if proof.lo == proof.hi {
			continue
		}
		_, found, err := p.lookupRange(ctx, pt, id)
		if err != nil {
			return err
		}
		if found {
			presence.add(proof)
		}
	}
	return nil
}

func (p *enginePlan) gridPart(ctx context.Context, pt *part, rng rowRange, presence *gridPresence) error {
	if desc, ok := pt.reader.ColumnDescByName(colTs); ok && desc.Const {
		if ts := desc.ConstInt64; ts >= p.start && ts <= p.end {
			presence.sample(ts)
		}
		return nil
	}
	if reader := p.blockReaders[pt]; reader != nil {
		return reader.gridTimestamps(ctx, rng, p.start, p.end, presence)
	}
	dp := p.decoded[pt]
	if dp == nil {
		ranges, err := p.rangesFor(ctx, pt)
		if err != nil {
			return err
		}
		dp, err = p.engine.decodeOf(ctx, pt, colNeed{}, ranges)
		if err != nil {
			return err
		}
		p.decoded[pt] = dp
	}
	if rng.start < 0 || rng.start > rng.end || rng.end > len(dp.ts) {
		return errors.New("count grid timestamp range outside column")
	}
	for _, ts := range dp.ts[rng.start:rng.end] {
		if ts >= p.start && ts <= p.end {
			presence.sample(ts)
		}
	}
	return nil
}

func (r *seriesBlockReader) gridTimestamps(ctx context.Context, rng rowRange, start, end int64, presence *gridPresence) error {
	if r.countStreaming {
		return r.streamGridTimestamps(ctx, rng, start, end, presence)
	}
	granules := r.granules(ctx)
	for b, last := rng.start/r.blockRows, (rng.end-1)/r.blockRows; b <= last; b++ {
		if presence.complete() {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if !blockInWindow(granules, b, start, end) {
			continue
		}
		ts, err := r.tsBlock(ctx, b)
		if err != nil {
			return err
		}
		base := b * r.blockRows
		lo, hi := max(rng.start, base)-base, min(rng.end-base, len(ts))
		if lo < 0 || lo > hi {
			return errors.New("count grid block range outside column")
		}
		for _, ts := range ts[lo:hi] {
			if ts >= start && ts <= end {
				presence.sample(ts)
			}
		}
		r.releaseSeriesPins()
	}
	return nil
}
