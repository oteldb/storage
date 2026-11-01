package engine

import "github.com/oteldb/storage/signal"

type memorySamples struct {
	ts         []int64
	values, sf []float64
}

func (p *enginePlan) forEachMemorySeries(id signal.SeriesID, visit func(memorySamples)) {
	var roots [3]*memoryDirectoryNode
	if view := p.readView; view != nil {
		roots = [3]*memoryDirectoryNode{view.recent, view.frozen, view.active}
	}
	for i, batch := range p.memBatches(id) {
		if batch != nil {
			visit(memorySamples{ts: batch.Timestamps, values: batch.Values, sf: batch.ScaleFactors})
		}
		start := p.start
		if i == 0 && p.readView != nil {
			start = max(start, p.readView.recentMin)
		}
		if start > p.end {
			continue
		}
		roots[i].walk(start, p.end, func(run *memoryRun) bool {
			rng, found := run.lookup(id)
			if found {
				ts := run.ts[rng.start:rng.end]
				lo, hi := lowerBound(ts, start), upperBound(ts, p.end)
				if lo == hi {
					return true
				}
				rng.start, rng.end = rng.start+lo, rng.start+hi
				var sf []float64
				if run.sf != nil {
					sf = run.sf[rng.start:rng.end]
				}
				samples := memorySamples{ts: run.ts[rng.start:rng.end], values: run.values[rng.start:rng.end], sf: sf}
				visit(samples)
			}
			return true
		})
	}
}

func (p *enginePlan) forEachMemoryRun(visit func(*memoryRun) bool) {
	if view := p.readView; view != nil {
		for _, root := range [3]*memoryDirectoryNode{view.recent, view.frozen, view.active} {
			if !root.walk(p.start, p.end, visit) {
				return
			}
		}
	}
}

// The registry/state guard keeps parts and directory installation coherent during capture.
func (e *Engine) captureReadView(p *enginePlan) {
	e.viewMu.Lock()
	if view := e.readView; view != nil {
		view.acquire()
		p.readView = view
	}
	e.viewMu.Unlock()
}

func (p *enginePlan) markMemoryRunExistence() {
	if p.readView == nil {
		return
	}
	for i, id := range p.ids {
		if p.memActive[i] {
			continue
		}
		p.memActive[i] = p.memoryRunPresent(id)
	}
}

func (p *enginePlan) memoryRunPresent(id signal.SeriesID) (found bool) {
	p.forEachMemorySeries(id, func(samples memorySamples) { found = found || len(samples.ts) > 0 })
	return found
}

func (cp *countGridPlan) includeMemoryRuns(presence *gridPresence) {
	if cp.plan.readView == nil {
		return
	}
	heads, starts := []gridSpan{}, make([]int, len(cp.plan.ids)+1)
	for i, id := range cp.plan.ids {
		presence.reset()
		if cp.plan.memActive[i] {
			presence.add(gridSpan{0, uint32(presence.grid.NumSteps)})
		} else {
			for _, span := range cp.heads[cp.starts[i]:cp.starts[i+1]] {
				presence.add(span)
			}
			cp.plan.forEachMemorySeries(id, func(samples memorySamples) {
				for _, ts := range samples.ts {
					if ts >= cp.plan.start && ts <= cp.plan.end {
						presence.sample(ts)
					}
				}
			})
		}
		cp.plan.memActive[i] = presence.complete()
		if !presence.complete() {
			heads = presence.spans(heads)
		}
		starts[i+1] = len(heads)
	}
	cp.heads, cp.starts = heads, starts
}
