package engine

import (
	"context"

	"github.com/oteldb/storage/internal/buildbudget"
	"github.com/oteldb/storage/internal/memsize"
	"github.com/oteldb/storage/query/fetch"
)

type countGridPlan struct {
	plan          *enginePlan
	heads         []gridSpan
	starts        []int
	identityBytes int64
}

func (cp *countGridPlan) workspace(p *gridPresence) int64 {
	n := memsize.Slice(cp.plan.ids) + memsize.Slice(cp.plan.memActive) + memsize.Slice(cp.plan.series)
	n = buildbudget.Add(n, cp.identityBytes)
	n = buildbudget.Add(n, memsize.Slice(cp.heads)+memsize.Slice(cp.starts)+memsize.Slice(p.words))
	n = buildbudget.Add(n, buildbudget.Mul(int64(p.grid.NumSteps), memsize.Of[gridSpan]()))
	return n
}

func (cp *countGridPlan) groups(groupBy []byte, steps int) ([]uint32, []string, int64) {
	if groupBy == nil {
		return nil, []string{""}, buildbudget.Mul(int64(steps)+1, 8)
	}
	groups := make([]uint32, len(cp.plan.ids))
	names := []string{}
	index := make(map[string]uint32)
	var scratch []byte
	var owned int64
	for i := range cp.plan.series {
		scratch = scratch[:0]
		if v, ok := seriesLabelValue(cp.plan.series[i], groupBy); ok {
			scratch = v.AppendText(scratch)
		}
		group, ok := index[string(scratch)]
		if !ok {
			group = uint32(len(names))
			name := string(scratch)
			names = append(names, name)
			index[name] = group
			owned += int64(len(name))
		}
		groups[i] = group
	}
	n := owned + memsize.Slice(groups) + memsize.Slice(names) + memsize.Map(index)
	n = buildbudget.Add(n, buildbudget.Mul(int64(len(names)), buildbudget.Mul(int64(steps)+1, 8)))
	return groups, names, n
}

func (e *Engine) snapshotCountGrid(ctx context.Context, r fetch.Request, presence *gridPresence, grouped bool) (*countGridPlan, error) {
	e.mu.RLock()
	for !e.head.indexSorted() {
		e.mu.RUnlock()
		e.mu.LockState()
		e.head.ensureIndexSorted()
		e.mu.UnlockState()
		e.mu.RLock()
	}
	defer e.mu.RUnlock()

	ids := e.head.resolve(r.Matchers)
	p := &enginePlan{ids: ids, engine: e, start: r.Start, end: r.End,
		decoded: make(partDecodeCache), memActive: make([]bool, len(ids))}
	e.captureReadView(p)
	cp := &countGridPlan{plan: p, starts: make([]int, len(ids)+1)}
	if grouped {
		p.series = e.getSeriesSlice(len(ids))
	}
	recent := e.recentEnabled()
	if !p.recentCovers(e, r.Start) {
		e.acquireWindowParts(p, r.Start, r.End, false)
	}
	view := e.head.series.View()
	for i, id := range ids {
		if err := ctx.Err(); err != nil {
			return cp, err
		}
		if grouped {
			if s, ok := e.head.series.Get(id); ok {
				p.series[i] = s
			}
			if ref, ok := e.head.series.Ref(id); ok {
				cp.identityBytes = buildbudget.Add(cp.identityBytes, view.MaterializationBytes(int(ref)))
			}
		}
		presence.reset()
		var recentBuf *sampleBuf
		if recent {
			recentBuf = e.recent[id]
		}
		if err := presence.addBuffers(ctx, r, [3]*sampleBuf{e.head.samples[id], e.flushing[id], recentBuf}); err != nil {
			return cp, err
		}
		p.memActive[i] = presence.complete()
		if !p.memActive[i] {
			cp.heads = presence.spans(cp.heads)
		}
		cp.starts[i+1] = len(cp.heads)
	}
	return cp, nil
}

func (presence *gridPresence) addBuffers(ctx context.Context, r fetch.Request, buffers [3]*sampleBuf) error {
	for _, buf := range buffers {
		if buf == nil || presence.complete() {
			continue
		}
		for j, ts := range buf.ts {
			if j%1024 == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			if ts >= r.Start && ts <= r.End {
				presence.sample(ts)
			}
			if presence.complete() {
				break
			}
		}
	}
	return nil
}
