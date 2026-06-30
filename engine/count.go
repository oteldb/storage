package engine

import (
	"context"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// Count returns the number of series matching r.Matchers that have at least one sample in
// [r.Start, r.End]. It is the count-pushdown read path for PromQL `count(<selector>)`: it
// resolves the matched series and checks each for an in-window sample without materializing
// samples (no result batches, no value copies) or labels (no projection) — so a query that
// needs only a cardinality pays none of the per-series decode-to-result cost a full Fetch
// incurs.
//
// Correctness mirrors [Engine.Fetch]: parts are decoded once (shared, pooled) and a series'
// in-window existence is found by binary search over its sorted timestamp run; head, recent,
// and mid-flush windows are scanned in memory. A series counts if any source holds an
// in-window sample. Series with no sample in the window are omitted.
func (e *Engine) Count(ctx context.Context, r fetch.Request) (int, error) {
	e.mu.RLock()
	for !e.head.indexSorted() {
		e.mu.RUnlock()
		e.mu.Lock()
		e.head.ensureIndexSorted()
		e.mu.Unlock()
		e.mu.RLock()
	}

	ids := e.head.resolve(r.Matchers)
	plan := e.planFetch(ids, r)
	e.mu.RUnlock()

	defer plan.releaseParts()

	return plan.countActive(ctx, ids, e)
}

// countActive counts ids that have at least one sample in [start, end] across every source the
// plan snapshotted: head/recent/flush windows (in memory) and the decoded parts. It never
// materializes result buffers — a series is counted as soon as any source yields an in-window
// sample, so the remaining sources skip it.
//
// ids is the sorted, deduplicated output of head.resolve, so id→index lookups for the in-memory
// batches are a binary search (no per-call map allocation).
func (p *enginePlan) countActive(ctx context.Context, ids []signal.SeriesID, e *Engine) (int, error) {
	active := make([]bool, len(ids))

	mark := func(id signal.SeriesID) {
		if i := seriesIndex(ids, id); i >= 0 {
			active[i] = true
		}
	}

	// In-memory windows first (cheap): head, recent tier, mid-flush buffers. Each batch present is
	// a matched series (planFetch seeds these from ids); mark it active if any sample is in window.
	for _, b := range p.headB {
		markBatchInWindow(b, p.start, p.end, mark)
	}

	for _, b := range p.recentB {
		markBatchInWindow(b, p.start, p.end, mark)
	}

	for _, b := range p.flushB {
		markBatchInWindow(b, p.start, p.end, mark)
	}

	// Parts: a part whose sample bounds fall entirely inside [start, end] guarantees that every
	// matched series it holds has an in-window sample — buildPartIndex records only ids actually
	// present, each with a non-empty row run, and every sample of a present series lies within the
	// part's [minTime, maxTime] ⊆ [start, end]. So a fully-covered part's contribution is just the
	// matched ids present in it: a linear intersection of the two sorted id slices with zero column
	// decode. A partially-overlapping part (a window edge) still decodes its timestamp run and
	// binary-searches for an in-window sample. Since planFetch already time-prunes disjoint parts, a
	// typical count's parts are either pruned or fully covered, collapsing decode to at most the two
	// window-edge parts.
	for _, part := range p.liveParts {
		if part.minTime >= p.start && part.maxTime <= p.end {
			intersectMark(ids, part.index.ids, active)

			continue
		}

		dp, err := e.decodeOf(ctx, part)
		if err != nil {
			return 0, err
		}

		for i, id := range ids {
			if active[i] {
				continue
			}

			rng, ok := part.index.lookup(id)
			if !ok {
				continue
			}

			// dp.ts[rng.start:rng.end] is sorted ascending; lowerBound finds the first index ≥ start.
			rel := lowerBound(dp.ts[rng.start:rng.end], p.start)
			abs := rng.start + rel

			if abs < rng.end && dp.ts[abs] <= p.end {
				active[i] = true
			}
		}
	}

	n := 0
	for _, a := range active {
		if a {
			n++
		}
	}

	return n, nil
}

// intersectMark sets active[i]=true for every ids[i] that also appears in partIDs. Both slices are
// ascending by SeriesID.Compare (ids from head.resolve, partIDs from the part index), so a single
// linear two-pointer merge suffices — no per-id binary search. It is the fully-covered-part count
// shortcut: presence in such a part already implies an in-window sample, so no decode is needed.
func intersectMark(ids, partIDs []signal.SeriesID, active []bool) {
	i, j := 0, 0
	for i < len(ids) && j < len(partIDs) {
		switch c := ids[i].Compare(partIDs[j]); {
		case c < 0:
			i++
		case c > 0:
			j++
		default:
			active[i] = true
			i++
			j++
		}
	}
}

// seriesIndex returns the index of id in the sorted ids slice, or -1.
func seriesIndex(ids []signal.SeriesID, id signal.SeriesID) int {
	lo, hi := 0, len(ids)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		switch c := ids[mid].Compare(id); {
		case c < 0:
			lo = mid + 1
		case c > 0:
			hi = mid
		default:
			return mid
		}
	}

	return -1
}

// markBatchInWindow calls mark with the batch's id if any of its samples falls in [start, end].
func markBatchInWindow(b *fetch.Batch, start, end int64, mark func(signal.SeriesID)) {
	if b == nil {
		return
	}

	for _, ts := range b.Timestamps {
		if ts >= start && ts <= end {
			mark(b.ID)

			return
		}
	}
}
