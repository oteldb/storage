package engine

import (
	"context"
	"slices"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// AggregateRange returns a per-series aggregate (count, sum, min, max — enough for avg) over
// [r.Start, r.End] for the series matching r.Matchers. It is the aggregate-pushdown read path:
// for parts the range fully covers, it folds each part's precomputed stats sidecar instead of
// decoding the value column — so an aggregate over many points returns one number per series for
// almost no I/O — and falls back to decoding + merging when that would not be exact (a part only
// partially in range, parts overlapping in time so timestamps could be duplicated, or a sampled/
// sidecar-less part). The fast path is taken in the common compacted, time-disjoint case.
//
// Series with no sample in the window are omitted from the result.
func (e *Engine) AggregateRange(ctx context.Context, r fetch.Request) (map[signal.SeriesID]SeriesAgg, error) {
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

	safe := aggPushdownSafe(plan)

	out := make(map[signal.SeriesID]SeriesAgg, len(ids))

	for _, id := range ids {
		var (
			agg SeriesAgg
			err error
		)

		if safe {
			agg, err = e.aggViaStats(ctx, plan, id)
		} else {
			agg, err = aggViaDecode(ctx, plan, id)
		}

		if err != nil {
			return nil, err
		}

		if agg.Count > 0 {
			out[id] = agg
		}
	}

	return out, nil
}

// aggPushdownSafe reports whether the plan's parts can be aggregated from their stats sidecars
// without risking a wrong count/sum: every in-window part must be fully inside [start, end] (else
// its whole-part stats would include out-of-range samples) and the parts — plus any head/mid-flush
// samples — must be pairwise time-disjoint (else a timestamp could appear in two sources and be
// counted twice). When false, the caller decodes and merges, which dedups by timestamp.
func aggPushdownSafe(plan *enginePlan) bool {
	type span struct{ lo, hi int64 }

	spans := make([]span, 0, len(plan.liveParts)+1)

	for _, p := range plan.liveParts {
		if p.minTime < plan.start || p.maxTime > plan.end {
			return false // partially covered: whole-part stats are not range-exact
		}

		spans = append(spans, span{p.minTime, p.maxTime})
	}

	// The head + mid-flush samples in window form one more span (they are newer, unflushed data).
	if lo, hi, ok := planHeadSpan(plan); ok {
		spans = append(spans, span{lo, hi})
	}

	slices.SortFunc(spans, func(a, b span) int {
		switch {
		case a.lo < b.lo:
			return -1
		case a.lo > b.lo:
			return 1
		default:
			return 0
		}
	})

	for i := 1; i < len(spans); i++ {
		if spans[i].lo <= spans[i-1].hi {
			return false // overlapping time ranges ⇒ a timestamp could be duplicated across sources
		}
	}

	return true
}

// planHeadSpan returns the [min, max] timestamp of the plan's in-window head + mid-flush samples,
// and whether there are any.
func planHeadSpan(plan *enginePlan) (lo, hi int64, ok bool) {
	consider := func(b *fetch.Batch) {
		for _, ts := range b.Timestamps {
			if !ok {
				lo, hi, ok = ts, ts, true

				continue
			}

			if ts < lo {
				lo = ts
			}

			if ts > hi {
				hi = ts
			}
		}
	}

	for _, b := range plan.headB {
		consider(b)
	}

	for _, b := range plan.flushB {
		consider(b)
	}

	return lo, hi, ok
}

// aggViaStats folds id's aggregate from each covering part's stats sidecar (decoding only a part
// whose sidecar is absent — old or sampled), plus the in-window head/mid-flush samples. Used only
// when [aggPushdownSafe] holds, so every contribution is range-exact and disjoint.
func (e *Engine) aggViaStats(ctx context.Context, plan *enginePlan, id signal.SeriesID) (SeriesAgg, error) {
	var agg SeriesAgg

	for _, p := range plan.liveParts {
		rng, ok := p.ranges[id]
		if !ok {
			continue
		}

		if st, ok := p.seriesStat(ctx, id); ok {
			agg.merge(st)

			continue
		}

		// No sidecar (a pre-sidecar or sampled part): decode this part's run and fold it. Coverage
		// is full (safe), so the whole run is in range.
		dp, err := e.decodeOf(ctx, p)
		if err != nil {
			return agg, err
		}

		foldRange(&agg, dp, rng, plan.start, plan.end)
	}

	if hb := plan.headB[id]; hb != nil {
		foldBatch(&agg, hb, plan.start, plan.end)
	}

	if fb := plan.flushB[id]; fb != nil {
		foldBatch(&agg, fb, plan.start, plan.end)
	}

	return agg, nil
}

// aggViaDecode is the exact fallback: it decodes and merges id's samples (deduping by timestamp,
// freshest wins) and folds the result — identical to what a raw fetch would return, aggregated.
func aggViaDecode(ctx context.Context, plan *enginePlan, id signal.SeriesID) (SeriesAgg, error) {
	m, err := plan.mergeSeries(ctx, id)
	if err != nil {
		return SeriesAgg{}, err
	}

	_, values, _ := m.collect()

	var agg SeriesAgg
	for _, v := range values {
		agg.addSample(v)
	}

	return agg, nil
}

// foldRange folds dp's value rows [rng.start, rng.end) whose timestamp is within [start, end].
func foldRange(agg *SeriesAgg, dp *decodedPart, rng rowRange, start, end int64) {
	for i := rng.start; i < rng.end; i++ {
		if dp.ts[i] < start || dp.ts[i] > end {
			continue
		}

		agg.addSample(dp.vals[i])
	}
}

// foldBatch folds a fetch batch's values whose timestamp is within [start, end].
func foldBatch(agg *SeriesAgg, b *fetch.Batch, start, end int64) {
	for i, ts := range b.Timestamps {
		if ts < start || ts > end {
			continue
		}

		agg.addSample(b.Values[i])
	}
}
