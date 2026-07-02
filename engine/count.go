package engine

import (
	"bytes"
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
	ids, plan := e.planCount(r)
	defer plan.releaseParts()

	active, err := plan.activeFlags(ctx, ids, e)
	if err != nil {
		return 0, err
	}

	n := 0

	for _, a := range active {
		if a {
			n++
		}
	}

	return n, nil
}

// CountBy is the grouped variant of [Engine.Count]: it returns, for each distinct value of the
// given label among the matched series, how many of them have at least one sample in
// [r.Start, r.End]. It backs the PromQL `count by (label)(<selector>)` pushdown — and, via the
// result's length, `count(count by (label)(...))`, i.e. distinct-label-values — with the same
// zero-decode economics as Count: group membership comes from the snapshotted series identities
// (no label projection into results) and in-window existence from the part index / timestamp-only
// edge decode.
//
// Grouping key: the label's canonical text (map key), read from the series identity over the same
// key space the postings index (and matchers) see — point attributes first, then the
// otel.scope.name/version synthetics, scope attributes, and resource attributes. Matched series
// without the label group under "" (indistinguishable from an explicit empty-text value, matching
// PromQL's absent-label grouping). Groups whose every series is empty in the window are omitted.
func (e *Engine) CountBy(ctx context.Context, r fetch.Request, label []byte) (map[string]int, error) {
	ids, plan := e.planCount(r)
	defer plan.releaseParts()

	active, err := plan.activeFlags(ctx, ids, e)
	if err != nil {
		return nil, err
	}

	var (
		groups  = make(map[string]int)
		scratch []byte
	)

	for i, a := range active {
		if !a {
			continue
		}

		scratch = scratch[:0]
		if v, ok := seriesLabelValue(plan.series[i], label); ok {
			scratch = v.AppendText(scratch)
		}

		groups[string(scratch)]++
	}

	return groups, nil
}

// planCount resolves r's matched ids and builds the fetch plan for a count-shaped read (existence
// only): the decode budget reserves timestamps only, and only window-edge parts ever decode. The
// caller must releaseParts.
func (e *Engine) planCount(r fetch.Request) ([]signal.SeriesID, *enginePlan) {
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

	// Count decodes timestamps only (existence), and only for window-edge parts; reserve that.
	plan.acquireDecodeBudget(colNeed{})

	return ids, plan
}

// seriesLabelValue returns the series' value for the queryable label name, over the same flattened
// key space head.indexLabels registers in the postings index: point attributes take precedence,
// then the otel.scope.name/version synthetics, scope attributes, and resource attributes. ok is
// false when the series does not carry the label.
func seriesLabelValue(s signal.Series, name []byte) (signal.Value, bool) {
	if v, ok := s.Attributes.Get(name); ok {
		return v, true
	}

	if bytes.Equal(name, labelScopeName) && len(s.Scope.Name) > 0 {
		return signal.StringValue(s.Scope.Name), true
	}

	if bytes.Equal(name, labelScopeVersion) && len(s.Scope.Version) > 0 {
		return signal.StringValue(s.Scope.Version), true
	}

	if v, ok := s.Scope.Attributes.Get(name); ok {
		return v, true
	}

	if v, ok := s.Resource.Attributes.Get(name); ok {
		return v, true
	}

	return signal.Value{}, false
}

// activeFlags reports, per matched id, whether it has at least one sample in [start, end] across
// every source the plan snapshotted: head/recent/flush windows (in memory) and the decoded parts.
// It never materializes result buffers — a series is flagged as soon as any source yields an
// in-window sample, so the remaining sources skip it.
//
// ids is the sorted, deduplicated output of head.resolve, so id→index lookups for the in-memory
// batches are a binary search (no per-call map allocation).
func (p *enginePlan) activeFlags(ctx context.Context, ids []signal.SeriesID, e *Engine) ([]bool, error) {
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
	// decode. A partially-overlapping part (a window edge) decodes its timestamp column only
	// (colNeed{} skips the value column count never reads) and binary-searches for an in-window
	// sample. Since planFetch already time-prunes disjoint parts, a typical count's parts are either
	// pruned or fully covered, collapsing decode to at most the two window-edge parts — and even
	// those decode no values.
	for _, part := range p.liveParts {
		if part.minTime >= p.start && part.maxTime <= p.end {
			if err := part.index.intersectMark(ctx, ids, active); err != nil {
				return nil, err
			}

			continue
		}

		if err := p.markEdgePart(ctx, e, part, ids, active); err != nil {
			return nil, err
		}
	}

	return active, nil
}

// markEdgePart marks every still-inactive matched series in a partially-covered part that has an
// in-window sample. It decodes only the blocks those series' row runs touch (series-skip) and only
// the timestamp column (count never reads values), then binary-searches each run for a sample in
// [start, end].
func (p *enginePlan) markEdgePart(ctx context.Context, e *Engine, part *part, ids []signal.SeriesID, active []bool) error {
	type idRange struct {
		i   int
		rng rowRange
	}

	var matched []idRange

	for i, id := range ids {
		if active[i] {
			continue
		}

		rng, ok, err := part.index.lookup(ctx, id)
		if err != nil {
			return err
		}

		if ok {
			matched = append(matched, idRange{i: i, rng: rng})
		}
	}

	if len(matched) == 0 {
		return nil
	}

	ranges := make([]rowRange, len(matched))
	for k := range matched {
		ranges[k] = matched[k].rng
	}

	dp, err := e.decodeOf(ctx, part, colNeed{}, ranges)
	if err != nil {
		return err
	}

	for _, m := range matched {
		// dp.ts[rng.start:rng.end] is sorted ascending; lowerBound finds the first index ≥ start.
		rel := lowerBound(dp.ts[m.rng.start:m.rng.end], p.start)
		abs := m.rng.start + rel

		if abs < m.rng.end && dp.ts[abs] <= p.end {
			active[m.i] = true
		}
	}

	return nil
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
