package engine

import (
	"context"
	"slices"

	"github.com/oteldb/storage/index/symbols"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// LabelNames returns, sorted, the distinct attribute names carried by the series matching r.Matchers
// that hold samples in [r.Start, r.End]. LabelValues is its per-name twin, returning the distinct
// canonical-text values of one name.
//
// Both answer from the **inverted index**, not from series: with no matchers the walk is over the
// postings' (name → values) map, so an unmatched label query — the shape a dashboard's template
// variable issues — costs O(distinct values), not O(series). That is the difference between reading
// a few thousand symbols and materializing a million identities to project and deduplicate them.
// With matchers it narrows to the matched ids and reads only the requested name off each identity
// (never a whole label set).
//
// Liveness is what keeps the index-driven answer honest: the head's series index is **all-time** (it
// outlives flushes, and retention prunes samples and parts, never identities), so a value is emitted
// only once some series carrying it is found live — an in-window in-memory sample, or membership in
// a part overlapping the window. Like [Engine.Series] the part test is overlap-granular, and the
// probe stops at the first live series, so a live value costs one probe rather than a scan of its
// postings list.
func (e *Engine) LabelNames(ctx context.Context, r fetch.Request) ([]string, error) {
	pr, err := e.newLabelProbe(ctx, r)
	if err != nil {
		return nil, err
	}

	defer pr.release()

	e.mu.RLock()
	defer e.mu.RUnlock()

	if len(r.Matchers) > 0 {
		return e.matchedLabelNames(ctx, pr, r)
	}

	var (
		out    []string
		names  []uint32
		nameOf = e.head.sym
	)

	e.head.post.ForEachName(func(nameID uint32, _, _ int) { names = append(names, nameID) })

	for _, nameID := range names {
		live, err := pr.nameLive(ctx, e, nameID)
		if err != nil {
			return nil, err
		}

		if !live {
			continue
		}

		if raw, ok := nameOf.Get(symbols.ID(nameID)); ok {
			out = append(out, string(raw))
		}
	}

	slices.Sort(out)

	return out, nil
}

// LabelValues returns, sorted, the distinct canonical-text values of name across the series matching
// r.Matchers that hold samples in [r.Start, r.End]. See [Engine.LabelNames] for the shared contract.
func (e *Engine) LabelValues(ctx context.Context, r fetch.Request, name []byte) ([]string, error) {
	pr, err := e.newLabelProbe(ctx, r)
	if err != nil {
		return nil, err
	}

	defer pr.release()

	e.mu.RLock()
	defer e.mu.RUnlock()

	if len(r.Matchers) > 0 {
		return e.matchedLabelValues(ctx, pr, r, name)
	}

	nameID, ok := e.head.sym.Lookup(name)
	if !ok {
		return nil, nil
	}

	var (
		out     []string
		scratch []byte
	)

	for _, valueID := range e.head.post.LabelValues(uint32(nameID)) {
		live, err := pr.valueLive(ctx, e, uint32(nameID), valueID)
		if err != nil {
			return nil, err
		}

		if !live {
			continue
		}

		raw, ok := e.head.sym.Get(symbols.ID(valueID))
		if !ok {
			continue
		}

		v, _, err := signal.DecodeValue(raw)
		if err != nil {
			continue
		}

		scratch = v.AppendText(scratch[:0])
		out = append(out, string(scratch))
	}

	slices.Sort(out)

	return slices.Compact(out), nil
}

// matchedLabelNames is [Engine.LabelNames] restricted to a matcher set: the matched ids are the
// narrow set already, so it reads each live identity's names directly. Caller holds e.mu.
func (e *Engine) matchedLabelNames(ctx context.Context, pr *labelProbe, r fetch.Request) ([]string, error) {
	seen := make(map[string]struct{})

	for _, id := range e.head.resolve(r.Matchers) {
		live, err := pr.seriesLive(ctx, e, id)
		if err != nil {
			return nil, err
		}

		if !live {
			continue
		}

		s, ok := e.head.series.Get(id)
		if !ok {
			continue
		}

		forEachLabelName(s, func(name []byte) { seen[string(name)] = struct{}{} })
	}

	out := make([]string, 0, len(seen))
	for name := range seen {
		out = append(out, name)
	}

	slices.Sort(out)

	return out, nil
}

// matchedLabelValues is [Engine.LabelValues] restricted to a matcher set: it reads only the
// requested name off each live matched identity, never a whole label set. Caller holds e.mu.
func (e *Engine) matchedLabelValues(
	ctx context.Context, pr *labelProbe, r fetch.Request, name []byte,
) ([]string, error) {
	var (
		seen    = make(map[string]struct{})
		scratch []byte
	)

	for _, id := range e.head.resolve(r.Matchers) {
		live, err := pr.seriesLive(ctx, e, id)
		if err != nil {
			return nil, err
		}

		if !live {
			continue
		}

		s, ok := e.head.series.Get(id)
		if !ok {
			continue
		}

		v, ok := seriesLabelValue(s, name)
		if !ok {
			continue
		}

		scratch = v.AppendText(scratch[:0])
		if _, dup := seen[string(scratch)]; !dup {
			seen[string(scratch)] = struct{}{}
		}
	}

	out := make([]string, 0, len(seen))
	for v := range seen {
		out = append(out, v)
	}

	slices.Sort(out)

	return out, nil
}

// forEachLabelName calls fn for every queryable label name of s, over the same flattened key space
// head.indexLabels registers in the postings index (point attributes, the otel.scope.* synthetics,
// scope attributes, resource attributes). Names may repeat across scopes; the caller deduplicates.
func forEachLabelName(s signal.Series, fn func(name []byte)) {
	for _, kv := range s.Attributes {
		fn(kv.Key)
	}

	if len(s.Scope.Name) > 0 {
		fn(labelScopeName)
	}

	if len(s.Scope.Version) > 0 {
		fn(labelScopeVersion)
	}

	for _, kv := range s.Scope.Attributes {
		fn(kv.Key)
	}

	for _, kv := range s.Resource.Attributes {
		fn(kv.Key)
	}
}

// labelProbe answers "is this series live in the window?" without decoding a column: the in-memory
// tiers are checked exactly, the flushed parts by membership in a part overlapping the window. It
// holds the acquired in-window parts, whose indexes are **warmed before the engine lock is taken**,
// so every probe under the lock is a pure in-memory binary search.
type labelProbe struct {
	plan *enginePlan
}

func (e *Engine) newLabelProbe(ctx context.Context, r fetch.Request) (*labelProbe, error) {
	// Sort the index under the write lock up front: every read below (postings walks, matcher
	// resolution) assumes a sorted index, and sorting is in place — a reader that did it under the
	// read lock would race another. Unlike the fetch plans this resolves no ids: an unmatched label
	// query must not materialize the whole series set just to acquire its parts.
	e.mu.RLock()
	for !e.head.indexSorted() {
		e.mu.RUnlock()
		e.mu.Lock()
		e.head.ensureIndexSorted()
		e.mu.Unlock()
		e.mu.RLock()
	}

	plan := e.planExistence(nil, r, false)
	e.mu.RUnlock()

	for _, p := range plan.liveParts {
		if err := p.index.warm(ctx); err != nil {
			plan.releaseParts()

			return nil, err
		}
	}

	return &labelProbe{plan: plan}, nil
}

func (p *labelProbe) release() { p.plan.releaseParts() }

// seriesLive reports whether id has an in-window sample in memory or lives in a window-overlapping
// part. Caller holds e.mu.
func (p *labelProbe) seriesLive(ctx context.Context, e *Engine, id signal.SeriesID) (bool, error) {
	start, end := p.plan.start, p.plan.end

	if bufHasInWindow(e.head.samples[id], start, end) || bufHasInWindow(e.flushing[id], start, end) {
		return true, nil
	}

	if e.recentEnabled() && bufHasInWindow(e.recent[id], start, end) {
		return true, nil
	}

	for _, part := range p.plan.liveParts {
		ok, err := part.index.has(ctx, id)
		if err != nil || ok {
			return ok, err
		}
	}

	return false, nil
}

// valueLive reports whether any series carrying nameID=valueID is live, stopping at the first one.
// Caller holds e.mu.
func (p *labelProbe) valueLive(ctx context.Context, e *Engine, nameID, valueID uint32) (bool, error) {
	it := e.head.post.Get(nameID, valueID)
	for it.Next() {
		live, err := p.seriesLive(ctx, e, it.At())
		if err != nil {
			return false, err
		}

		if live {
			return true, nil
		}
	}

	return false, it.Err()
}

// nameLive reports whether any series carrying nameID (with any value) is live. Caller holds e.mu.
func (p *labelProbe) nameLive(ctx context.Context, e *Engine, nameID uint32) (bool, error) {
	for _, valueID := range e.head.post.LabelValues(nameID) {
		live, err := p.valueLive(ctx, e, nameID, valueID)
		if err != nil {
			return false, err
		}

		if live {
			return true, nil
		}
	}

	return false, nil
}
