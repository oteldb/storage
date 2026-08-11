package engine

import (
	"context"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// Series returns the identities of the series matching r.Matchers that hold samples in
// [r.Start, r.End], without reading a single value or timestamp. It is the metrics twin of
// recordengine's stream enumeration and backs the label endpoints (/api/v1/labels,
// /api/v1/label/<name>/values, /series): those need identities only, so paying a full fetch —
// decoding and copying every sample of every matching series just to read b.Series — costs
// (cardinality x window) for a list of strings.
//
// The read is **series-only**: matched ids come from the head's postings index (which outlives a
// flush), and each in-window part contributes the matched ids its series index holds. No sample
// column is touched, so the cost is proportional to matched cardinality alone — not to the window's
// depth. Consequently the time filter is **part-overlap granular**, exactly as
// [recordengine.Engine.Series] and Prometheus' own block-granular label endpoints are: a returned
// series is guaranteed to match the matchers and to live in a part overlapping the window (or to
// have an in-memory sample inside it, which is checked exactly), but a series whose samples sit just
// outside the window in an overlapping part may still be listed. Use [Engine.Count] where the
// "has >= 1 sample in the window" test must be exact — it pays a timestamp decode at the window
// edges for it.
//
// The returned identities alias engine-owned, immutable interned memory; copy them to retain past
// an engine Reset.
func (e *Engine) Series(ctx context.Context, r fetch.Request) ([]signal.Series, error) {
	ids, plan := e.planLookup(r, true)
	defer plan.releaseParts()

	// The in-memory tiers were folded in exactly under the plan lock; the parts add every matched id
	// they hold — planLookup already dropped the parts disjoint from the window, so presence in a
	// surviving part is the part-granular answer.
	present := plan.memActive

	for _, part := range plan.liveParts {
		if err := part.index.intersectMark(ctx, ids, present); err != nil {
			return nil, err
		}
	}

	out := make([]signal.Series, 0, len(ids))

	for i, ok := range present {
		if ok {
			out = append(out, plan.series[i])
		}
	}

	return out, nil
}
