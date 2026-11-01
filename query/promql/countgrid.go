package promql

import (
	"context"
	"math"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/oteldb/storage/query/fetch"
)

// CountSeriesGrid counts presence on a nanosecond grid within this querier's window.
// An empty label means ungrouped. Unsupported capabilities or projection require ordinary folding.
func (q *querier) CountSeriesGrid(
	ctx context.Context, grid fetch.WindowGrid, label string, matchers ...*labels.Matcher,
) (map[string][]uint64, error) {
	pushed := PushableMatchers(matchers)
	if len(pushed) != len(matchers) {
		return nil, fetch.ErrCountGridUnsupported
	}
	if _, hidden := hiddenLabels[label]; hidden {
		return nil, fetch.ErrCountGridUnsupported
	}
	counter := fetch.GridCounterOf(q.fetcher)
	if counter == nil {
		return nil, fetch.ErrCountGridUnsupported
	}
	var group []byte
	if label != "" {
		group = []byte(label)
	}
	counts, err := counter.CountGrid(ctx, fetch.Request{
		Tenant: q.tenant, Scope: q.scope,
		Start: msToNsClamp(q.mint, math.MinInt64), End: msToNsClamp(q.maxt, math.MaxInt64),
		Matchers: pushed,
	}, grid, group)
	if err != nil {
		return nil, err
	}
	out := make(map[string][]uint64, len(counts))
	for key, values := range counts {
		converted := make([]uint64, len(values))
		for i, n := range values {
			converted[i] = uint64(n)
		}
		out[key] = converted
	}
	return out, nil
}
