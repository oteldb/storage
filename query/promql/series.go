package promql

import (
	"context"
	"slices"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

func (q *querier) selectSeries(ctx context.Context, sortSeries bool, matchers []*labels.Matcher) storage.SeriesSet {
	// /series may select several matcher sets concurrently on one querier. Keep the projection
	// scratch local while sharing the scope and the concurrency-safe label cache.
	local := &querier{
		fetcher: q.fetcher, tenant: q.tenant, mint: q.mint, maxt: q.maxt, labels: q.labels, scope: q.scope,
	}
	sets, err := local.seriesLabels(ctx, matchers)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}

	slab := make([]batchSeries, len(sets))
	out := make([]storage.Series, 0, len(sets))
	seen := make(map[uint64]int, len(sets))
	next := make([]int, len(sets))
	for _, ls := range sets {
		hash := ls.Hash()
		duplicate := false
		for ref := seen[hash]; ref != 0; ref = next[ref-1] {
			if labels.Equal(out[ref-1].Labels(), ls) {
				duplicate = true
				break
			}
		}
		if duplicate {
			continue
		}

		i := len(out)
		slab[i].labels = ls
		next[i] = seen[hash]
		seen[hash] = i + 1
		out = append(out, &slab[i])
	}
	if sortSeries {
		slices.SortFunc(out, func(a, b storage.Series) int { return labels.Compare(a.Labels(), b.Labels()) })
	}

	return newSliceSeriesSet(out, nil)
}
