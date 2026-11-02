package engine

import (
	"github.com/oteldb/storage/internal/buildbudget"
	"github.com/oteldb/storage/internal/memsize"
)

func mergeSeriesScratchBound(src []*part, tiers []DownsampleTier) int64 {
	var rows, ranges int64
	tierCount := int64(len(tiers))
	weights, tagged := false, false
	for _, p := range src {
		n := mergeMaximumSeriesRows(p)
		rows = buildbudget.Add(rows, n)
		rangeRow := memsize.Of[int64]() + memsize.Of[float64]()
		if p.hasSF {
			rangeRow += memsize.Of[float64]()
		}
		// A growing range temporarily owns both its old and replacement backing arrays.
		ranges = buildbudget.Add(ranges, buildbudget.Mul(n, 2*rangeRow))
		tierCount = buildbudget.Add(tierCount, int64(len(p.rollup)))
		weights = weights || p.hasSF || hasAvgTier(p.rollup)
		tagged = tagged || activeTiers(p.rollup)
	}
	if rows == 0 {
		return 0
	}
	gathered := memsize.Of[int64]() + memsize.Of[float64]()
	if weights {
		gathered += memsize.Of[float64]()
	}
	if tagged {
		gathered += memsize.Of[rollupTag]()
	}
	bound := buildbudget.Add(ranges, buildbudget.Mul(rows, gathered))
	header := 2 * (memsize.Of[tsRun]() + memsize.Of[[]DownsampleTier]())
	bound = buildbudget.Add(bound, buildbudget.Mul(int64(len(src)), header+memsize.Of[rangeBuf]()))
	bound = buildbudget.Add(bound, buildbudget.Mul(tierCount, 4*memsize.Of[DownsampleTier]()))
	if !tagged && !activeTiers(tiers) {
		return bound
	}
	// The map is hinted for every input row, even when most rows remain raw. Allow slot slack
	// and concurrent old/new map storage rather than memsize's average occupancy alone.
	perRow := memsize.Of[bucketAcc]() + 4*memsize.MapEntry[rollupBucketKey, *bucketAcc]()
	perRow += memsize.Of[rollupBucketKey]() + memsize.Of[rollupRep]()
	// Timestamp/value outputs are presized; coverage can clone and repeatedly grow its array.
	perRow += memsize.Of[int64]() + memsize.Of[float64]() + 4*memsize.Of[int64]()
	if weights || hasAvgTier(tiers) {
		perRow += memsize.Of[float64]()
	}
	return buildbudget.Add(bound, buildbudget.Add(memsize.MapBase, buildbudget.Mul(rows, perRow)))
}

func mergeMaximumSeriesRows(p *part) int64 {
	rows := p.index.maxRows
	if p.index.paged != nil {
		rows = p.index.paged.maxRows
	}
	if rows > 0 {
		return int64(rows)
	}
	if p.rows() == 0 {
		return 0
	}
	return buildbudget.Add(max(int64(p.rows())-int64(p.index.seriesCount()), 0), 1)
}
