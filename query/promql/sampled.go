package promql

import (
	"fmt"

	"github.com/prometheus/prometheus/util/annotations"
)

// SampledWarning is the PromQL warning annotation a Select attaches when at least one returned
// series carries weights (a [fetch.Batch.ScaleFactors] entry above 1): a lossy-sampled row standing
// for the rows sampling dropped, or an Avg downsample representative standing for its bucket's
// samples. The Prometheus engine surfaces it in the query result's warnings. The adapter cannot tell
// the two kinds apart, so the warning claims nothing is exact: count_over_time, sum_over_time and
// any operator that treats each point as an independent event undercount the rows the weights stand
// for, and rate, increase, resets and other reset-sensitive functions are approximate over an Avg
// rollup, whose points are bucket means rather than counter values. Only a sampling weight leaves
// the rate of a cumulative counter intact.
//
//nolint:revive,errname,staticcheck // an annotation, named like the PromQL *Warning sentinels it is matched alongside.
var SampledWarning = fmt.Errorf(
	"%w: result computed over weighted series (lossy-sampled or rolled up); per-sample counts and sums "+
		"undercount the rows behind each point, and rates over rolled-up series are approximate",
	annotations.PromQLWarning,
)

// AmbiguousRollupWarning is the PromQL warning annotation a Select attaches when a returned series
// is [fetch.Batch.AmbiguousRollup]: at some timestamp it held downsample representatives rolled up
// with different aggregations, and the result carries one aggregation's and leaves the others out.
//
//nolint:revive,errname,staticcheck // an annotation, named like the PromQL *Warning sentinels it is matched alongside.
var AmbiguousRollupWarning = fmt.Errorf(
	"%w: result left out downsampled data rolled up with a different aggregation; "+
		"storage nodes run different downsampling policies for this tenant",
	annotations.PromQLWarning,
)

// WeightedSeries is implemented by every series a [Queryable] returns. ScaleFactors returns the
// per-sample weights in the iterator's sample order, or nil when every weight is 1.
// A chunkenc.Iterator has no weight channel, so the adapter never folds the weight into a value
// (that would scale gauges and cumulative counters, which are correct as stored); an embedder's
// weight-aware operator reads it here instead.
type WeightedSeries interface {
	ScaleFactors() []float64
}

// sampled reports whether sf carries a weight above 1. A weight counts the rows a point stands for,
// so anything at or below 1 carries no information.
func sampled(sf []float64) bool {
	for _, w := range sf {
		if w > 1 {
			return true
		}
	}

	return false
}
