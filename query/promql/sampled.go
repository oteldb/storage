package promql

import (
	"fmt"

	"github.com/prometheus/prometheus/util/annotations"
)

// SampledWarning is the PromQL warning annotation a Select attaches when at least one returned
// series carries lossy-sampling weights (a [fetch.Batch.ScaleFactors] entry above 1). The
// Prometheus engine surfaces it in the query result's warnings, so the embedder and the API
// caller learn the result is computed over a sampled subset: an instant read, min/max or the
// rate of a cumulative counter are unaffected, but count_over_time, sum_over_time and any
// operator that treats each point as an independent event undercount by the dropped rows.
//
//nolint:revive,errname,staticcheck // an annotation, named like the PromQL *Warning sentinels it is matched alongside.
var SampledWarning = fmt.Errorf(
	"%w: result computed over lossy-sampled series; per-sample counts and sums undercount the rows sampling dropped",
	annotations.PromQLWarning,
)

// WeightedSeries is implemented by every series a [Queryable] returns. ScaleFactors returns the
// per-sample lossy-sampling weights in the iterator's sample order, or nil when every weight is 1.
// A chunkenc.Iterator has no weight channel, so the adapter never folds the weight into a value
// (that would scale gauges and cumulative counters, which are correct as stored); an embedder's
// weight-aware operator reads it here instead.
type WeightedSeries interface {
	ScaleFactors() []float64
}

// sampled reports whether sf carries a weight above 1. Weights are ceil(observed/budget) ≥ 1 by
// construction, so anything at or below 1 carries no sampling information.
func sampled(sf []float64) bool {
	for _, w := range sf {
		if w > 1 {
			return true
		}
	}

	return false
}
