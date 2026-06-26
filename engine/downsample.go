package engine

import (
	"cmp"
	"slices"

	"github.com/oteldb/storage/signal"
)

// MergeOptions parameterizes a merge. The zero value is a plain compaction (no retention, no
// downsampling) — the same effect as the historical Merge(ctx, 0).
type MergeOptions struct {
	// RetainFrom drops samples with a timestamp < RetainFrom before the merged part is
	// written (retention). ≤ 0 disables it. It is an absolute unix-nanosecond cutoff so the
	// engine stays free of wall-clock dependencies; the caller derives it from tenant policy.
	RetainFrom int64
	// Downsample, when non-empty, rolls up old samples at merge time (coarsening resolution
	// with age). It reuses the one merge engine — no separate subsystem. The cutoffs are
	// absolute (the caller resolves now − After into Before), keeping the merge deterministic.
	Downsample []DownsampleTier
	// Recompress, when non-nil, rewrites a fully-cold merged part (every sample older than its
	// Before cutoff) with a higher-ratio compression profile — the fourth merge mode after
	// compaction, retention, and downsampling, still one pass over the parts. nil keeps the
	// default (codec-only) compression.
	Recompress *RecompressSpec
}

// DownsampleTier is the absolute (wall-clock-free) form of a tenant downsampling tier: every
// sample older than Before is rolled up into one representative per Interval-wide bucket, the
// bucket's samples combined by Agg. Buckets are aligned to absolute multiples of Interval, so a
// time range's rollup does not depend on when the merge runs. A tier with Interval ≤ 0 is
// ignored. The caller ([storage.Storage]) builds these from [tenant.DownsampleTier] and the
// current time.
type DownsampleTier struct {
	Before   int64 // samples with ts < Before are subject to this tier
	Interval int64 // bucket width, nanoseconds
	Agg      signal.Aggregation
}

// applies reports whether any tier could roll up a sample as old as minTime (i.e. there is data
// old enough to downsample). It lets a single-part merge skip work when nothing is old enough.
func downsampleApplies(tiers []DownsampleTier, minTime int64) bool {
	for _, t := range tiers {
		if t.Interval > 0 && minTime < t.Before {
			return true
		}
	}

	return false
}

// downsample rolls up (ts, values, sf) — sorted ascending by ts with no duplicate timestamps, as
// produced by sampleMerge.collect — according to tiers, returning the rolled-up series (still
// sorted ascending, unique ts). sf carries each input sample's lossy-sampling weight (nil ⇒ every
// weight is 1); the returned sf is nil when every output weight is 1. Samples younger than every
// tier's Before pass through unchanged (weight included). A sample old enough for a tier is
// assigned to the coarsest applicable tier and contributes to that tier's Interval bucket; the
// bucket emits one sample at its aligned start timestamp.
//
// The rollup is weight-aware so a sampled series stays unbiased: Sum emits Σ(value·sf) with weight
// 1, Count emits Σsf (the estimated original count) with weight 1, Avg emits the weighted mean
// with weight 1, and Last/First/Min/Max carry the representative sample's value and its weight.
//
// The transform is a fixed point for an already-rolled-up series under Last/First/Min/Max/Sum/Avg
// (a one-sample bucket aggregates to itself), so repeated merges are stable. Count is the
// exception — re-counting a representative yields 1 — and is documented as non-idempotent.
func downsample(ts []int64, values, sf []float64, tiers []DownsampleTier) ([]int64, []float64, []float64) {
	// Keep only usable tiers, ordered by Before ascending so the first match for a sample is the
	// coarsest tier it qualifies for (smallest Before ⇒ largest age threshold).
	active := make([]DownsampleTier, 0, len(tiers))
	for _, t := range tiers {
		if t.Interval > 0 {
			active = append(active, t)
		}
	}

	if len(active) == 0 || len(ts) == 0 {
		return ts, values, sf
	}

	weight := func(i int) float64 {
		if sf == nil {
			return 1
		}

		return sf[i]
	}

	slices.SortFunc(active, func(a, b DownsampleTier) int { return cmp.Compare(a.Before, b.Before) })

	// Bucket key: the (interval, aligned-start) pair. Including the interval disambiguates the
	// rare case where two tiers' aligned starts coincide across a misaligned Before boundary;
	// raw samples use interval 0 and their own ts, and never collide with a bucket start (a
	// bucket start is strictly below every Before, hence below every raw sample).
	type key struct{ interval, start int64 }

	buckets := make(map[key]*bucketAcc, len(ts))
	order := make([]key, 0, len(ts))

	for i, t := range ts {
		tier, ok := pickTier(active, t)
		if !ok {
			k := key{interval: 0, start: t} // raw, pass through
			b := buckets[k]
			if b == nil {
				b = &bucketAcc{agg: signal.AggLast}
				buckets[k] = b
				order = append(order, k)
			}

			b.add(t, values[i], weight(i))

			continue
		}

		k := key{interval: tier.Interval, start: alignDown(t, tier.Interval)}
		b := buckets[k]
		if b == nil {
			b = &bucketAcc{agg: tier.Agg}
			buckets[k] = b
			order = append(order, k)
		}

		b.add(t, values[i], weight(i))
	}

	// Emit one sample per bucket at its start ts. Sort by ts, finer interval first, so an
	// overlap on a misaligned boundary deterministically keeps the finer (more accurate) value.
	slices.SortFunc(order, func(a, b key) int {
		if c := cmp.Compare(a.start, b.start); c != 0 {
			return c
		}

		return cmp.Compare(a.interval, b.interval)
	})

	outTs := make([]int64, 0, len(order))
	outVal := make([]float64, 0, len(order))

	var outSF []float64

	for _, k := range order {
		if n := len(outTs); n > 0 && outTs[n-1] == k.start {
			continue // a finer bucket already emitted this timestamp
		}

		v, w := buckets[k].result()
		outTs = append(outTs, k.start)
		outVal = append(outVal, v)

		if w != 1 && outSF == nil {
			outSF = make([]float64, len(outTs)-1, len(order))
			for i := range outSF {
				outSF[i] = 1
			}
		}

		if outSF != nil {
			outSF = append(outSF, w)
		}
	}

	return outTs, outVal, outSF
}

// pickTier returns the coarsest tier a sample at ts qualifies for (the first, in Before-ascending
// order, with ts < Before), or ok=false when ts is younger than every tier (stays raw).
func pickTier(active []DownsampleTier, ts int64) (DownsampleTier, bool) {
	for _, t := range active {
		if ts < t.Before {
			return t, true
		}
	}

	return DownsampleTier{}, false
}

// alignDown floors ts to a multiple of interval (interval > 0), correctly for negative ts.
func alignDown(ts, interval int64) int64 {
	r := ts % interval
	if r < 0 {
		r += interval
	}

	return ts - r
}

// bucketAcc accumulates the samples of one downsample bucket, weight-aware so sampled data stays
// unbiased. Input timestamps within a bucket are unique (sampleMerge dedups by ts), so first/last
// are unambiguous. n counts samples; nWeighted sums their weights (the estimated original count);
// wsum sums value·weight (the estimated original total). min/max track the extreme value and the
// weight of the sample that set it.
type bucketAcc struct {
	agg       signal.Aggregation
	n         int64
	nWeighted float64
	wsum      float64
	min, max  float64
	minSF     float64
	maxSF     float64
	firstTs   int64
	firstVal  float64
	firstSF   float64
	lastTs    int64
	lastVal   float64
	lastSF    float64
}

func (b *bucketAcc) add(ts int64, v, sf float64) {
	if b.n == 0 {
		b.min, b.max = v, v
		b.minSF, b.maxSF = sf, sf
		b.firstTs, b.firstVal, b.firstSF = ts, v, sf
		b.lastTs, b.lastVal, b.lastSF = ts, v, sf
		b.wsum, b.nWeighted, b.n = v*sf, sf, 1

		return
	}

	if v < b.min {
		b.min, b.minSF = v, sf
	}

	if v > b.max {
		b.max, b.maxSF = v, sf
	}

	if ts < b.firstTs {
		b.firstTs, b.firstVal, b.firstSF = ts, v, sf
	}

	if ts > b.lastTs {
		b.lastTs, b.lastVal, b.lastSF = ts, v, sf
	}

	b.wsum += v * sf
	b.nWeighted += sf
	b.n++
}

// result returns the bucket's representative (value, weight). The value-selecting aggregations
// carry the chosen sample's weight; the summarizing ones fold the weight into the value and emit
// weight 1 (already unbiased).
func (b *bucketAcc) result() (float64, float64) {
	switch b.agg {
	case signal.AggFirst:
		return b.firstVal, b.firstSF
	case signal.AggMin:
		return b.min, b.minSF
	case signal.AggMax:
		return b.max, b.maxSF
	case signal.AggSum:
		return b.wsum, 1
	case signal.AggAvg:
		return b.wsum / b.nWeighted, 1
	case signal.AggCount:
		return b.nWeighted, 1
	default: // signal.AggLast
		return b.lastVal, b.lastSF
	}
}
