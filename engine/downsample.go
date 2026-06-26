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

// downsample rolls up (ts, values) — sorted ascending by ts with no duplicate timestamps, as
// produced by sampleMerge.collect — according to tiers, returning the rolled-up series (still
// sorted ascending, unique ts). Samples younger than every tier's Before pass through unchanged.
// A sample old enough for a tier is assigned to the coarsest applicable tier (the one with the
// largest Before it still falls under) and contributes to that tier's Interval bucket; the bucket
// emits one sample at its aligned start timestamp with the Agg-combined value.
//
// The transform is a fixed point for an already-rolled-up series under Last/First/Min/Max/Sum/Avg
// (a one-sample bucket aggregates to itself), so repeated merges are stable. Count is the
// exception — re-counting a representative yields 1 — and is documented as non-idempotent.
func downsample(ts []int64, values []float64, tiers []DownsampleTier) ([]int64, []float64) {
	// Keep only usable tiers, ordered by Before ascending so the first match for a sample is the
	// coarsest tier it qualifies for (smallest Before ⇒ largest age threshold).
	active := make([]DownsampleTier, 0, len(tiers))
	for _, t := range tiers {
		if t.Interval > 0 {
			active = append(active, t)
		}
	}

	if len(active) == 0 || len(ts) == 0 {
		return ts, values
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

			b.add(t, values[i])

			continue
		}

		k := key{interval: tier.Interval, start: alignDown(t, tier.Interval)}
		b := buckets[k]
		if b == nil {
			b = &bucketAcc{agg: tier.Agg}
			buckets[k] = b
			order = append(order, k)
		}

		b.add(t, values[i])
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

	for _, k := range order {
		if n := len(outTs); n > 0 && outTs[n-1] == k.start {
			continue // a finer bucket already emitted this timestamp
		}

		outTs = append(outTs, k.start)
		outVal = append(outVal, buckets[k].result())
	}

	return outTs, outVal
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

// bucketAcc accumulates the samples of one downsample bucket. Input timestamps within a bucket
// are unique (sampleMerge dedups by ts), so first/last are unambiguous.
type bucketAcc struct {
	agg      signal.Aggregation
	count    int64
	sum      float64
	min, max float64
	firstTs  int64
	firstVal float64
	lastTs   int64
	lastVal  float64
}

func (b *bucketAcc) add(ts int64, v float64) {
	if b.count == 0 {
		b.min, b.max = v, v
		b.firstTs, b.firstVal = ts, v
		b.lastTs, b.lastVal = ts, v
		b.sum, b.count = v, 1

		return
	}

	b.min = min(b.min, v)
	b.max = max(b.max, v)

	if ts < b.firstTs {
		b.firstTs, b.firstVal = ts, v
	}

	if ts > b.lastTs {
		b.lastTs, b.lastVal = ts, v
	}

	b.sum += v
	b.count++
}

func (b *bucketAcc) result() float64 {
	switch b.agg {
	case signal.AggFirst:
		return b.firstVal
	case signal.AggMin:
		return b.min
	case signal.AggMax:
		return b.max
	case signal.AggSum:
		return b.sum
	case signal.AggAvg:
		return b.sum / float64(b.count)
	case signal.AggCount:
		return float64(b.count)
	default: // signal.AggLast
		return b.lastVal
	}
}
