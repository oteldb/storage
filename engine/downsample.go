package engine

import (
	"cmp"
	"math"
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
	// absolute (the caller floors now − After into Before), keeping the merge deterministic.
	Downsample []DownsampleTier
	// Recompress, when non-nil, rewrites a fully-cold merged part (every sample older than its
	// Before cutoff) with a higher-ratio compression profile — the fourth merge mode after
	// compaction, retention, and downsampling, still one pass over the parts. nil keeps the
	// default (codec-only) compression.
	Recompress *RecompressSpec
	// Precision, when non-empty, re-encodes a cold part's value column lossily at merge — fewer
	// significant mantissa bits for older data (age-tiered) — the fifth merge mode, still one
	// pass. The cutoffs are absolute (the caller resolves now − After into Before). Empty keeps
	// every part lossless.
	Precision []PrecisionTier
	// Background marks this merge as the maintenance loop's own, where declining costs nothing: if
	// the process merge budget ([Config.MergeAdmission]) is fully committed the merge is skipped and
	// the next cycle retries it, rather than waiting.
	//
	// It is opt-in because waiting is the safe default. A caller that asked for a merge — an
	// operator command, a test, an embedder driving the engine itself — must get one, not a silent
	// no-op; the maintenance loop is the only caller for which the opposite is true, because it runs
	// on one goroutine that also services flush pressure and must not park on a busy budget.
	Background bool
	// Force takes the best run of unsealed parts even when it does not earn its rewrite, instead of
	// selecting nothing — the operator escape from a fixed point where every run scores below
	// [minMergeMultiplier] and the engine would sit on its part count until the idle waiver fires.
	// It bypasses the selection heuristic only: the seal threshold and the run's cumulative-bytes
	// cap still bound what one merge reads, writes, and holds.
	Force bool
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
// sorted ascending, unique ts). sf carries each input sample's weight (nil ⇒ every weight is 1);
// the returned sf is nil when every output weight is 1. Samples younger than every tier's Before
// pass through unchanged (weight included). A sample old enough for a tier is assigned to the
// coarsest applicable tier and contributes to that tier's Interval bucket, which emits one
// representative.
//
// Last/First/Min/Max emit the chosen sample itself — its timestamp, value and weight — so a
// representative is a real sample and re-rolling it is exact. Sum and Count emit at the bucket
// start with weight 1 (the weight is folded into the value); Avg emits the weighted mean at the
// bucket start with the bucket's total weight. A coarser Sum or Avg over representatives is
// therefore the one-pass rollup up to floating-point grouping: the same sum, added in a different
// order, since each representative was rounded once when stored. A Count representative carries its
// count in its value, so only a merge that knows it is one ([downsampleCovering] with tags) can
// combine it; here it would recount as 1.
//
// Min and Max ignore NaN while the bucket holds any other value; an all-NaN bucket emits its first
// NaN. First and Last take the sample whatever its value, and a NaN in a Sum or Avg bucket makes
// its representative NaN, so every aggregation composes the same way with NaN as without.
func downsample(ts []int64, values, sf []float64, tiers []DownsampleTier) ([]int64, []float64, []float64) {
	ts, values, sf, _ = downsampleCovering(ts, values, sf, nil, tiers)

	return ts, values, sf
}

// downsampleCovering is [downsample] over samples tagged with what they stand for (tags nil: all
// raw), returning also each output sample's newest source timestamp: a representative can sit before
// samples it rolled up (First, Min, Max, and every bucket-start aggregation), and the watermark a
// replica trims its head through must not fall back with it. covered is nil when it equals the
// output timestamps.
//
// A representative rolls into the wider of its own recorded tier and the one tiers assign it, and
// combines by its recorded Agg, which the bucket keeps over the tier's. Rolled data thus keeps the Agg
// it was rolled with, and a Count representative adds its count rather than counting as 1.
func downsampleCovering(
	ts []int64, values, sf []float64, tags []rollupTag, tiers []DownsampleTier,
) ([]int64, []float64, []float64, []int64) {
	active := make([]DownsampleTier, 0, len(tiers))
	for _, t := range tiers {
		if t.Interval > 0 {
			active = append(active, t)
		}
	}

	if len(ts) == 0 || len(active) == 0 && !repBucketShared(ts, tags) {
		return ts, values, sf, nil
	}

	weight := func(i int) float64 {
		if sf == nil {
			return 1
		}

		return sf[i]
	}

	slices.SortStableFunc(active, widestFirst)

	// Bucket key: the (interval, aligned-start) pair. Including the interval disambiguates the
	// rare case where two tiers' aligned starts coincide across a misaligned Before boundary;
	// raw samples use interval 0 and their own ts, and never collide with a bucket start (a
	// bucket start is strictly below every Before, hence below every raw sample).
	type key struct{ interval, start int64 }

	buckets := make(map[key]*bucketAcc, len(ts))
	order := make([]key, 0, len(ts))

	for i, t := range ts {
		tag := tagAt(tags, i)
		interval, agg := rollTarget(active, t, tag)

		k := key{interval: 0, start: t} // raw, pass through
		if interval > 0 {
			k = key{interval: interval, start: alignDown(t, interval)}
		}

		b := buckets[k]
		if b == nil {
			b = &bucketAcc{agg: agg}
			buckets[k] = b
			order = append(order, k)
		}

		if tag.interval > 0 {
			b.addRep(t, values[i], weight(i), tag.agg)
		} else {
			b.add(t, values[i], weight(i))
		}
	}

	reps := make([]rollupRep, 0, len(order))
	for _, k := range order {
		b := buckets[k]
		ts, v, w := b.result(k.start)
		reps = append(reps, rollupRep{ts: ts, interval: k.interval, covered: b.lastTs, v: v, w: w})
	}

	// Finer interval first on a timestamp tie, so an overlap on a misaligned boundary
	// deterministically keeps the finer (more accurate) value.
	slices.SortFunc(reps, func(a, b rollupRep) int {
		if c := cmp.Compare(a.ts, b.ts); c != 0 {
			return c
		}

		return cmp.Compare(a.interval, b.interval)
	})

	out := rollupOut{ts: make([]int64, 0, len(reps)), val: make([]float64, 0, len(reps))}
	for _, r := range reps {
		out.add(r)
	}

	return out.ts, out.val, out.sf, out.covered
}

// rollupRep is one bucket's representative, with the newest source timestamp it covers.
type rollupRep struct {
	ts, interval, covered int64
	v, w                  float64
}

// rollupOut collects the representatives in timestamp order. sf and covered stay nil until a row
// differs from its default (weight 1, covered = ts).
type rollupOut struct {
	ts      []int64
	val     []float64
	sf      []float64
	covered []int64
}

func (o *rollupOut) add(r rollupRep) {
	if n := len(o.ts); n > 0 && o.ts[n-1] == r.ts {
		// The coarser bucket's value is dropped, but the samples it held are still covered.
		if o.covered == nil && r.covered > o.ts[n-1] {
			o.covered = slices.Clone(o.ts)
		}

		if o.covered != nil {
			o.covered[n-1] = max(o.covered[n-1], r.covered)
		}

		return
	}

	o.ts = append(o.ts, r.ts)
	o.val = append(o.val, r.v)

	switch {
	case o.covered != nil:
		o.covered = append(o.covered, r.covered)
	case r.covered != r.ts:
		o.covered = append(slices.Clone(o.ts[:len(o.ts)-1]), r.covered)
	}

	switch {
	case o.sf != nil:
		o.sf = append(o.sf, r.w)
	case r.w != 1:
		o.sf = make([]float64, len(o.ts)-1, cap(o.ts))
		for i := range o.sf {
			o.sf[i] = 1
		}

		o.sf = append(o.sf, r.w)
	}
}

// widestFirst orders tiers so the first a sample qualifies for is the coarsest. Before order would not
// do: quantized cutoffs tie, and tiers whose quanta do not nest can briefly invert.
func widestFirst(a, b DownsampleTier) int {
	if c := cmp.Compare(b.Interval, a.Interval); c != 0 {
		return c
	}

	return cmp.Compare(a.Before, b.Before)
}

// pickTier returns the coarsest tier a sample at ts qualifies for (the first, in Interval-descending
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
// count is nWeighted with each Count representative contributing the count it carries; wsum sums
// value·weight (the estimated original total), compensated by wcomp (Neumaier) so a long bucket's
// total is rounded once rather than once per sample. min/max track the earliest sample holding the
// extreme non-NaN value, or the first sample while every value so far is NaN. repAgg is the recorded
// Agg of the first representative added, which the bucket keeps over agg.
type bucketAcc struct {
	agg       signal.Aggregation
	repAgg    signal.Aggregation
	hasRep    bool
	n         int64
	nWeighted float64
	count     float64
	wsum      float64
	wcomp     float64
	min, max  float64
	minTs     int64
	maxTs     int64
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
	b.addCounted(ts, v, sf, sf)
}

// addRep adds a representative rolled up by agg. Every representative but Count's already carries
// what the bucket needs as an ordinary sample: an anchored sample, a total with weight 1, or a mean
// weighted by its population. A Count representative adds its value to the count. One of another
// Agg than the bucket's (only after an Agg change) folds as a plain sample.
func (b *bucketAcc) addRep(ts int64, v, sf float64, agg signal.Aggregation) {
	if !b.hasRep {
		b.repAgg, b.hasRep = agg, true
	}

	counted := sf
	if agg == signal.AggCount && b.repAgg == signal.AggCount {
		counted = v
	}

	b.addCounted(ts, v, sf, counted)
}

func (b *bucketAcc) addCounted(ts int64, v, sf, counted float64) {
	if b.n == 0 {
		b.min, b.max = v, v
		b.minTs, b.maxTs = ts, ts
		b.minSF, b.maxSF = sf, sf
		b.firstTs, b.firstVal, b.firstSF = ts, v, sf
		b.lastTs, b.lastVal, b.lastSF = ts, v, sf
		b.wsum, b.nWeighted, b.count, b.n = v*sf, sf, counted, 1

		return
	}

	if v < b.min || math.IsNaN(b.min) && !math.IsNaN(v) {
		b.min, b.minTs, b.minSF = v, ts, sf
	}

	if v > b.max || math.IsNaN(b.max) && !math.IsNaN(v) {
		b.max, b.maxTs, b.maxSF = v, ts, sf
	}

	if ts < b.firstTs {
		b.firstTs, b.firstVal, b.firstSF = ts, v, sf
	}

	if ts > b.lastTs {
		b.lastTs, b.lastVal, b.lastSF = ts, v, sf
	}

	b.addWeighted(v * sf)
	b.nWeighted += sf
	b.count += counted
	b.n++
}

func (b *bucketAcc) addWeighted(x float64) {
	t := b.wsum + x
	if math.Abs(b.wsum) >= math.Abs(x) {
		b.wcomp += (b.wsum - t) + x
	} else {
		b.wcomp += (x - t) + b.wsum
	}

	b.wsum = t
}

// total is the compensated sum. Once wsum is infinite or NaN the compensation is meaningless (it
// turns into NaN), so wsum stands alone.
func (b *bucketAcc) total() float64 {
	if math.IsInf(b.wsum, 0) || math.IsNaN(b.wsum) {
		return b.wsum
	}

	return b.wsum + b.wcomp
}

// result returns the bucket's representative (ts, value, weight); start is the bucket's aligned
// start.
func (b *bucketAcc) result(start int64) (int64, float64, float64) {
	agg := b.agg
	if b.hasRep {
		agg = b.repAgg
	}

	switch agg {
	case signal.AggFirst:
		return b.firstTs, b.firstVal, b.firstSF
	case signal.AggMin:
		return b.minTs, b.min, b.minSF
	case signal.AggMax:
		return b.maxTs, b.max, b.maxSF
	case signal.AggSum:
		return start, b.total(), 1
	case signal.AggAvg:
		if b.n == 1 {
			return start, b.lastVal, b.lastSF // v·w/w can round; a re-rolled representative must not drift
		}

		return start, b.total() / b.nWeighted, b.nWeighted
	case signal.AggCount:
		return start, b.count, 1
	default: // signal.AggLast
		return b.lastTs, b.lastVal, b.lastSF
	}
}
