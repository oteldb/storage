package engine

import (
	"github.com/oteldb/storage/signal"
)

// rollupTag is what a merged sample stands for: interval 0 is a raw sample, otherwise a
// representative of an Interval-wide bucket rolled up by agg under a tier ending at before.
type rollupTag struct {
	interval, before int64
	agg              signal.Aggregation
}

func layoutOf(layouts [][]DownsampleTier, i int) []DownsampleTier {
	if layouts == nil {
		return nil
	}

	return layouts[i]
}

func runTag(layout []DownsampleTier, ts int64) rollupTag {
	if len(layout) == 0 {
		return rollupTag{}
	}

	t, ok := tierAt(layout, ts)
	if !ok {
		return rollupTag{}
	}

	return rollupTag{interval: t.Interval, before: t.Before, agg: t.Agg}
}

func tagAt(tags []rollupTag, i int) rollupTag {
	if tags == nil {
		return rollupTag{}
	}

	return tags[i]
}

// appendTag appends tag to the lazily-materialized tags column, like [appendWeight]: nil until the
// first representative.
func appendTag(tags []rollupTag, tag rollupTag, n, capHint int) []rollupTag {
	if tags == nil {
		if tag.interval == 0 {
			return nil
		}

		tags = make([]rollupTag, n-1, capHint)
	}

	return append(tags, tag)
}

// tagSamples tags ts, all from one source recording layout; nil when none is a representative.
func tagSamples(ts []int64, layout []DownsampleTier) []rollupTag {
	var tags []rollupTag

	if len(layout) > 0 {
		for i, t := range ts {
			tags = appendTag(tags, runTag(layout, t), i+1, len(ts))
		}
	}

	return tags
}

func appendTags(tags []rollupTag, r *tsRun, layout []DownsampleTier, lo, hi, n, capHint int) []rollupTag {
	if tags == nil && len(layout) == 0 {
		return nil
	}

	for k := lo; k < hi; k++ {
		tags = appendTag(tags, runTag(layout, r.ts[k]), n-(hi-k)+1, capHint)
	}

	return tags
}

// foldTie consumes every run's sample at ts and returns the one sample kept there. Raw samples
// dedup freshest-wins. Representatives combine instead, with the freshest raw sample folded in as one
// more sample: two sources can each hold a representative of one bucket start, or a late raw sample
// can land on one, and freshest-wins would drop the other's data. They combine by the Agg layout
// assigns ts, the one a merge records; without it, by the oldest source's, which is the one the merge
// layout lists first. Runs are visited newest first, so a value-selecting tie goes to the freshest
// sample.
func foldTie(
	runs []tsRun, layouts [][]DownsampleTier, layout []DownsampleTier, cur []int, ts int64,
) (float64, float64, rollupTag) {
	raw, reps, oldest := -1, 0, -1

	for i := len(runs) - 1; i >= 0; i-- {
		r := &runs[i]
		if cur[i] >= len(r.ts) || r.ts[cur[i]] != ts {
			continue
		}

		switch {
		case runTag(layoutOf(layouts, i), ts).interval > 0:
			reps++
			oldest = i
		case raw < 0:
			raw = i
		}
	}

	if reps == 0 {
		v, w := runs[raw].vals[cur[raw]], runs[raw].weight(cur[raw])
		advanceTie(runs, cur, ts)

		return v, w, rollupTag{}
	}

	var (
		acc bucketAcc
		tag rollupTag
	)

	acc.repAgg, acc.hasRep = runTag(layoutOf(layouts, oldest), ts).agg, true
	if t, ok := tierAt(layout, ts); ok {
		acc.repAgg = t.Agg
	}

	for i := len(runs) - 1; i >= 0; i-- {
		r := &runs[i]
		if cur[i] >= len(r.ts) || r.ts[cur[i]] != ts {
			continue
		}

		k := cur[i]

		switch t := runTag(layoutOf(layouts, i), ts); {
		case t.interval > 0:
			acc.addRep(ts, r.vals[k], r.weight(k), t.agg)

			if t.interval > tag.interval || t.interval == tag.interval && t.before > tag.before {
				tag = t
			}
		case i == raw:
			acc.add(ts, r.vals[k], r.weight(k))
		}
	}

	advanceTie(runs, cur, ts)

	_, v, w := acc.result(ts)
	tag.agg = acc.repAgg

	return v, w, tag
}

func advanceTie(runs []tsRun, cur []int, ts int64) {
	for i := range runs {
		if cur[i] < len(runs[i].ts) && runs[i].ts[cur[i]] == ts {
			cur[i]++
		}
	}
}

// rollTarget returns the bucket width a sample at ts rolls into and the Agg a bucket it opens starts
// with: the wider of the tier active assigns ts and the sample's own recorded tier. fixed reports the
// Agg is active's, which representatives do not override ([bucketAcc.addRep]). Width 0 leaves the
// sample raw.
func rollTarget(active []DownsampleTier, ts int64, tag rollupTag) (interval int64, agg signal.Aggregation, fixed bool) {
	agg = signal.AggLast
	if t, ok := pickTier(active, ts); ok {
		interval, agg, fixed = t.Interval, t.Agg, true
	}

	if tag.interval > interval {
		interval, agg, fixed = tag.interval, tag.agg, false
	}

	return interval, agg, fixed
}

// repBucketShared reports whether a representative shares its bucket with another sample its
// recorded tier covers, the only case a merge applying no tiers has anything to fold: a
// representative alone in its bucket is a fixed point. ts is sorted, so a bucket's samples are
// adjacent.
func repBucketShared(ts []int64, tags []rollupTag) bool {
	for i, tag := range tags {
		if tag.interval == 0 {
			continue
		}

		start := alignDown(ts[i], tag.interval)
		shares := func(j int) bool {
			return j >= 0 && j < len(ts) && ts[j] < tag.before && alignDown(ts[j], tag.interval) == start
		}

		if shares(i-1) || shares(i+1) {
			return true
		}
	}

	return false
}

// rollSeries merges one series' sources and rolls the result up under tiers, combining the
// representatives the sources recorded rather than re-aggregating them as raw samples.
func rollSeries(m *sampleMerge, tiers []DownsampleTier) (ts []int64, values, sf []float64, covered []int64) {
	ts, values, sf, tags := m.collectTagged(tiers)

	return downsampleCovering(ts, values, sf, tags, tiers)
}
