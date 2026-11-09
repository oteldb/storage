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
// can land on one, and freshest-wins would drop the other's data. Runs are visited newest first, so
// a value-selecting tie goes to the freshest sample.
//
// A merge takes parts of one Agg only ([mergeCohorts]). A read can still meet representatives of two
// Aggs, from parts written under different policies; it folds only those of the smallest Agg and
// reports mixed, so the caller can say a cohort was left out. The pick depends on nothing but the
// samples, so the read stays the same whichever parts merges combine meanwhile.
func foldTie(runs []tsRun, layouts [][]DownsampleTier, cur []int, ts int64, normalizeRaw bool) (v, w float64, tag rollupTag, mixed bool) {
	var (
		raw    = -1
		reps   int
		agg    signal.Aggregation
		hasRep bool
	)

	for i := len(runs) - 1; i >= 0; i-- {
		r := &runs[i]
		if cur[i] >= len(r.ts) || r.ts[cur[i]] != ts {
			continue
		}

		switch t := runTag(layoutOf(layouts, i), ts); {
		case t.interval > 0:
			reps++

			mixed = mixed || hasRep && t.agg != agg

			if !hasRep || t.agg < agg {
				agg, hasRep = t.agg, true
			}
		case raw < 0:
			raw = i
		}
	}

	if reps == 0 {
		k := rawTieAt(&runs[raw], cur[raw], ts, normalizeRaw)
		v, w = runs[raw].vals[k], runs[raw].weight(k)
		advanceTie(runs, cur, ts, normalizeRaw)

		return v, w, rollupTag{}, false
	}

	var acc bucketAcc

	acc.repAgg, acc.hasRep = agg, true

	for i := len(runs) - 1; i >= 0; i-- {
		r := &runs[i]
		if cur[i] >= len(r.ts) || r.ts[cur[i]] != ts {
			continue
		}

		k := cur[i]

		switch t := runTag(layoutOf(layouts, i), ts); {
		case t.interval > 0 && t.agg == agg:
			acc.addRep(ts, r.vals[k], r.weight(k), t.agg)

			if t.interval > tag.interval || t.interval == tag.interval && t.before > tag.before {
				tag = t
			}
		case t.interval == 0 && i == raw:
			k = rawTieAt(r, k, ts, normalizeRaw)
			acc.add(ts, r.vals[k], r.weight(k))
		}
	}

	advanceTie(runs, cur, ts, normalizeRaw)

	_, v, w = acc.result(ts)

	return v, w, tag, mixed
}

func rawTieAt(r *tsRun, at int, ts int64, normalize bool) int {
	if !normalize {
		return at
	}
	for at+1 < len(r.ts) && r.ts[at+1] == ts {
		at++
	}
	return at
}

func advanceTie(runs []tsRun, cur []int, ts int64, normalizeRaw bool) {
	for i := range runs {
		for cur[i] < len(runs[i].ts) && runs[i].ts[cur[i]] == ts {
			cur[i]++
			if !normalizeRaw {
				break
			}
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
	var scratch rollupScratch
	return scratch.roll(m, tiers)
}

type rollupScratch struct {
	ts     []int64
	values []float64
}

func (s *rollupScratch) collect(m *sampleMerge) (ts []int64, values, sf []float64, tags []rollupTag) {
	s.ts, s.values, sf = m.gather(s.ts, s.values, &tags)
	return s.ts, s.values, sf, tags
}

// roll's result is owned by the caller until the next use of s; source runs stay immutable.
func (s *rollupScratch) roll(m *sampleMerge, tiers []DownsampleTier) (ts []int64, values, sf []float64, covered []int64) {
	ts, values, sf, tags := s.collect(m)
	n := 0
	for i, t := range ts {
		if i+1 < len(ts) && t == ts[i+1] && tagAt(tags, i).interval == 0 && tagAt(tags, i+1).interval == 0 {
			continue
		}
		ts[n], values[n] = t, values[i]
		if sf != nil {
			sf[n] = sf[i]
		}
		if tags != nil {
			tags[n] = tags[i]
		}
		n++
	}
	if sf != nil {
		sf = sf[:n]
	}
	if tags != nil {
		tags = tags[:n]
	}
	return downsampleCovering(ts[:n], values[:n], sf, tags, tiers)
}
