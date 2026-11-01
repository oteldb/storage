package engine

import (
	"math"
	"math/bits"
	"slices"

	"github.com/oteldb/storage/signal"
)

const (
	minLocalMergeEfficiency = 0.9
	minMergeLocalityGain    = 2
)

type mergeSeriesRange struct {
	part        *part
	first, last signal.SeriesID
	span        float64
}

func partSeriesRange(p *part) (mergeSeriesRange, bool) {
	var first, last signal.SeriesID
	if index := p.index.paged; index != nil {
		if !index.bounded || index.n == 0 {
			return mergeSeriesRange{}, false
		}
		first, last = index.first, index.last
	} else {
		ids := p.index.ids
		if len(ids) == 0 {
			return mergeSeriesRange{}, false
		}
		first, last = ids[0], ids[len(ids)-1]
	}
	if first.Compare(last) > 0 {
		return mergeSeriesRange{}, false
	}
	return mergeSeriesRange{part: p, first: first, last: last, span: seriesRangeSpan(first, last)}, true
}

func seriesRangeSpan(first, last signal.SeriesID) float64 {
	low, borrow := bits.Sub64(last.Lo, first.Lo, 0)
	high, _ := bits.Sub64(last.Hi, first.Hi, borrow)
	return math.Ldexp(float64(high), 64) + float64(low) + 1
}

func mergeRunLocality(parts []*part) (density, multiplier float64, ok bool) {
	var first, last signal.SeriesID
	var spans float64
	var total, largest int64
	for i, p := range parts {
		r, found := partSeriesRange(p)
		if !found {
			return 0, 0, false
		}
		if i == 0 || r.first.Compare(first) < 0 {
			first = r.first
		}
		if i == 0 || r.last.Compare(last) > 0 {
			last = r.last
		}
		spans += r.span
		total += p.sizeBytes()
		largest = max(largest, p.sizeBytes())
	}
	if largest <= 0 {
		return 0, 0, false
	}
	return spans / seriesRangeSpan(first, last), float64(total) / float64(largest), true
}

func orderedMergeRanges(eligible []*part) []mergeSeriesRange {
	ranges := make([]mergeSeriesRange, 0, len(eligible))
	for _, p := range eligible {
		if r, found := partSeriesRange(p); found {
			ranges = append(ranges, r)
		}
	}
	slices.SortStableFunc(ranges, func(a, b mergeSeriesRange) int {
		if c := a.first.Compare(b.first); c != 0 {
			return c
		}
		return a.last.Compare(b.last)
	})
	return ranges
}

// Bounds estimate overlap without reading indexes. They never determine which samples survive.
func preferSeriesLocalRun(eligible, current []*part, capBytes int64) []*part {
	density, multiplier, ok := mergeRunLocality(current)
	if !ok || density*minMergeLocalityGain > maxMergeParts {
		return current
	}
	minimumDensity := max(float64(minMergeLocalityGain), density*minMergeLocalityGain)
	minimumMultiplier := multiplier * minLocalMergeEfficiency
	ranges := orderedMergeRanges(eligible)
	bestFirst, bestEnd := 0, 0
	bestDensity, bestMultiplier := minimumDensity, 0.0
	for i := range ranges {
		var spans float64
		var total, largest int64
		smallest := maxInt64
		last := ranges[i].last
		for j := i; j < min(i+maxMergeParts, len(ranges)); j++ {
			r := ranges[j]
			bytes := r.part.sizeBytes()
			total += bytes
			if capBytes > 0 && total > capBytes {
				break
			}
			smallest, largest = min(smallest, bytes), max(largest, bytes)
			spans += r.span
			if r.last.Compare(last) > 0 {
				last = r.last
			}
			n := j - i + 1
			if n < minMergeParts || largest <= 0 {
				continue
			}
			m := float64(total) / float64(largest)
			balanced := total <= smallRunBytes || smallest > (largest-1)/int64(n) && m >= minMergeMultiplier
			if m < minimumMultiplier || !balanced {
				continue
			}
			d := spans / seriesRangeSpan(ranges[i].first, last)
			if d > bestDensity || d == bestDensity && m > bestMultiplier {
				bestFirst, bestEnd = i, j+1
				bestDensity, bestMultiplier = d, m
			}
		}
	}
	if bestEnd == 0 {
		return current
	}
	result := make([]*part, 0, bestEnd-bestFirst)
	for _, r := range ranges[bestFirst:bestEnd] {
		result = append(result, r.part)
	}
	return result
}
