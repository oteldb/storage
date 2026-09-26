// Package timebucket is the aligned time-bucket ladder both engines confine merges to, and the
// straddler selection over it. The engines' ladder walks differ (size tiers against scored runs);
// what is shared here is the bucket arithmetic and the handling of a part that fits no bucket.
package timebucket

import (
	"cmp"
	"slices"
	"time"
)

// Ladder is the merge bucket ladder, ascending. Each level divides the next, so a bucket nests
// exactly inside its parent and a part promoted upward never straddles. The top level is the widest
// part a merge builds, and so the coarsest time locality a query can rely on.
var Ladder = []int64{
	int64(time.Hour),
	int64(6 * time.Hour),
	int64(24 * time.Hour),
}

const maxInt64 = int64(1<<63 - 1)

// Top is the widest ladder level, and so the boundary every merge output is split on.
func Top() int64 { return Ladder[len(Ladder)-1] }

// Of returns the start of the level-aligned bucket holding ts. It rounds toward negative infinity,
// not toward zero: Go's % keeps the dividend's sign, which would make the bucket at ts = -1 start at
// 0 and overlap its successor.
func Of(ts, level int64) int64 {
	m := ts % level
	if m < 0 {
		m += level
	}

	return ts - m
}

// End returns the last timestamp of the level-aligned bucket holding ts, saturating at the int64
// maximum instead of overflowing past it.
func End(ts, level int64) int64 {
	b := Of(ts, level)
	if b > maxInt64-(level-1) {
		return maxInt64
	}

	return b + level - 1
}

// Fits reports whether [lo, hi] lies within one level-aligned bucket.
func Fits(lo, hi, level int64) bool { return Of(lo, level) == Of(hi, level) }

// Finest returns the narrowest level whose bucket contains [lo, hi], reporting false for a
// straddler: a span crossing a top-level boundary.
func Finest(lo, hi int64) (int64, bool) {
	for _, level := range Ladder {
		if Fits(lo, hi, level) {
			return level, true
		}
	}

	return 0, false
}

// Union returns the union of the parts' spans.
func Union[P any](parts []P, span func(P) (lo, hi int64)) (lo, hi int64) {
	lo, hi = maxInt64, -maxInt64-1
	for _, p := range parts {
		plo, phi := span(p)
		lo, hi = min(lo, plo), max(hi, phi)
	}

	return lo, hi
}

// SplitsFrom reports whether merging parts from start on writes more than one top-level bucket.
func SplitsFrom[P any](parts []P, span func(P) (lo, hi int64), start int64) bool {
	lo, hi := Union(parts, span)

	return Of(max(lo, start), Top()) != Of(hi, Top())
}

// Straddlers returns the straddlers to split in one merge: oldest first, up to capBytes of their
// size (≤ 0 ⇒ unbounded) and maxParts of them (≤ 0 ⇒ unbounded), at least one, in src order.
//
// Merging straddlers together rather than one at a time is what makes a backlog converge: their
// rows land in the same few buckets, so one merge writes a part per bucket touched, not per
// straddler.
func Straddlers[P any](src []P, span func(P) (lo, hi int64), size func(P) int64, capBytes int64, maxParts int) []P {
	var idx []int

	for i, p := range src {
		if _, ok := Finest(span(p)); !ok {
			idx = append(idx, i)
		}
	}

	if len(idx) == 0 {
		return nil
	}

	slices.SortStableFunc(idx, func(a, b int) int {
		alo, _ := span(src[a])
		blo, _ := span(src[b])

		return cmp.Compare(alo, blo)
	})

	var total int64

	n := 0
	for ; n < len(idx); n++ {
		sz := size(src[idx[n]])
		if n > 0 && ((capBytes > 0 && total+sz > capBytes) || (maxParts > 0 && n >= maxParts)) {
			break
		}

		total += sz
	}

	picked := idx[:n]
	slices.Sort(picked)

	out := make([]P, len(picked))
	for i, j := range picked {
		out[i] = src[j]
	}

	return out
}
