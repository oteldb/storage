package timebucket

import (
	"cmp"
	"slices"
)

// Forced returns the forced rewrite for one merge, in src order. The oldest forced part picks the
// bucket; that bucket's forced parts are taken oldest first up to capBytes of their size (≤ 0 ⇒
// unbounded) and maxParts of them (≤ 0 ⇒ unbounded), at least one, and its parts neither forced nor
// sealed then fill what remains, smallest first. A forced straddler belongs to no bucket and goes
// alone.
func Forced[P any](
	src []P,
	span func(P) (lo, hi int64),
	size func(P) int64,
	forced, sealed func(P) bool,
	capBytes int64,
	maxParts int,
) []P {
	oldest := -1

	var oldestLo int64

	for i, p := range src {
		if !forced(p) {
			continue
		}

		if lo, _ := span(p); oldest < 0 || lo < oldestLo {
			oldest, oldestLo = i, lo
		}
	}

	if oldest < 0 {
		return nil
	}

	level, ok := Finest(span(src[oldest]))
	if !ok {
		return []P{src[oldest]}
	}

	bucket := Of(oldestLo, level)

	var due, rest []int

	for i, p := range src {
		lo, hi := span(p)

		switch {
		case !Fits(lo, hi, level) || Of(lo, level) != bucket:
		case forced(p):
			due = append(due, i)
		case !sealed(p):
			rest = append(rest, i)
		}
	}

	slices.SortStableFunc(due, func(a, b int) int {
		alo, _ := span(src[a])
		blo, _ := span(src[b])

		return cmp.Compare(alo, blo)
	})
	// Smallest first, so a cap cutoff strands the part that is cheapest to carry.
	slices.SortStableFunc(rest, func(a, b int) int { return cmp.Compare(size(src[a]), size(src[b])) })

	b := budget[P]{size: size, capBytes: capBytes, maxParts: maxParts}
	due = b.take(src, due)
	rest = b.take(src, rest)

	return pick(src, slices.Concat(due, rest))
}

// budget admits parts to one merge up to capBytes of their size and maxParts of them (≤ 0 ⇒
// unbounded), always admitting the first so a part over the cap still makes progress alone.
type budget[P any] struct {
	size     func(P) int64
	capBytes int64
	maxParts int

	total int64
	n     int
}

// take admits the longest prefix of idx that fits what remains.
func (b *budget[P]) take(src []P, idx []int) []int {
	k := 0
	for ; k < len(idx); k++ {
		sz := b.size(src[idx[k]])
		if b.n > 0 && ((b.capBytes > 0 && b.total+sz > b.capBytes) || (b.maxParts > 0 && b.n >= b.maxParts)) {
			break
		}

		b.total += sz
		b.n++
	}

	return idx[:k]
}

// pick returns src at idx, in src order.
func pick[P any](src []P, idx []int) []P {
	slices.Sort(idx)

	out := make([]P, len(idx))
	for i, j := range idx {
		out[i] = src[j]
	}

	return out
}
