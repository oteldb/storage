package recordengine

import (
	"cmp"
	"container/heap"
	"slices"

	"github.com/oteldb/storage/signal"
)

// A top-N request (fetch.Request.Limit with Reverse) only ever returns rows at one end of the time
// range, but a part scan in engine order has no way to know it is already past that end — so it
// decodes every matching row of every live part and throws almost all of them away in limitTrim.
//
// Reading the live parts in time order fixes that: newest-first for a Reverse request, oldest-first
// otherwise. Once Limit rows are collected, the Limit-th of them is a watermark — a part whose whole
// time range lies beyond it cannot contribute, and since the parts are time-ordered, neither can any
// part after it. The scan stops there.
//
// Two properties keep this exact rather than approximate:
//
//   - The comparison is strict, so a part that merely *touches* the watermark is still read. limitTrim
//     keeps boundary ties (its result is a correct superset), and skipping a tied part would drop
//     rows it is entitled to return.
//   - It applies only when the request carries no conditions. With conditions a part's contribution
//     is not known until its rows are filtered, so a collected-row count is an upper bound on
//     survivors and stopping on it could return fewer than Limit rows.

// orderPartsForLimit sorts the plan's live parts so the rows a top-N request wants come first:
// descending by maxTime when reverse (newest first), ascending by minTime otherwise. Ties break on
// the opposite bound, then on prefix, so the order is deterministic.
func (p *fetchPlan) orderPartsForLimit(reverse bool) {
	// leadKey is the part's leading bound in scan order (maxTime newest-first, minTime oldest-first),
	// negated when reverse so a plain ascending sort puts the wanted end first; tailKey is the other
	// bound, breaking ties in the same direction.
	leadKey := func(pt *part) int64 { return pt.minTime }
	tailKey := func(pt *part) int64 { return pt.maxTime }
	if reverse {
		leadKey = func(pt *part) int64 { return -pt.maxTime }
		tailKey = func(pt *part) int64 { return -pt.minTime }
	}

	slices.SortFunc(p.liveParts, func(a, b *part) int {
		if c := cmp.Compare(leadKey(a), leadKey(b)); c != 0 {
			return c
		}

		if c := cmp.Compare(tailKey(a), tailKey(b)); c != 0 {
			return c
		}

		return cmp.Compare(len(a.prefix), len(b.prefix))
	})
}

// beyondWatermark reports whether part's whole time range lies strictly past the watermark, so it
// cannot hold a row the top-N wants.
func beyondWatermark(pt *part, watermark int64, reverse bool) bool {
	if reverse {
		return pt.maxTime < watermark
	}

	return pt.minTime > watermark
}

// limitWatermark returns the limit-th newest (reverse) or oldest timestamp among the rows collected
// so far, and whether that many rows exist. It selects through a bounded heap, so the cost is
// O(rows · log limit) with no allocation beyond the heap itself.
func limitWatermark(accs map[signal.SeriesID]*recordCols, ids []signal.SeriesID, limit int, reverse bool) (int64, bool) {
	if limit <= 0 {
		return 0, false
	}

	h := &tsHeap{reverse: reverse, ts: make([]int64, 0, limit)}

	for _, id := range ids {
		acc := accs[id]
		if acc == nil {
			continue
		}

		for _, t := range acc.ts {
			switch {
			case len(h.ts) < limit:
				heap.Push(h, t)
			case h.better(t, h.ts[0]):
				h.ts[0] = t
				heap.Fix(h, 0)
			}
		}
	}

	if len(h.ts) < limit {
		return 0, false
	}

	return h.ts[0], true
}

// tsHeap keeps the limit best timestamps seen, with the *worst* of them at the root so a new
// candidate is compared and swapped in O(log limit). "Best" is newest when reverse, else oldest.
type tsHeap struct {
	ts      []int64
	reverse bool
}

func (h *tsHeap) Len() int           { return len(h.ts) }
func (h *tsHeap) Less(i, j int) bool { return h.better(h.ts[j], h.ts[i]) } // worst at the root
func (h *tsHeap) Swap(i, j int)      { h.ts[i], h.ts[j] = h.ts[j], h.ts[i] }
func (h *tsHeap) Push(x any)         { v, _ := x.(int64); h.ts = append(h.ts, v) }
func (h *tsHeap) Pop() any           { v := h.ts[len(h.ts)-1]; h.ts = h.ts[:len(h.ts)-1]; return v }

// better reports whether a ranks ahead of b for this request's direction.
func (h *tsHeap) better(a, b int64) bool {
	if h.reverse {
		return a > b
	}

	return a < b
}
