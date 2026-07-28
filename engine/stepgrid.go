package engine

import (
	"cmp"
	"math"
	"slices"
)

// maxDenseBuckets caps the dense step grid at ~8 MiB of scratch. A grid wider than this — a fine
// step over a long span — falls back to a map, which costs more per sample but is sized by the
// samples rather than by the grid.
const maxDenseBuckets = 1 << 18

// stepGrid accumulates one series' step-aligned buckets, then hands them over sorted. It is
// allocated once per aggregate call and reused across every series in it: a series' buckets are
// drained and the slots it touched are reset, so per-series cost is proportional to the buckets
// that series actually filled, not to the width of the grid.
//
// The common case is dense: bucket index is arithmetic on the timestamp, so filling costs no
// hashing and draining costs no comparator-driven sort of aggregate structs. The grid spans the
// plan's data (parts' time ranges ∪ the head span, clipped to the request), not the request itself,
// which is usually unbounded on one side. When even that is too wide to index densely, sparse
// holds the buckets instead and the two paths differ only inside this type.
type stepGrid struct {
	first int64 // timestamp of slot 0; 0 when step ≤ 0 (everything collapses to one bucket at 0)
	step  int64

	slots   []SeriesAgg // dense grid; nil ⇒ use sparse
	touched []int32     // indices of the non-empty slots of the series being accumulated

	sparse map[int64]SeriesAgg // bucket start → aggregate; nil ⇒ use slots

	// openLeft makes buckets (b, b+step] instead of [b, b+step). The window path needs it: a PromQL
	// range vector's window is half-open on the left, so only left-open fine buckets are never split
	// by a window edge.
	openLeft bool

	// phase shifts the grid off the absolute one: bucket edges sit at phase + k*step rather than at
	// multiples of step. Always in [0, step). An evaluation grid is anchored at the query's start,
	// which is rarely a multiple of the step, and a fine bucket must not straddle a window edge.
	phase int64
}

// newStepGrid sizes a grid for the plan's data span at the given step, with buckets [b, b+step).
// step ≤ 0 collapses to a single bucket at 0, matching [bucketStart].
func newStepGrid(plan *enginePlan, step int64) *stepGrid {
	return newBucketGrid(plan, step, false, 0)
}

// newWindowGrid is [newStepGrid] with left-open buckets (b, b+step] on the evaluation grid anchored
// at phase — the fine grid the sliding range-vector accumulator folds, so that a window (t-W, t] is
// exactly the union of W/step of them, none of which a window edge splits.
func newWindowGrid(plan *enginePlan, step, phase int64) *stepGrid {
	return newBucketGrid(plan, step, true, phase)
}

func newBucketGrid(plan *enginePlan, step int64, openLeft bool, phase int64) *stepGrid {
	if step <= 0 {
		return &stepGrid{slots: make([]SeriesAgg, 1)}
	}

	g := &stepGrid{step: step, openLeft: openLeft, phase: phase}

	lo, hi, ok := planDataSpan(plan)
	if !ok {
		g.slots = make([]SeriesAgg, 1) // nothing in window; nothing will be added

		return g
	}

	g.first = g.bucketOf(lo)
	n := (g.bucketOf(hi)-g.first)/step + 1

	if n > maxDenseBuckets {
		g.sparse = map[int64]SeriesAgg{}

		return g
	}

	g.slots = make([]SeriesAgg, n)

	return g
}

// bucketOf returns the key of the bucket holding ts: [b, b+step) normally, (b, b+step] on a
// left-open grid, with edges at phase + k*step.
func (g *stepGrid) bucketOf(ts int64) int64 {
	if g.openLeft && ts != math.MinInt64 {
		ts--
	}

	if g.phase == 0 {
		return bucketStart(ts, g.step)
	}

	return g.phase + bucketStart(ts-g.phase, g.step)
}

// addSample folds one sample into its bucket.
func (g *stepGrid) addSample(ts int64, v float64) {
	if g.sparse != nil {
		bs := g.bucketOf(ts)
		a := g.sparse[bs]
		a.addSample(v)
		g.sparse[bs] = a

		return
	}

	if a, ok := g.slot(ts); ok {
		empty := a.Count == 0
		a.addSample(v)

		if empty {
			g.touched = append(g.touched, g.indexOf(ts))
		}
	}
}

// mergeStat folds a whole part's precomputed aggregate into the bucket containing ts — the sidecar
// pushdown, used when the part lies wholly inside that bucket.
func (g *stepGrid) mergeStat(ts int64, st SeriesAgg) {
	if st.Count == 0 {
		return
	}

	if g.sparse != nil {
		bs := g.bucketOf(ts)
		a := g.sparse[bs]
		a.merge(st)
		g.sparse[bs] = a

		return
	}

	if a, ok := g.slot(ts); ok {
		empty := a.Count == 0
		a.merge(st)

		if empty {
			g.touched = append(g.touched, g.indexOf(ts))
		}
	}
}

// collect appends the current series' non-empty buckets to dst in ascending Start order and resets
// the grid for the next series.
func (g *stepGrid) collect(dst []BucketAgg) []BucketAgg {
	if g.sparse != nil {
		dst = growBuckets(dst, len(g.sparse))

		base := len(dst)
		for start, agg := range g.sparse {
			dst = append(dst, BucketAgg{Start: start, SeriesAgg: agg})

			delete(g.sparse, start)
		}

		slices.SortFunc(dst[base:], func(a, b BucketAgg) int { return cmp.Compare(a.Start, b.Start) })

		return dst
	}

	slices.Sort(g.touched) // small (one entry per non-empty bucket) and a plain int sort — no comparator

	dst = growBuckets(dst, len(g.touched))

	for _, i := range g.touched {
		a := &g.slots[i]
		dst = append(dst, BucketAgg{Start: g.first + int64(i)*g.step, SeriesAgg: *a})
		*a = SeriesAgg{}
	}

	g.touched = g.touched[:0]

	return dst
}

// growBuckets makes room for n more buckets in one allocation, so draining a series costs one
// growth rather than the handful an unsized append would take.
func growBuckets(dst []BucketAgg, n int) []BucketAgg {
	if cap(dst)-len(dst) >= n {
		return dst
	}

	return append(make([]BucketAgg, 0, len(dst)+n), dst...)
}

// indexOf maps a timestamp to its dense slot.
func (g *stepGrid) indexOf(ts int64) int32 {
	if g.step <= 0 {
		return 0
	}

	return int32((g.bucketOf(ts) - g.first) / g.step)
}

// slot returns the dense slot for ts. A timestamp outside the grid cannot occur — the grid spans
// every source's contribution to the request window — so it is dropped rather than resized into.
func (g *stepGrid) slot(ts int64) (*SeriesAgg, bool) {
	i := g.indexOf(ts)
	if i < 0 || int(i) >= len(g.slots) {
		return nil, false
	}

	return &g.slots[i], true
}

// planDataSpan returns the timestamp span the plan's sources can contribute within the request
// window: every live part's time range ∪ the head/mid-flush span, clipped to [plan.start,
// plan.end]. Sizing the step grid by this rather than by the request keeps it proportional to the
// data — a request is routinely unbounded on one side (`End: 1<<62`), the data never is.
func planDataSpan(plan *enginePlan) (lo, hi int64, ok bool) {
	for _, p := range plan.liveParts {
		if !ok {
			lo, hi, ok = p.minTime, p.maxTime, true

			continue
		}

		lo, hi = min(lo, p.minTime), max(hi, p.maxTime)
	}

	if hlo, hhi, has := planHeadSpan(plan); has {
		if !ok {
			lo, hi, ok = hlo, hhi, true
		} else {
			lo, hi = min(lo, hlo), max(hi, hhi)
		}
	}

	if !ok {
		return 0, 0, false
	}

	lo, hi = max(lo, plan.start), min(hi, plan.end)

	return lo, hi, lo <= hi
}
