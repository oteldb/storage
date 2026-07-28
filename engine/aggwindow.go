package engine

import (
	"context"
	"iter"
	"math"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// WindowAgg is one evaluation step's range-vector aggregate: the count/sum of the samples in the
// half-open window (End-window, End], keyed by the evaluation timestamp End. Unlike [BucketAgg],
// consecutive windows overlap whenever the window is wider than the step, so a sample contributes
// to window/step of them.
//
// Min and Max are NaN: the sliding accumulator that keeps the overlapping case cheap can add and
// subtract a count and a sum, but not an extremum. Use Count, Sum and Sum/Count (count/sum/avg/
// present_over_time); min/max need a separate pass that is not implemented yet.
type WindowAgg struct {
	SeriesAgg

	End int64
}

// NamedWindowAgg pairs a series' identity with its evaluation windows — the labeled form of
// [Engine.AggregateWindow], for a caller that renders the result as a PromQL range vector.
type NamedWindowAgg struct {
	Series  signal.Series
	Windows []WindowAgg
}

// errWindowStep rejects a non-positive step: unlike [Engine.AggregateStep], where step ≤ 0 has the
// sensible meaning "one whole-range bucket", a stepped window has no evaluation grid without it.
var errWindowStep = errors.New("engine: aggregate window needs step > 0")

// AggregateWindow returns, per series, the aggregate of every step-aligned evaluation window
// (t-window, t] that holds a sample — the *overlapping* range-vector shape, `<fn>_over_time(m[W])`
// evaluated at a step finer than W. [Engine.AggregateStep] is the disjoint special case; this is the
// 12x-overlap one (a 1h range at a 5m step).
//
// The cost is proportional to the data in the request window, not to window/step times it: samples
// fold once into disjoint step-wide fine buckets (reusing the sidecar pushdown of
// [Engine.AggregateStep] — a part wholly inside one fine bucket never decodes), and a sliding
// accumulator then walks those buckets once per series, adding the bucket that enters each window
// and subtracting the one that leaves. Advancing a step is O(1) regardless of how wide the window is.
//
// Windows align to the absolute grid (End is a multiple of step) and are returned sorted ascending
// by End; empty windows and series with no sample are omitted. The window is half-open on the left:
// a sample exactly at End-window is excluded, one exactly at End included.
//
// Only windows ending inside [r.Start, r.End] are returned, and each covers only the fetched data —
// so a caller wanting complete windows from its first evaluation point must fetch a window's worth
// of lead-in before it, as a PromQL engine does.
//
// window ≤ 0 means window = step (disjoint windows). Min/Max are not computed; see [WindowAgg].
func (e *Engine) AggregateWindow(
	ctx context.Context, r fetch.Request, step, window int64,
) (map[signal.SeriesID][]WindowAgg, error) {
	out := map[signal.SeriesID][]WindowAgg{}

	for na, err := range e.aggregateWindowSeq(ctx, r, step, window) {
		if err != nil {
			return nil, err
		}

		out[na.Series.Hash()] = na.Windows
	}

	return out, nil
}

// AggregateWindowNamed is [Engine.AggregateWindow] returning each series' identity alongside its
// windows, for a caller that must re-check matchers or render labels (the cluster aggregate RPC).
func (e *Engine) AggregateWindowNamed(
	ctx context.Context, r fetch.Request, step, window int64,
) ([]NamedWindowAgg, error) {
	var out []NamedWindowAgg

	for na, err := range e.aggregateWindowSeq(ctx, r, step, window) {
		if err != nil {
			return nil, err
		}

		out = append(out, na)
	}

	return out, nil
}

// aggregateWindowSeq is the one implementation both exported forms drain: it yields each matching
// series' windows as they are computed, so only one series' worth of scratch is resident at a time
// (the point of the series-major shape — a window-major evaluation would hold series × steps
// results until the last window landed). It yields a non-nil error at most once and stops after.
//
// The fetch plan lives for the whole iteration and is released when it ends, including on an early
// break.
func (e *Engine) aggregateWindowSeq(
	ctx context.Context, r fetch.Request, step, window int64,
) iter.Seq2[NamedWindowAgg, error] {
	return func(yield func(NamedWindowAgg, error) bool) {
		if step <= 0 {
			yield(NamedWindowAgg{}, errWindowStep)

			return
		}

		e.mu.RLock()
		for !e.head.indexSorted() {
			e.mu.RUnlock()
			e.mu.Lock()
			e.head.ensureIndexSorted()
			e.mu.Unlock()
			e.mu.RLock()
		}

		ids := e.head.resolve(r.Matchers)
		plan := e.planFetch(ids, r)
		e.mu.RUnlock()

		defer plan.releaseParts()

		plan.acquireDecodeBudget(colNeed{values: true})

		w := newWindower(plan, step, window)

		for i, id := range ids {
			windows, err := w.series(ctx, e, plan, id)
			if err != nil {
				yield(NamedWindowAgg{}, err)

				return
			}

			if len(windows) == 0 {
				continue
			}

			if !yield(NamedWindowAgg{Series: plan.series[i], Windows: windows}, nil) {
				return
			}
		}
	}
}

// windower turns one series' samples into its overlapping evaluation windows. It is allocated once
// per aggregate call and reused across every series in it, so the scratch it needs is amortized.
//
// It has two paths. When the window is a whole multiple of the step, each window is exactly the
// union of window/step fine buckets and none of them is split by a window edge, so the fine grid —
// with its sidecar pushdown — is exact: that is the fast path. Otherwise a window edge can fall
// inside a bucket, and the call falls back to decoding and merging the series' samples and sliding
// over them individually; it is the same decode fallback the disjoint path already takes for a part
// that straddles a bucket boundary, and it stays exact at any window width.
type windower struct {
	step, window int64

	grid *stepGrid // fine step-wide buckets; nil ⇒ the misaligned per-sample fallback
	safe bool      // grid path: whether the plan's parts can be folded from their stats sidecars

	fine []BucketAgg // scratch: the current series' fine buckets
	ents []windowEnt // scratch: those buckets (or raw samples) as accumulator entries
	ts   []int64     // scratch: merged timestamps, fallback path only
	vals []float64   // scratch: merged values, fallback path only
}

func newWindower(plan *enginePlan, step, window int64) *windower {
	if window <= 0 {
		window = step
	}

	w := &windower{step: step, window: window}
	if window%step == 0 {
		w.grid, w.safe = newWindowGrid(plan, step), aggPushdownSafe(plan)
	}

	return w
}

// series returns id's non-empty evaluation windows, ascending by End. The returned slice is freshly
// allocated (it outlives the call); everything else is scratch reused across series.
func (w *windower) series(ctx context.Context, e *Engine, plan *enginePlan, id signal.SeriesID) ([]WindowAgg, error) {
	if w.grid != nil {
		if err := e.bucketSeries(ctx, plan, id, w.grid, w.safe); err != nil {
			return nil, err
		}

		w.fine = w.grid.collect(w.fine[:0])
		w.ents = bucketEntries(w.ents[:0], w.fine, w.step)
	} else {
		m, err := plan.mergeSeries(ctx, id)
		if err != nil {
			return nil, err
		}

		w.ts, w.vals, _ = m.collect(w.ts[:0], w.vals[:0])
		plan.releaseSeriesPins() // samples copied out; recirculate this series' block pins

		w.ents = sampleEntries(w.ents[:0], w.ts, w.vals)
	}

	return slideWindows(nil, w.ents, w.step, w.window, plan.end), nil
}

// windowEnt is one contribution to the sliding accumulator: an aggregate that enters a window when
// the evaluation timestamp reaches end, and leaves it once the window's open lower bound passes it.
// A fine bucket (b, b+step] enters at b+step; a single sample enters at its own timestamp.
type windowEnt struct {
	end   int64
	count int64
	sum   float64
}

// bucketEntries rewrites fine left-open buckets — (Start, Start+step], ascending — as accumulator
// entries.
func bucketEntries(dst []windowEnt, fine []BucketAgg, step int64) []windowEnt {
	for i := range fine {
		b := &fine[i]
		dst = append(dst, windowEnt{end: b.Start + step, count: b.Count, sum: b.Sum})
	}

	return dst
}

// sampleEntries rewrites merged raw samples — ascending by timestamp — as accumulator entries, one
// each. The misaligned fallback: with a window edge free to fall inside a step, only per-sample
// membership is exact.
func sampleEntries(dst []windowEnt, ts []int64, vals []float64) []windowEnt {
	for i, t := range ts {
		dst = append(dst, windowEnt{end: t, count: 1, sum: vals[i]})
	}

	return dst
}

// slideWindows folds ents — ascending by end — into one aggregate per step-aligned evaluation
// timestamp t whose window (t-window, t] is non-empty, appending them to dst in ascending order.
//
// An entry is in the window exactly when t-window < end ≤ t, so as t advances the running total
// gains the entries whose end it has reached and loses those the lower bound has passed: each entry
// is added once and subtracted once over the whole walk, making a step O(1) no matter how many
// windows overlap. Runs of empty windows are skipped rather than walked, so a sparse series costs
// its samples, not its span.
//
// end bounds the evaluation grid at the request's end: a window ending past it would be missing the
// data that follows, so it is not reported.
func slideWindows(dst []WindowAgg, ents []windowEnt, step, window, end int64) []WindowAgg {
	if len(ents) == 0 {
		return dst
	}

	// The evaluation timestamps that can see any entry: from the first step at or after the earliest
	// entry's end, through the last step that still holds the latest entry (t-window < end).
	lo := ceilStep(ents[0].end, step)

	last := ents[len(ents)-1].end
	horizon := last + window - 1
	if horizon < last { // window so wide it overflows; nothing beyond the end of time to evaluate at
		horizon = math.MaxInt64
	}

	hi := bucketStart(min(horizon, end), step)

	var (
		acc  SeriesAgg
		head int // entries already added
		tail int // entries already subtracted
	)

	for t := lo; t <= hi; t += step {
		for head < len(ents) && ents[head].end <= t {
			acc.Count += ents[head].count
			acc.Sum += ents[head].sum
			head++
		}

		for tail < head && ents[tail].end <= t-window {
			acc.Count -= ents[tail].count
			acc.Sum -= ents[tail].sum
			tail++
		}

		if acc.Count > 0 {
			dst = append(dst, WindowAgg{
				End:       t,
				SeriesAgg: SeriesAgg{Count: acc.Count, Sum: acc.Sum, Min: math.NaN(), Max: math.NaN()},
			})

			continue
		}

		acc.Sum = 0 // the window emptied: drop whatever rounding the add/subtract pairs left behind

		if head == len(ents) {
			break // every entry has expired and none is left to enter
		}

		if next := ceilStep(ents[head].end, step); next > t {
			t = next - step // jump the gap; the loop's post statement lands us on next
		}
	}

	return dst
}

// ceilStep rounds ts up to a multiple of step.
func ceilStep(ts, step int64) int64 {
	b := bucketStart(ts, step)
	if b == ts {
		return b
	}

	return b + step
}
