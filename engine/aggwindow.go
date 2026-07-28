package engine

import (
	"context"
	"iter"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// WindowAgg is one evaluation step's range-vector aggregate: the count/sum/min/max of the samples
// in the half-open window (End-window, End], keyed by the evaluation timestamp End. Unlike
// [BucketAgg], consecutive windows overlap whenever the window is wider than the step, so a sample
// contributes to window/step of them.
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
// and subtracting the one that leaves — and, since an extremum cannot be subtracted back out,
// tracking min/max on monotonic deques. Advancing a step is O(1) regardless of how wide the window
// is.
//
// Windows align to the absolute grid (End is a multiple of step) and are returned sorted ascending
// by End; empty windows and series with no sample are omitted. The window is half-open on the left:
// a sample exactly at End-window is excluded, one exactly at End included.
//
// Only windows ending inside [r.Start, r.End] are returned, and each covers only the fetched data —
// so a caller wanting complete windows from its first evaluation point must fetch a window's worth
// of lead-in before it, as a PromQL engine does.
//
// window ≤ 0 means window = step (disjoint windows).
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

		ids, plan := e.planAggregate(r)
		defer plan.releaseParts()

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
	windowSlider

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

	return w.slide(w.ents, w.step, w.window, plan.end), nil
}

// bucketEntries rewrites fine left-open buckets — (Start, Start+step], ascending — as accumulator
// entries.
func bucketEntries(dst []windowEnt, fine []BucketAgg, step int64) []windowEnt {
	for i := range fine {
		b := &fine[i]
		dst = append(dst, windowEnt{end: b.Start + step, count: b.Count, sum: b.Sum, min: b.Min, max: b.Max})
	}

	return dst
}

// sampleEntries rewrites merged raw samples — ascending by timestamp — as accumulator entries, one
// each. The misaligned fallback: with a window edge free to fall inside a step, only per-sample
// membership is exact.
func sampleEntries(dst []windowEnt, ts []int64, vals []float64) []windowEnt {
	for i, t := range ts {
		v := vals[i]
		dst = append(dst, windowEnt{end: t, count: 1, sum: v, min: v, max: v})
	}

	return dst
}
