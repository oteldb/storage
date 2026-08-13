package engine

import (
	"context"
	"iter"
	"time"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

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

// WindowSpec is the evaluation grid of an overlapping range-vector aggregate.
type WindowSpec struct {
	// Step is the distance between evaluation timestamps. Must be > 0.
	Step int64

	// Window is the width of each evaluation window, (t-Window, t]. A value ≤ 0 means Step: one
	// aggregate per step over disjoint windows, no overlap.
	Window int64

	// Anchor is any timestamp on the evaluation grid — windows end at Anchor + k*Step. The zero
	// value anchors on the absolute grid (multiples of Step). A PromQL range query's grid is
	// anchored at the query's start, which is only a multiple of the step by coincidence, so an
	// embedder must pass it: the answer is otherwise computed at timestamps nobody asked about.
	Anchor int64
}

// window returns the effective window width (Step when unset).
func (w WindowSpec) window() int64 {
	if w.Window <= 0 {
		return w.Step
	}

	return w.Window
}

// phase returns the grid's offset from the absolute one, in [0, Step).
func (w WindowSpec) phase() int64 {
	p := w.Anchor % w.Step
	if p < 0 {
		p += w.Step
	}

	return p
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
	ctx context.Context, r fetch.Request, spec WindowSpec,
) (map[signal.SeriesID][]WindowAgg, error) {
	out := map[signal.SeriesID][]WindowAgg{}

	for na, err := range e.aggregateWindowSeq(ctx, r, spec) {
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
	ctx context.Context, r fetch.Request, spec WindowSpec,
) ([]NamedWindowAgg, error) {
	var out []NamedWindowAgg

	for na, err := range e.aggregateWindowSeq(ctx, r, spec) {
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
	ctx context.Context, r fetch.Request, spec WindowSpec,
) iter.Seq2[NamedWindowAgg, error] {
	return func(yield func(NamedWindowAgg, error) bool) {
		// The span covers the whole iteration rather than just the plan: the per-series work is
		// where the time goes, and it only runs as the caller drains the sequence. Ending it here
		// also covers an early break, which is when the caller stops mid-iteration.
		ctx, span := e.startAggregateSpan(ctx, "engine.aggregateWindow")

		// The result counters are only final once iteration stops — which happens by running out
		// of series, by the caller breaking, or by an error — so they are recorded in one place on
		// the way out rather than at each of those exits.
		var (
			windowsEmitted, seriesEmitted int
			plan                          *enginePlan
			w                             *windower
		)

		stoppedEarly := false

		defer func() {
			span.SetAttributes(
				attribute.Int("storage.series_emitted", seriesEmitted),
				attribute.Int("storage.windows_emitted", windowsEmitted),
				attribute.Bool("storage.stopped_early", stoppedEarly),
			)

			if plan != nil {
				span.SetAttributes(samplesDecodedAttr(plan))
			}

			if w != nil && w.timed {
				span.SetAttributes(
					attribute.Float64("storage.decode_duration_ms", w.decodeDur.Seconds()*1000),
					attribute.Float64("storage.fold_duration_ms", w.foldDur.Seconds()*1000),
				)
			}

			span.End()
		}()

		span.SetAttributes(
			attribute.Int64("storage.step", spec.Step),
			attribute.Int64("storage.window", spec.window()),
		)

		if spec.Step <= 0 {
			span.RecordError(errWindowStep)
			yield(NamedWindowAgg{}, errWindowStep)

			return
		}

		// Planning is the one phase of this call that is contiguous, so it is the one that gets a
		// child span. It is opened only when the parent records: an untraced read must not pay even
		// a no-op span's allocation.
		var planSpan trace.Span
		if span.IsRecording() {
			_, planSpan = e.cfg.Obs.Tracer.Start(ctx, "engine.aggregateWindow.plan")
		}

		ids, plan := e.planAggregate(r)
		w = newWindower(plan, spec)

		if planSpan != nil {
			planSpan.End()
		}

		defer plan.releaseParts()

		// Which path this call takes is the first thing to know when it is slow: the grid path
		// folds each series from step-wide buckets (and, when safe, straight from the parts' stats
		// sidecars without decoding a sample), while a window that is not a whole multiple of the
		// step falls back to decoding and merging every sample. Recording it as an attribute means
		// a slow call says *why* rather than only how long.
		span.SetAttributes(append(
			aggregatePlanAttrs(ids, plan, w.push),
			attribute.Bool("storage.window_grid", w.grid != nil),
		)...)

		// The per-series work is split into decode and fold, but as accumulated durations rather
		// than sub-spans: the two phases interleave once per series (this is a streaming, series-
		// major iteration), so a pair of contiguous child spans could not describe them without
		// lying about when each ran, and a span per series or per part would put thousands of spans
		// in a trace of one query. Planning is the one contiguous phase, so it does get a child
		// span. Timing is read only when the span records, so an untraced call takes no clock reads.
		w.timed = span.IsRecording()

		for i, id := range ids {
			windows, err := w.series(ctx, e, plan, id)
			if err != nil {
				span.RecordError(err)
				yield(NamedWindowAgg{}, err)

				return
			}

			if len(windows) == 0 {
				continue
			}

			windowsEmitted += len(windows)
			seriesEmitted++

			if !yield(NamedWindowAgg{Series: plan.series[i], Windows: windows}, nil) {
				stoppedEarly = true

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

	step, window, phase int64

	grid *stepGrid        // fine step-wide buckets; nil ⇒ the misaligned per-sample fallback
	push pushdownDecision // why the parts can (or cannot) be folded from their stats sidecars
	safe bool             // grid path: whether that pushdown applies

	fine []BucketAgg // scratch: the current series' fine buckets
	ents []windowEnt // scratch: those buckets (or raw samples) as accumulator entries
	ts   []int64     // scratch: merged timestamps, fallback path only
	vals []float64   // scratch: merged values, fallback path only

	timed              bool // read the clock around each series' phases (only when the span records)
	decodeDur, foldDur time.Duration
}

func newWindower(plan *enginePlan, spec WindowSpec) *windower {
	w := &windower{step: spec.Step, window: spec.window(), phase: spec.phase()}
	if w.window%w.step != 0 {
		// No fine grid at all: a window edge can fall inside a bucket, so nothing is foldable from
		// the sidecars regardless of how the parts are laid out.
		w.push = pushdownDecision{reason: pushdownGridUnusable}

		return w
	}

	w.grid = newWindowGrid(plan, w.step, w.phase)
	w.push = aggPushdownCheck(plan)
	w.safe = w.push.safe()

	return w
}

// mark returns the current time when timing is on, and the zero time otherwise.
func (w *windower) mark() time.Time {
	if !w.timed {
		return time.Time{}
	}

	return time.Now()
}

// add charges the time since from to dst and returns the new mark.
func (w *windower) add(dst *time.Duration, from time.Time) time.Time {
	if !w.timed {
		return time.Time{}
	}

	now := time.Now()
	*dst += now.Sub(from)

	return now
}

// series returns id's non-empty evaluation windows, ascending by End. The returned slice is freshly
// allocated (it outlives the call); everything else is scratch reused across series.
func (w *windower) series(ctx context.Context, e *Engine, plan *enginePlan, id signal.SeriesID) ([]WindowAgg, error) {
	started := w.mark()

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
		plan.samplesDecoded += len(w.ts)

		w.ents = sampleEntries(w.ents[:0], w.ts, w.vals)
	}

	decoded := w.add(&w.decodeDur, started)

	out := w.slide(w.ents, w.step, w.window, w.phase, plan.end)
	w.add(&w.foldDur, decoded)

	return out, nil
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
