package engine

import (
	"context"
	"io"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/query/profile"
)

// fetchIter is [Engine.Fetch]'s streaming iterator: it walks the plan's matched series one per
// Next, gathering that series' samples (the part reads and the head/flush merge) only when the
// consumer asks for it. So a consumer that folds and releases each batch holds one series' samples
// at a time rather than the whole matched set — what remains proportional to the matched-series
// count is the plan itself (one identity and one head snapshot per series, both taken under the
// engine lock) and a part's whole-column decode.
//
// Close settles everything the iteration held: the acquired parts, the decode-memory reservation,
// the pooled plan buffers, and the fetch's span/profile/metric accounting (which counts what was
// actually consumed — a consumer that stops early records fewer rows). It is idempotent.
type fetchIter struct {
	e    *Engine
	plan *enginePlan
	i    int

	recycle bool // Request.Recycle: hand out pooled result buffers with a release hook

	// ctx is the *fetch's* context, not Next's: it carries the span and profile node this fetch
	// opened, so each Next's part reads stay attached to that tree (Next's own ctx is consulted for
	// cancellation only). It has the lifetime of the iterator, which the caller ends with Close.
	ctx     context.Context //nolint:containedctx // the fetch's span/profile scope spans the iteration
	span    trace.Span
	pf, spf *profile.Handle
	log     *zap.Logger
	startNs time.Time

	partsScanned int
	rows         int
	closed       bool
}

// Next gathers and returns the next matched series' batch, skipping series with no sample in the
// window. It returns (nil, io.EOF) once every matched series has been visited.
func (it *fetchIter) Next(ctx context.Context) (*fetch.Batch, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	p := it.plan

	for it.i < len(p.ids) {
		i := it.i
		it.i++

		id := p.ids[i]

		//nolint:contextcheck // deliberate: the read belongs to the fetch's span/profile, not Next's ctx
		m, err := p.mergeSeries(it.ctx, id)
		if err != nil {
			it.span.RecordError(err)

			return nil, err
		}

		// Buffer pooling is opt-in (Request.Recycle): only then do we touch the pool or set the
		// release hook, so the default path is exactly as before — no sync.Pool.Get overhead.
		var tsBuf []int64

		var valBuf []float64

		if it.recycle {
			tsBuf, valBuf = it.e.getI64(), it.e.getF64()
		}

		ts, values, sf := m.collect(tsBuf, valBuf)

		// The series' samples are copied out; drop this round's block pins so evicted blocks'
		// buffers recirculate to later decodes of this same fetch (and concurrent ones).
		p.releaseSeriesPins()

		if len(ts) == 0 {
			if it.recycle {
				it.e.putI64(ts)
				it.e.putF64(values)
			}

			continue
		}

		b := &fetch.Batch{ID: id, Series: p.series[i], Timestamps: ts, Values: values, ScaleFactors: sf}
		if it.recycle {
			b.SetRelease(it.e.recycle) // caller will Release to recycle the ts/value buffers
		}

		it.rows += len(ts)

		return b, nil
	}

	return nil, io.EOF
}

// Close releases the fetch's parts and decode budget and records its observability. Idempotent —
// releasing twice would double-return pooled buffers, so a second call is a no-op.
func (it *fetchIter) Close() error {
	if it.closed {
		return nil
	}

	it.closed = true

	it.plan.releaseParts()

	matched := len(it.plan.ids)

	it.spf.Add("parts_scanned", int64(it.partsScanned))
	it.spf.Add("rows", int64(it.rows))
	it.spf.End()

	it.pf.Add("series_matched", int64(matched))
	it.pf.Add("parts_scanned", int64(it.partsScanned))
	it.pf.Add("rows", int64(it.rows))
	it.pf.End()

	it.span.SetAttributes(
		attribute.Int("storage.series_matched", matched),
		attribute.Int("storage.parts_scanned", it.partsScanned),
		attribute.Int("storage.rows", it.rows),
	)

	// Record while the span is still live, so a metric exemplar can carry it, then end it.
	it.e.cfg.Obs.Fetch.Record(it.ctx, metricSignal, time.Since(it.startNs),
		int64(matched), int64(it.partsScanned), int64(it.rows))
	it.span.End()
	it.log.Debug("fetch done",
		zap.String("prefix", it.e.cfg.Prefix), zap.Int("series_matched", matched),
		zap.Int("parts_scanned", it.partsScanned), zap.Int("rows", it.rows),
		zap.Duration("took", time.Since(it.startNs)))

	return nil
}
