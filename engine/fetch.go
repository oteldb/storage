package engine

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/go-faster/sdk/zctx"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/query/profile"
)

// Fetch implements [fetch.Fetcher] over the head ∪ flushed parts: it resolves the
// request's matchers to series (the index spans every series ever seen, flushed or not)
// and yields one batch per series with its samples in the window, merged across the head
// buffer and every part by timestamp.
//
// The iterator is **streaming**: a series' samples are gathered on the [fetch.Iterator.Next] that
// yields them, so a consumer that folds and releases each batch stays O(1) in matched series
// instead of O(matched series x samples). The acquired parts, the decode-memory reservation and
// the profiling/observability accounting therefore live for the whole iteration and are settled by
// [fetch.Iterator.Close] — a caller **must** Close (directly or via [fetch.Drain]), otherwise the
// parts stay pinned and the decode budget stays reserved.
func (e *Engine) Fetch(ctx context.Context, r fetch.Request) (fetch.Iterator, error) {
	return e.fetch(ctx, r, false)
}

// TryFetch implements [fetch.TryFetcher].
func (e *Engine) TryFetch(ctx context.Context, r fetch.Request) (fetch.Iterator, error) {
	return e.fetch(ctx, r, true)
}

func (e *Engine) fetch(ctx context.Context, r fetch.Request, try bool) (fetch.Iterator, error) {
	if try {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
	ctx = e.cfg.Obs.Base(ctx)
	ctx, span := e.cfg.Obs.Tracer.Start(ctx, "engine.fetch",
		trace.WithAttributes(attribute.String("storage.prefix", e.cfg.Prefix)))

	ctx, pf := profile.Begin(ctx, "engine.fetch")

	startNs := time.Now()
	log := zctx.From(ctx)
	log.Debug("fetch start",
		zap.String("prefix", e.cfg.Prefix), zap.Int("matchers", len(r.Matchers)),
		zap.Int64("start", r.Start), zap.Int64("end", r.End))

	// The label index sorts lazily on first read after a write, mutating in place. Reads run
	// under the shared lock (concurrent fetches are allowed — e.g. split-by-interval), so do
	// that one-time sort under the exclusive lock first: hold the read lock, and while the
	// index is still unsorted, upgrade to sort and re-check. Once we hold the read lock with a
	// sorted index, no writer can be running, so resolve only reads.
	var plan *enginePlan
	if e.cfg.DiskSeries {
		var err error
		plan, err = e.planDiskQuery(ctx, r, true, false)
		if err != nil {
			span.RecordError(err)
			span.End()
			pf.End()
			return nil, err
		}
	} else {
		e.mu.RLock()
		for !e.head.indexSorted() {
			e.mu.RUnlock()
			e.mu.LockState()
			e.head.ensureIndexSorted()
			e.mu.UnlockState()
			e.mu.RLock()
		}

		_, rpf := profile.Begin(ctx, "resolve-matchers")
		ids := e.head.resolve(r.Matchers)
		rpf.Add("series_matched", int64(len(ids)))
		rpf.End()

		// Plan under the read lock: acquire the in-window parts to read and snapshot each series' head
		// (and any mid-flush) samples + identity, so the part reads below run lock-free — appends and
		// flush/merge proceed concurrently, and the acquired parts can't be reclaimed until released.
		plan = e.planFetch(ids, r)
		plan.tryDecode = try
		e.mu.RUnlock()
	}
	plan.tryDecode = try

	// Reserve this fetch's decode footprint from the memory budget (blocks under concurrency
	// pressure), off the engine lock, before any part is decoded. A fetch materializes ts+value.
	if err := plan.acquireDecodeBudget(ctx, r, colNeed{values: true}); err != nil {
		plan.releaseParts()
		span.RecordError(err)
		span.End()
		pf.End()

		return nil, err
	}

	// Prefetch: decode the parts this fetch will touch concurrently (and cache them), so their
	// backend reads + decodes overlap instead of happening one part at a time during the merge.
	e.prefetch(ctx, plan)

	_, spf := profile.Begin(ctx, "scan")

	return &fetchIter{
		e:       e,
		plan:    plan,
		recycle: r.Recycle,
		ctx:     ctx,
		span:    span,
		pf:      pf,
		spf:     spf,
		log:     log,
		startNs: startNs,
		// Parts actually scanned this fetch — the recent tier may have short-circuited acquisition.
		partsScanned: len(plan.liveParts),
	}, nil
}
