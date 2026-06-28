package engine

import (
	"context"
	"slices"
	"time"

	"github.com/go-faster/sdk/zctx"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/oteldb/storage/signal"
)

// Merge compacts every flushed part into a single new part, dropping samples older than
// retainFrom (retention; retainFrom ≤ 0 disables it). It is a no-op when there is nothing
// to gain — fewer than two parts and no retention cutoff. Source parts are deleted from
// the backend after the new part is durably written.
//
// Retention is expressed as an absolute timestamp (unix nanoseconds), so the engine stays
// free of wall-clock dependencies; the caller derives it from the tenant policy. For
// downsampling, use [Engine.MergeWith].
func (e *Engine) Merge(ctx context.Context, retainFrom int64) error {
	return e.MergeWith(ctx, MergeOptions{RetainFrom: retainFrom})
}

// MergeWith compacts every flushed part into a single new part, applying retention and
// downsampling per opts. It is the one background-merge entry point; compaction, retention,
// and downsampling are the same pass over the immutable parts (no separate subsystem).
func (e *Engine) MergeWith(ctx context.Context, opts MergeOptions) error {
	ctx = e.cfg.Obs.Base(ctx)
	ctx, span := e.cfg.Obs.Tracer.Start(ctx, "engine.merge",
		trace.WithAttributes(
			attribute.String("storage.prefix", e.cfg.Prefix),
			attribute.Bool("storage.merge.downsample", len(opts.Downsample) > 0),
			attribute.Bool("storage.merge.recompress", opts.Recompress != nil),
		))
	defer span.End()

	e.mergeRunning.Store(true)
	defer e.mergeRunning.Store(false)

	startNs := time.Now()
	log := zctx.From(ctx)
	log.Debug("merge requested",
		zap.String("prefix", e.cfg.Prefix),
		zap.Bool("downsample", len(opts.Downsample) > 0),
		zap.Bool("recompress", opts.Recompress != nil))

	compacted, err := e.merge(ctx, opts)
	if err != nil {
		span.RecordError(err)
		log.Error("merge failed", zap.String("prefix", e.cfg.Prefix), zap.Error(err))

		return err
	}

	if compacted > 0 {
		span.SetAttributes(attribute.Int("storage.merge.parts_in", compacted))
		e.cfg.Obs.Merge.Record(ctx, metricSignal, time.Since(startNs), int64(compacted))
		log.Debug("merged parts",
			zap.String("prefix", e.cfg.Prefix), zap.Int("parts_in", compacted),
			zap.Bool("downsample", len(opts.Downsample) > 0),
			zap.Bool("recompress", opts.Recompress != nil),
			zap.Duration("took", time.Since(startNs)))
	} else {
		log.Debug("merge no-op (nothing to compact)", zap.String("prefix", e.cfg.Prefix))
	}

	return nil
}

// mergeLocked compacts the parts per opts and returns the number of source parts compacted (0 ⇒ a
// no-op). The caller holds e.mu.
func (e *Engine) merge(ctx context.Context, opts MergeOptions) (int, error) {
	// Plan (under lock): snapshot the source parts (immutable backing) and reserve the sequence.
	e.mu.Lock()
	src := e.parts
	seq := e.nextSeq
	e.mu.Unlock()

	// A single part with no retention cutoff, nothing old enough to downsample, and nothing to
	// recompress has nothing to gain — skip without decoding it.
	if len(src) == 0 || (len(src) == 1 && opts.RetainFrom <= 0 &&
		!downsampleApplies(opts.Downsample, src[0].minTime) && !recompressApplies(src[0], opts.Recompress) &&
		!precisionApplies(src[0], opts.Precision)) {
		e.reclaimRetired(ctx)

		return 0, nil
	}

	start := minInt64
	if opts.RetainFrom > 0 {
		start = opts.RetainFrom
	}

	// Build (lock-free): read+compact the source parts, downsample, write the merged part, read it
	// back. The source parts stay live (not retired) until publish, so they can't be reclaimed here.
	cols, err := e.compactParts(ctx, src, start, opts.Downsample)
	if err != nil {
		return 0, err
	}

	// Fixed point: a single source part whose row count downsampling did not reduce, and which
	// needs neither recompression nor precision coarsening, is already at its target — rewriting it
	// would only churn the backend.
	if len(src) == 1 && opts.RetainFrom <= 0 && len(cols.ts) == src[0].rows() &&
		!recompressApplies(src[0], opts.Recompress) && !precisionApplies(src[0], opts.Precision) {
		e.reclaimRetired(ctx)

		return 0, nil
	}

	// Split the merged columns into one or more parts, each under MaxPartBytes (one when unlimited).
	// Each part's cold-tier recompression/precision is decided from its own newest sample.
	var newParts []*part

	if len(cols.ts) > 0 {
		ranges := chunkRanges(len(cols.ts), maxRowsPerPart(e.cfg.MaxPartBytes))
		newParts = make([]*part, 0, len(ranges))

		for i, rg := range ranges {
			sub := cols.slice(rg[0], rg[1])
			minT, maxT := colsTimeRange(sub)
			prefix := e.partPrefix(seq + i)

			if err := writePart(ctx, e.cfg.Backend, prefix, sub,
				coldProfile(opts.Recompress, maxT), pickPrecision(opts.Precision, maxT), e.cfg.AggregateStats); err != nil {
				return 0, err
			}

			p, err := openPart(ctx, e.cfg.Backend, prefix)
			if err != nil {
				return 0, err
			}

			p.minTime, p.maxTime = minT, maxT
			newParts = append(newParts, p)
		}
	}

	// Publish (under lock): swap the source parts for the merged one(s) copy-on-write (keeping any part
	// a concurrent flush may have added), retire the sources, and commit the index before any deletion —
	// so a crash mid-merge never leaves the index referencing a deleted part. The retired parts' objects
	// are deleted by reclaimRetired once their readers drain.
	removed := make(map[string]struct{}, len(src))
	for _, p := range src {
		removed[p.prefix] = struct{}{}
	}

	e.mu.Lock()
	e.parts = replaceParts(e.parts, removed, newParts...)
	e.nextSeq = seq + len(newParts)

	e.retireLocked(src)
	err = e.updateIndexLocked(ctx)
	e.mu.Unlock()

	if err != nil {
		return len(src), err
	}

	e.reclaimRetired(ctx)

	return len(src), nil
}

// compactParts merges each source part's samples per series (within [start, maxInt64], so
// retention is applied), then downsamples the survivors per tiers, returning the combined
// columns sorted by (series, ts). The returned columns are empty when no sample survives. It reads
// the parts off the engine lock; src is the immutable snapshot the caller planned over.
func (e *Engine) compactParts(ctx context.Context, src []*part, start int64, tiers []DownsampleTier) (*flushColumns, error) {
	idSet := make(map[signal.SeriesID]struct{})
	for _, p := range src {
		for id := range p.ranges {
			idSet[id] = struct{}{}
		}
	}

	ids := make([]signal.SeriesID, 0, len(idSet))
	for id := range idSet {
		ids = append(ids, id)
	}

	slices.SortFunc(ids, func(a, b signal.SeriesID) int { return a.Compare(b) })

	cols := &flushColumns{}

	// Decode each source part once and reuse across all its series (compaction reads every
	// series of every part), instead of re-decoding the whole part per series.
	decoded := make(partDecodeCache, len(src))

	for _, id := range ids {
		var m sampleMerge

		// Oldest → newest part, so a later part's value wins on a duplicate timestamp.
		for _, p := range src {
			rng, ok := p.ranges[id]
			if !ok {
				continue
			}

			d, err := decoded.get(ctx, p, decodePart)
			if err != nil {
				return nil, err
			}

			d.mergeSeriesInto(rng, &m, start, maxInt64)
		}

		ts, values, sf := m.collect(nil, nil)
		ts, values, sf = downsample(ts, values, sf, tiers)

		u := idToU128(id)
		for i := range ts {
			w := float64(1)
			if sf != nil {
				w = sf[i]
			}

			cols.appendRow(u, ts[i], values[i], w)
		}
	}

	return cols, nil
}

// Close flushes any buffered samples to a part and closes the WAL. It does not stop a background
// loop — the owner ([storage.Storage]) does that before calling Close.
func (e *Engine) Close(ctx context.Context) error {
	if _, err := e.flush(ctx); err != nil {
		return err
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.cfg.WAL != nil {
		return e.cfg.WAL.Close()
	}

	return nil
}

// CloseWAL closes the engine's open WAL segment file handle without flushing the head or
// checkpointing — modeling a process crash, where the OS reclaims open descriptors but the on-disk
// WAL segments survive for replay. The head is left as-is (and lost, as a crash would lose it). A
// crash-recovery test uses this to release the file handle so the WAL directory can be removed even
// on platforms that refuse to delete a file held open by a live process (Windows). No-op without a
// WAL.
func (e *Engine) CloseWAL() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.cfg.WAL != nil {
		return e.cfg.WAL.Close()
	}

	return nil
}

// SyncWAL fsyncs the engine's WAL, if any (the background WALSyncInterval path). No-op without a WAL.
func (e *Engine) SyncWAL() error {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.cfg.WAL != nil {
		return e.cfg.WAL.Sync()
	}

	return nil
}
