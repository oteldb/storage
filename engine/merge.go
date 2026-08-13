package engine

import (
	"context"
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

// merge compacts a bounded, size-tiered group of the engine's parts per opts and returns the number
// of source parts compacted (0 ⇒ a no-op). It does not re-read the whole part set: [selectMergeParts]
// picks only the parts worth merging this cycle (a same-size tier group plus any part a forced
// rewrite must touch), so a single merge's working set is O(part size), not O(dataset).
func (e *Engine) merge(ctx context.Context, opts MergeOptions) (int, error) {
	e.flushMu.Lock()
	defer e.flushMu.Unlock()

	// Plan (under lock): snapshot the source parts (immutable backing). Output part sequences are
	// reserved one at a time, as the parts are written.
	e.mu.Lock()
	src := e.parts
	e.mu.Unlock()

	capBytes := e.mergeCapBytes(ctx)

	selected := selectMergeParts(src, opts, capBytes, e.idleMerges)
	if len(selected) == 0 {
		e.idleMerges++

		// A no-op is indistinguishable from a healthy engine without the shape of what it looked
		// at: 59 parts sat uncompacted for hours logging only "nothing to compact". These are the
		// exact inputs to that decision.
		sealedN, eligible, bestM := mergeShape(src, capBytes)
		zctx.From(ctx).Debug("merge selected nothing",
			zap.String("prefix", e.cfg.Prefix), zap.Int("parts", len(src)),
			zap.Int("sealed", sealedN), zap.Int64("cap_bytes", capBytes),
			zap.Int("eligible", eligible), zap.Float64("best_multiplier", bestM),
			zap.Float64("min_multiplier", minMergeMultiplier),
			zap.Int("idle_rounds", e.idleMerges), zap.Int("waive_after", mergeIdleRounds))

		e.reclaimRetired(ctx)

		return 0, nil
	}

	e.idleMerges = 0

	start := minInt64
	if opts.RetainFrom > 0 {
		start = opts.RetainFrom
	}

	// Build (lock-free): compact the selected parts into new output part(s), reading them back. The
	// source parts stay live (not retired) until publish, so they can't be reclaimed here.
	var (
		newParts []*part
		err      error
	)

	if len(selected) == 1 {
		// A single forced part: decode it (bounded — one part), apply retention/downsample, and skip
		// the rewrite if it is already at its target (the fixed point), avoiding backend churn.
		var cols *flushColumns
		if cols, err = e.compactParts(ctx, selected, start, opts.Downsample); err != nil {
			return 0, err
		}

		p := selected[0]
		if opts.RetainFrom <= 0 && len(cols.ts) == p.rows() &&
			!recompressApplies(p, opts.Recompress) && !precisionApplies(p, opts.Precision) {
			e.reclaimRetired(ctx)

			return 0, nil
		}

		if newParts, err = e.writeColumns(ctx, cols, rowCapFor(p, capBytes), opts); err != nil {
			return 0, err
		}
	} else if newParts, err = e.compactStream(ctx, selected, start, capBytes, opts); err != nil {
		return 0, err
	}

	// Publish (under lock): swap the selected parts for the merged one(s) copy-on-write (keeping every
	// part not selected — including any a concurrent flush may have added) and commit the index. The
	// sources are retired — queued for backend deletion — only once that commit succeeds: the persisted
	// index is what a restart and every other replica read, so a part it still names must never become
	// reclaimable. A failed commit rolls the swap back to the committed set, leaving the merge output as
	// orphan objects the next [Engine.LoadParts] sweeps. The retired parts' objects are deleted by
	// reclaimRetired once their readers drain.
	removed := make(map[string]struct{}, len(selected))
	for _, p := range selected {
		removed[p.prefix] = struct{}{}
	}

	e.mu.Lock()
	committed := e.parts
	e.parts = replaceParts(e.parts, removed, newParts...)

	if err = e.updateIndexLocked(ctx); err != nil {
		e.parts = committed
		e.mu.Unlock()

		return len(selected), err
	}

	e.retireLocked(selected)
	// Rows that did not survive the merge are retention's work: the samples are gone, so the
	// identities naming them may be dead too and an identity prune has something to find. Merging
	// without dropping rows (a plain compaction) leaves every identity backed, so it arms nothing.
	if partRows(newParts) < partRows(selected) {
		e.identityDirty = true
	}

	e.mu.Unlock()

	e.reclaimRetired(ctx)

	return len(selected), nil
}

// writeColumns splits cols into one or more output parts, each kept under capRows (a single part
// when capRows ≤ 0), writes each, and reads it back. Used for the single-part merge path; the
// multi-part path streams (see compactStream). Returns nil when cols is empty (e.g. retention
// dropped every sample).
func (e *Engine) writeColumns(ctx context.Context, cols *flushColumns, capRows int, opts MergeOptions) ([]*part, error) {
	if len(cols.ts) == 0 {
		return nil, nil
	}

	ranges := chunkRanges(len(cols.ts), capRows)
	newParts := make([]*part, 0, len(ranges))

	for _, rg := range ranges {
		sub := cols.slice(rg[0], rg[1])

		p, err := e.writeMergedPart(ctx, sub, e.reserveSeq(), opts)
		if err != nil {
			return nil, err
		}

		newParts = append(newParts, p)
	}

	return newParts, nil
}

// writeMergedPart writes cols as the seq-th output part with the compression its size and age select
// ([mergeProfile]) and the precision its own newest sample selects, reads it back, and stamps its
// time bounds.
func (e *Engine) writeMergedPart(ctx context.Context, cols *flushColumns, seq int, opts MergeOptions) (*part, error) {
	minT, maxT := colsTimeRange(cols)
	prefix := e.partPrefix(seq)
	// The merged part's identities come from the resident index, which spans every live series —
	// snapshotted here (a brief read lock, off the flush path) because the write itself is off-lock.
	idents := e.identitiesForColumn(cols.series)

	if err := writePart(ctx, e.cfg.Backend, prefix, cols, idents,
		mergeProfile(opts.Recompress, maxT, len(cols.ts)), pickPrecision(opts.Precision, maxT),
		e.cfg.AggregateStats, e.cfg.MetricBlockRows); err != nil {
		return nil, err
	}

	p, err := openPart(ctx, e.cfg.Backend, prefix)
	if err != nil {
		return nil, err
	}

	p.minTime, p.maxTime = minT, maxT

	return p, nil
}

// compactParts merges each source part's samples per series (within [start, maxInt64], so
// retention is applied), then downsamples the survivors per tiers, returning the combined
// columns sorted by (series, ts). The returned columns are empty when no sample survives. It reads
// the parts off the engine lock; src is the immutable snapshot the caller planned over.
func (e *Engine) compactParts(ctx context.Context, src []*part, start int64, tiers []DownsampleTier) (*flushColumns, error) {
	ids, err := sortedSeriesIDs(ctx, src)
	if err != nil {
		return nil, err
	}

	cols := &flushColumns{}

	// Decode each source part once and reuse across all its series (compaction reads every
	// series of every part), instead of re-decoding the whole part per series.
	decoded := make(partDecodeCache, len(src))

	for _, id := range ids {
		var m sampleMerge

		// Oldest → newest part, so a later part's value wins on a duplicate timestamp.
		for _, p := range src {
			rng, ok, err := p.index.lookup(ctx, id)
			if err != nil {
				return nil, err
			}

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

// compactStream merges several source parts, streaming both sides so neither the whole merged
// dataset nor a whole output part is ever materialized: each source is read through a forward
// [partStream] decoding one series range at a time, and each merged series is handed straight to a
// [partStreamWriter]. The merge therefore holds O(parts × one series range) + the *encoded* output
// part. capBytes ≤ 0 writes a single output part.
//
// Series are visited in (series, ts) order; within a series the parts are visited oldest→newest so
// a later part's value wins a duplicate timestamp, then the result is downsampled.
func (e *Engine) compactStream(
	ctx context.Context, src []*part, start int64, capBytes int64, opts MergeOptions,
) ([]*part, error) {
	ids, err := sortedSeriesIDs(ctx, src)
	if err != nil {
		return nil, err
	}

	// One forward cursor per source part; one reusable per-part destination per series range.
	streams := make([]*partStream, len(src))
	for i, p := range src {
		s, err := newPartStream(ctx, p)
		if err != nil {
			return nil, err
		}

		streams[i] = s
	}

	scratch := make([]rangeBuf, len(src))
	comp, precision, withSF := mergeEncoding(src, capBytes, opts)
	memBytes := e.mergeMemoryBudgetBytes()

	var (
		newParts []*part
		cur      *partStreamWriter
	)

	// A part under way holds column objects the backend has not published yet; leaving on any path
	// but emit must release them rather than strand them.
	defer func() {
		if cur != nil {
			cur.abort()
		}
	}()

	emit := func() error {
		if cur == nil {
			return nil
		}

		p, err := cur.finish(ctx)
		cur = nil

		if err != nil {
			return err
		}

		newParts = append(newParts, p)

		return nil
	}

	for _, id := range ids {
		m, err := mergeStreamedSeries(ctx, src, streams, scratch, id, start)
		if err != nil {
			return nil, err
		}

		ts, values, sf := m.collect(nil, nil)
		ts, values, sf = downsample(ts, values, sf, opts.Downsample)

		if len(ts) == 0 {
			continue
		}

		if cur == nil {
			if cur, err = newPartStreamWriter(
				ctx, e, e.reserveSeq(), comp, precision, withSF, e.cfg.AggregateStats,
			); err != nil {
				return nil, err
			}
		}

		if err := cur.appendSeries(idToU128(id), ts, values, sf); err != nil {
			return nil, err
		}

		// Sealing on what has actually been written, rather than on rows times an assumed
		// bytes-per-row, is what makes the cap comparable to the free space it comes from. A series
		// overshooting the cap is split at the next series boundary — parts are independent, and
		// the read seam merges a series spanning two.
		//
		// The second bound is the part's resident footprint, which is not a function of its size on
		// disk: the per-series sidecars grow with distinct series, so a merge of very short series
		// reaches the memory budget long before the disk cap.
		if (capBytes > 0 && cur.encodedBytes() >= capBytes) || cur.residentBytes() >= memBytes {
			if err := emit(); err != nil {
				return nil, err
			}
		}
	}

	if err := emit(); err != nil {
		return nil, err
	}

	return newParts, nil
}

// mergeEncoding fixes what a streamed merge's output parts are encoded under. The decisions must be
// settled before the first row is encoded, so they come from the source parts rather than from each
// output part's own contents the way [Engine.writeMergedPart] takes them:
//
//   - Compression and precision key off the group's newest sample. Splitting a (series, ts)-sorted
//     stream by size divides it by *series*, not by time, so every output part spans essentially the
//     group's whole range anyway. Where it differs — a part made only of series that stopped
//     reporting early — the group's maxTime is the newer, so the part is treated as hotter: more
//     precision and a cheaper level than it needs, never less. The next merge sees that part's own
//     stamped maxTime, so the estimate is self-correcting rather than sticky.
//   - The compression ladder is a step function of row count, estimated from the source rows scaled
//     by the share of the group's bytes one output part will hold.
//   - The weight column is declared if any source carries one, and cannot appear from nowhere: with
//     no sampled input every collected weight is 1, and downsample returns a nil weight vector when
//     every output weight is 1. If they all turn out to be 1 anyway, the column is dropped at finish.
func mergeEncoding(src []*part, capBytes int64, opts MergeOptions) (compressProfile, uint8, bool) {
	var (
		maxT     = minInt64
		rows     int
		srcBytes int64
		withSF   bool
	)

	for _, p := range src {
		maxT = max(maxT, p.maxTime)
		rows += p.rows()
		srcBytes += p.sizeBytes()
		withSF = withSF || p.hasSF
	}

	if capBytes > 0 && srcBytes > capBytes {
		rows = int(int64(rows) * capBytes / srcBytes)
	}

	return mergeProfile(opts.Recompress, maxT, rows), pickPrecision(opts.Precision, maxT), withSF
}

// rowCapFor converts the byte cap into a row cap for the whole-column rewrite path, which holds
// decoded columns and so can only split by row. The part being rewritten is the best available
// estimate of how its own rewrite will compress.
func rowCapFor(p *part, capBytes int64) int {
	if capBytes <= 0 {
		return 0
	}

	size, rows := p.sizeBytes(), p.rows()
	if size <= 0 || rows <= 0 {
		return 0
	}

	return max(int(capBytes*int64(rows)/size), 1)
}

// mergeStreamedSeries gathers one series' samples across the source parts (oldest → newest, so a
// later part's value wins on a duplicate timestamp), each read through its forward stream cursor.
func mergeStreamedSeries(
	ctx context.Context, src []*part, streams []*partStream, scratch []rangeBuf,
	id signal.SeriesID, start int64,
) (sampleMerge, error) {
	var m sampleMerge

	for i, p := range src {
		rng, ok, err := p.index.lookup(ctx, id)
		if err != nil {
			return m, err
		}

		if !ok {
			continue
		}

		ts, vals, sf, err := streams[i].decodeRange(rng, &scratch[i])
		if err != nil {
			return m, err
		}

		m.add(ts, vals, sf, start, maxInt64)
	}

	return m, nil
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
