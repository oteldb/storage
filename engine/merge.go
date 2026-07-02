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
	// Plan (under lock): snapshot the source parts (immutable backing) and reserve the sequence.
	e.mu.Lock()
	src := e.parts
	seq := e.nextSeq
	e.mu.Unlock()

	maxRows := maxRowsPerPart(e.cfg.MaxPartBytes)
	capRows := mergeCapRows(maxRows)

	selected := selectMergeParts(src, opts, capRows)
	if len(selected) == 0 {
		e.reclaimRetired(ctx)

		return 0, nil
	}

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

		if newParts, err = e.writeColumns(ctx, cols, seq, capRows, opts); err != nil {
			return 0, err
		}
	} else if newParts, err = e.compactStream(ctx, selected, start, seq, capRows, opts); err != nil {
		return 0, err
	}

	// Publish (under lock): swap the selected parts for the merged one(s) copy-on-write (keeping every
	// part not selected — including any a concurrent flush may have added), retire the sources, and
	// commit the index before any deletion — so a crash mid-merge never leaves the index referencing a
	// deleted part. The retired parts' objects are deleted by reclaimRetired once their readers drain.
	removed := make(map[string]struct{}, len(selected))
	for _, p := range selected {
		removed[p.prefix] = struct{}{}
	}

	e.mu.Lock()
	e.parts = replaceParts(e.parts, removed, newParts...)
	e.nextSeq = seq + len(newParts)

	e.retireLocked(selected)
	err = e.updateIndexLocked(ctx)
	e.mu.Unlock()

	if err != nil {
		return len(selected), err
	}

	e.reclaimRetired(ctx)

	return len(selected), nil
}

// writeColumns splits cols into one or more output parts, each kept under capRows (a single part
// when capRows ≤ 0), writes each, and reads it back. Used for the single-part merge path; the
// multi-part path streams (see compactStream). Returns nil when cols is empty (e.g. retention
// dropped every sample).
func (e *Engine) writeColumns(ctx context.Context, cols *flushColumns, seq, capRows int, opts MergeOptions) ([]*part, error) {
	if len(cols.ts) == 0 {
		return nil, nil
	}

	ranges := chunkRanges(len(cols.ts), capRows)
	newParts := make([]*part, 0, len(ranges))

	for i, rg := range ranges {
		sub := cols.slice(rg[0], rg[1])

		p, err := e.writeMergedPart(ctx, sub, seq+i, opts)
		if err != nil {
			return nil, err
		}

		newParts = append(newParts, p)
	}

	return newParts, nil
}

// writeMergedPart writes cols as the seq-th output part with the cold-tier compression/precision its
// own newest sample selects, reads it back, and stamps its time bounds.
func (e *Engine) writeMergedPart(ctx context.Context, cols *flushColumns, seq int, opts MergeOptions) (*part, error) {
	minT, maxT := colsTimeRange(cols)
	prefix := e.partPrefix(seq)

	if err := writePart(ctx, e.cfg.Backend, prefix, cols,
		coldProfile(opts.Recompress, maxT), pickPrecision(opts.Precision, maxT),
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

// compactStream merges several source parts and writes the result directly to bounded output parts,
// never materializing the whole merged dataset: it accumulates merged rows in one reused buffer and
// flushes a part each time the buffer reaches the output cap.
//
// Each source part is read through a forward [partStream] that decodes one series range at a time,
// advancing strictly forward through the part's (series, ts)-sorted rows. So a merge's resident
// decoded input is O(parts × one-series-range) rather than O(parts × whole-column): the streaming
// k-way merge (issue #25, item 1's full fix), which keeps the background merge's working set bounded
// regardless of how large the merged parts grow — letting the merge cap rise and part count fall
// toward O(log N) without a decode-memory regression. capRows is the output-part row limit (the
// merge cap, mergeHeight × MaxPartBytes); capRows ≤ 0 (unlimited part size) writes a single output
// part.
//
// Series are visited in (series, ts) order; within a series the parts are visited oldest→newest so
// a later part's value wins a duplicate timestamp, then the result is downsampled.
func (e *Engine) compactStream(
	ctx context.Context, src []*part, start int64, seq, capRows int, opts MergeOptions,
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

	var (
		newParts []*part
		buf      flushColumns // the single reused output buffer
	)

	emit := func() error {
		if len(buf.ts) == 0 {
			return nil
		}

		p, err := e.writeMergedPart(ctx, &buf, seq+len(newParts), opts)
		if err != nil {
			return err
		}

		newParts = append(newParts, p)
		buf.reset()

		return nil
	}

	for _, id := range ids {
		m, err := mergeStreamedSeries(ctx, src, streams, scratch, id, start)
		if err != nil {
			return nil, err
		}

		ts, values, sf := m.collect(nil, nil)
		ts, values, sf = downsample(ts, values, sf, opts.Downsample)

		u := idToU128(id)
		for i := range ts {
			w := float64(1)
			if sf != nil {
				w = sf[i]
			}

			buf.appendRow(u, ts[i], values[i], w)
		}

		// Flush a full part once the buffer reaches the output cap. A series whose own run overshoots
		// the cap is split at the next series boundary (parts are independent; the read seam merges a
		// series spanning parts), keeping the buffer at ≈ one part regardless of a heavy series.
		if capRows > 0 && len(buf.ts) >= capRows {
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
