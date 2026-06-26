package engine

import (
	"context"
	"slices"

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
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.mergeLocked(ctx, opts)
}

func (e *Engine) mergeLocked(ctx context.Context, opts MergeOptions) error {
	if len(e.parts) == 0 {
		return nil
	}

	// A single part with no retention cutoff, nothing old enough to downsample, and nothing to
	// recompress has nothing to gain — skip without decoding it.
	if len(e.parts) == 1 && opts.RetainFrom <= 0 &&
		!downsampleApplies(opts.Downsample, e.parts[0].minTime) && !recompressApplies(e.parts[0], opts.Recompress) {
		return nil
	}

	start := minInt64
	if opts.RetainFrom > 0 {
		start = opts.RetainFrom
	}

	cols, err := e.compactParts(ctx, start, opts.Downsample)
	if err != nil {
		return err
	}

	// Fixed point: a single source part whose row count downsampling did not reduce, and which
	// needs no recompression, is already at its target — rewriting it would only churn the backend.
	if len(e.parts) == 1 && opts.RetainFrom <= 0 && len(cols.ts) == e.parts[0].rows() &&
		!recompressApplies(e.parts[0], opts.Recompress) {
		return nil
	}

	old := e.parts

	if len(cols.ts) == 0 {
		// Retention dropped every sample: keep no parts.
		e.parts = nil
	} else {
		minT, maxT := colsTimeRange(cols)
		prefix := e.partPrefix(e.nextSeq)

		// Recompress when the merged part is fully cold (its newest sample predates the cutoff).
		if err := writePart(ctx, e.cfg.Backend, prefix, cols, coldProfile(opts.Recompress, maxT)); err != nil {
			return err
		}

		p, err := openPart(ctx, e.cfg.Backend, prefix)
		if err != nil {
			return err
		}

		p.minTime, p.maxTime = minT, maxT
		e.parts = []*part{p}
		e.nextSeq++
	}

	// Commit the new part set to the bucket index before deleting the source parts, so a
	// crash mid-merge never leaves the index referencing a deleted part (it may leave orphan
	// objects, which are harmless and reclaimed by a later merge).
	if err := e.updateIndexLocked(ctx); err != nil {
		return err
	}

	for _, p := range old {
		if err := deletePart(ctx, e.cfg.Backend, p.prefix); err != nil {
			return err
		}
	}

	return nil
}

// compactParts merges every part's samples per series (within [start, maxInt64], so
// retention is applied), then downsamples the survivors per tiers, returning the combined
// columns sorted by (series, ts). The returned columns are empty when no sample survives.
func (e *Engine) compactParts(ctx context.Context, start int64, tiers []DownsampleTier) (*flushColumns, error) {
	idSet := make(map[signal.SeriesID]struct{})
	for _, p := range e.parts {
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

	for _, id := range ids {
		var m sampleMerge

		// Oldest → newest part, so a later part's value wins on a duplicate timestamp.
		for _, p := range e.parts {
			if err := p.mergeInto(ctx, id, &m, start, maxInt64); err != nil {
				return nil, err
			}
		}

		ts, values, sf := m.collect()
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
	e.mu.Lock()
	defer e.mu.Unlock()

	if err := e.flushLocked(ctx); err != nil {
		return err
	}

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
