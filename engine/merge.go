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
// free of wall-clock dependencies; the caller derives it from the tenant policy.
func (e *Engine) Merge(ctx context.Context, retainFrom int64) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.mergeLocked(ctx, retainFrom)
}

func (e *Engine) mergeLocked(ctx context.Context, retainFrom int64) error {
	if len(e.parts) == 0 || (len(e.parts) == 1 && retainFrom <= 0) {
		return nil
	}

	start := minInt64
	if retainFrom > 0 {
		start = retainFrom
	}

	cols, err := e.compactParts(ctx, start)
	if err != nil {
		return err
	}

	old := e.parts

	if len(cols.ts) == 0 {
		// Retention dropped every sample: keep no parts.
		e.parts = nil
	} else {
		prefix := e.partPrefix(e.nextSeq)
		if err := writePart(ctx, e.cfg.Backend, prefix, cols); err != nil {
			return err
		}

		p, err := openPart(ctx, e.cfg.Backend, prefix)
		if err != nil {
			return err
		}

		p.minTime, p.maxTime = colsTimeRange(cols)
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
// retention is applied), returning the combined columns sorted by (series, ts). The
// returned columns are empty when no sample survives.
func (e *Engine) compactParts(ctx context.Context, start int64) (*flushColumns, error) {
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

		ts, values := m.collect()

		u := idToU128(id)
		for i := range ts {
			cols.series = append(cols.series, u)
			cols.ts = append(cols.ts, ts[i])
			cols.value = append(cols.value, values[i])
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

// SyncWAL fsyncs the engine's WAL, if any (the background WALSyncInterval path). No-op without a WAL.
func (e *Engine) SyncWAL() error {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.cfg.WAL != nil {
		return e.cfg.WAL.Sync()
	}

	return nil
}
