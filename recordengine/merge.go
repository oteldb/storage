package recordengine

import (
	"context"
	"slices"

	"github.com/oteldb/storage/signal"
)

// Merge compacts every flushed part into a single new part, dropping records older than retainFrom
// (retention; retainFrom ≤ 0 disables it). No-op when there is nothing to gain — fewer than two
// parts and no retention cutoff. Records are append-only: a stream's records are concatenated
// across parts (no value dedup) and re-sorted by timestamp.
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

	f, err := e.compactParts(ctx, start)
	if err != nil {
		return err
	}

	old := e.parts

	if f.len() == 0 {
		e.parts = nil // retention dropped every record
	} else {
		prefix := e.partPrefix(e.nextSeq)
		if err := writePart(ctx, e.cfg.Backend, e.cfg.Schema, prefix, f); err != nil {
			return err
		}

		p, err := openPart(ctx, e.cfg.Backend, e.cfg.Schema, prefix)
		if err != nil {
			return err
		}

		p.minTime, p.maxTime = colsTimeRange(f)
		e.parts = []*part{p}
		e.nextSeq++

		if err := e.mergeSidecars(ctx, old, prefix); err != nil {
			return err
		}
	}

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

// mergeSidecars unions the side-store sidecars of the compacted parts and writes the merged tables
// under the new part. No-op when the engine has no side store. Content-addressing makes the union a
// plain dedup — no id remap.
func (e *Engine) mergeSidecars(ctx context.Context, old []*part, newPrefix string) error {
	if e.cfg.SideStore == nil {
		return nil
	}

	parts := make([]map[string][]byte, 0, len(old))
	for _, p := range old {
		m, err := loadSidecars(ctx, e.cfg.Backend, p.prefix, e.cfg.SideStore.Names())
		if err != nil {
			return err
		}

		parts = append(parts, m)
	}

	merged, err := e.cfg.SideStore.Union(parts)
	if err != nil {
		return err
	}

	return writeSidecars(ctx, e.cfg.Backend, newPrefix, merged)
}

// compactParts concatenates every part's records per stream (within [start, maxInt64], applying
// retention), returning the combined full columns sorted by (stream, ts). Empty when none survive.
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

	f := &flushColumns{cols: newRecordCols(e.cfg.Schema, 0, fullSel(e.cfg.Schema))}

	for _, id := range ids {
		acc := newRecordCols(e.cfg.Schema, 0, fullSel(e.cfg.Schema))

		for _, p := range e.parts {
			if err := p.appendWindow(ctx, id, acc, start, maxInt64); err != nil {
				return nil, err
			}
		}

		if acc.len() == 0 {
			continue
		}

		acc.sortByTs()

		u := idToU128(id)
		for i := range acc.ts {
			f.stream = append(f.stream, u)
			f.cols.appendRow(acc, i)
		}
	}

	return f, nil
}

// Close flushes any buffered records to a part and closes the WAL. It does not stop a background
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
