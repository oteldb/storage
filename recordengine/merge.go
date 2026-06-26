package recordengine

import (
	"context"
	"slices"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/oteldb/storage/signal"
)

// Merge compacts every flushed part into a single new part, dropping records older than retainFrom
// (retention; retainFrom ≤ 0 disables it). No-op when there is nothing to gain — fewer than two
// parts and no retention cutoff. Records are append-only: a stream's records are concatenated
// across parts (no value dedup) and re-sorted by timestamp.
func (e *Engine) Merge(ctx context.Context, retainFrom int64) error {
	ctx, span := e.cfg.Obs.Tracer.Start(ctx, "recordengine.merge",
		trace.WithAttributes(attribute.String("storage.prefix", e.cfg.Prefix)))
	defer span.End()

	startNs := time.Now()

	e.mu.Lock()
	compacted, err := e.mergeLocked(ctx, retainFrom)
	e.mu.Unlock()

	if err != nil {
		span.RecordError(err)
	}

	if compacted > 0 {
		span.SetAttributes(attribute.Int("storage.merge.parts_in", compacted))
		e.cfg.Obs.Merge.Record(ctx, e.cfg.Signal, time.Since(startNs), int64(compacted))
	}

	return err
}

// mergeLocked compacts the parts and returns the number of source parts compacted (0 ⇒ no-op). The
// caller holds e.mu.
func (e *Engine) mergeLocked(ctx context.Context, retainFrom int64) (int, error) {
	if len(e.parts) == 0 || (len(e.parts) == 1 && retainFrom <= 0) {
		return 0, nil
	}

	start := minInt64
	if retainFrom > 0 {
		start = retainFrom
	}

	f, err := e.compactParts(ctx, start)
	if err != nil {
		return 0, err
	}

	old := e.parts
	compacted := len(old)

	if f.len() == 0 {
		e.parts = nil // retention dropped every record
	} else {
		prefix := e.partPrefix(e.nextSeq)
		if err := writePart(ctx, e.cfg.Backend, e.cfg.Schema, prefix, f); err != nil {
			return 0, err
		}

		p, err := openPart(ctx, e.cfg.Backend, e.cfg.Schema, prefix)
		if err != nil {
			return 0, err
		}

		p.minTime, p.maxTime = colsTimeRange(f)
		e.parts = []*part{p}
		e.nextSeq++

		if err := e.mergeSidecars(ctx, old, prefix); err != nil {
			return 0, err
		}
	}

	if err := e.updateIndexLocked(ctx); err != nil {
		return 0, err
	}

	for _, p := range old {
		if err := deletePart(ctx, e.cfg.Backend, p.prefix); err != nil {
			return compacted, err
		}
	}

	return compacted, nil
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

	if _, err := e.flushLocked(ctx); err != nil {
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
