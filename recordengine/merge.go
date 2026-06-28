package recordengine

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

// Merge compacts every flushed part into a single new part, dropping records older than retainFrom
// (retention; retainFrom ≤ 0 disables it). No-op when there is nothing to gain — fewer than two
// parts and no retention cutoff. Records are append-only: a stream's records are concatenated
// across parts (no value dedup) and re-sorted by timestamp.
func (e *Engine) Merge(ctx context.Context, retainFrom int64) error {
	ctx = e.cfg.Obs.Base(ctx)
	ctx, span := e.cfg.Obs.Tracer.Start(ctx, "recordengine.merge",
		trace.WithAttributes(attribute.String("storage.prefix", e.cfg.Prefix)))
	defer span.End()

	e.mergeRunning.Store(true)
	defer e.mergeRunning.Store(false)

	startNs := time.Now()
	log := zctx.From(ctx)
	log.Debug("merge requested",
		zap.String("signal", e.cfg.Signal), zap.String("prefix", e.cfg.Prefix),
		zap.Int64("retain_from", retainFrom))

	compacted, err := e.merge(ctx, retainFrom)
	if err != nil {
		span.RecordError(err)
		log.Error("merge failed",
			zap.String("signal", e.cfg.Signal), zap.String("prefix", e.cfg.Prefix), zap.Error(err))

		return err
	}

	if compacted > 0 {
		span.SetAttributes(attribute.Int("storage.merge.parts_in", compacted))
		e.cfg.Obs.Merge.Record(ctx, e.cfg.Signal, time.Since(startNs), int64(compacted))
		log.Debug("merged parts",
			zap.String("signal", e.cfg.Signal), zap.String("prefix", e.cfg.Prefix),
			zap.Int("parts_in", compacted), zap.Duration("took", time.Since(startNs)))
	} else {
		log.Debug("merge no-op (nothing to compact)",
			zap.String("signal", e.cfg.Signal), zap.String("prefix", e.cfg.Prefix))
	}

	return nil
}

// merge compacts every flushed part into one new part, returning the number of source parts compacted
// (0 ⇒ no-op). Phased like flush: the source-part reads, the compacted-part write/read-back, and the
// sidecar union happen off the engine lock; only the small metadata publish runs under it. The old
// parts are retired (not deleted inline) and reclaimed once their in-flight readers drain. Only the
// background maintenance task calls merge, so the parts mutation has a single writer.
func (e *Engine) merge(ctx context.Context, retainFrom int64) (int, error) {
	// Plan (under lock): snapshot the source parts (immutable backing) and reserve the sequence.
	e.mu.Lock()
	src := e.parts
	noop := len(src) == 0 || (len(src) == 1 && retainFrom <= 0)
	seq := e.nextSeq
	e.mu.Unlock()

	if noop {
		e.reclaimRetired(ctx) // nothing to compact, but still sweep pending deletions

		return 0, nil
	}

	start := minInt64
	if retainFrom > 0 {
		start = retainFrom
	}

	// Build (lock-free): read+compact the source parts, write the merged part, read it back, union the
	// side-store sidecars. A merge reads the source parts; they stay live (not retired) until publish,
	// so they cannot be reclaimed underneath this read.
	f, err := e.compactParts(ctx, src, start)
	if err != nil {
		return 0, err
	}

	var newPart *part

	if f.len() > 0 {
		prefix := e.partPrefix(seq)
		if err := writePart(ctx, e.cfg.Backend, e.cfg.Schema, prefix, f); err != nil {
			return 0, err
		}

		newPart, err = openPart(ctx, e.cfg.Backend, e.cfg.Schema, prefix)
		if err != nil {
			return 0, err
		}

		newPart.minTime, newPart.maxTime = colsTimeRange(f)

		if err := e.mergeSidecars(ctx, src, prefix); err != nil {
			return 0, err
		}
	}

	// Publish (under lock): swap the source parts for the merged one copy-on-write (keeping any part a
	// concurrent flush may have added), retire the sources, and persist the index. The retired parts'
	// objects are deleted by reclaimRetired once their readers drain.
	removed := make(map[string]struct{}, len(src))
	for _, p := range src {
		removed[p.prefix] = struct{}{}
	}

	e.mu.Lock()
	e.parts = replaceParts(e.parts, removed, newPart)
	if newPart != nil {
		e.nextSeq = seq + 1
	}

	e.retireLocked(src)
	err = e.updateIndexLocked(ctx)
	e.mu.Unlock()

	if err != nil {
		return len(src), err
	}

	e.reclaimRetired(ctx)

	return len(src), nil
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

// compactParts concatenates each source part's records per stream (within [start, maxInt64], applying
// retention), returning the combined full columns sorted by (stream, ts). Empty when none survive. It
// reads the parts off the engine lock; src is the immutable snapshot the caller planned over.
func (e *Engine) compactParts(ctx context.Context, src []*part, start int64) (*flushColumns, error) {
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

	f := &flushColumns{cols: newRecordCols(e.cfg.Schema, 0, fullSel(e.cfg.Schema))}

	for _, id := range ids {
		acc := newRecordCols(e.cfg.Schema, 0, fullSel(e.cfg.Schema))

		for _, p := range src {
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
