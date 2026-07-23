package recordengine

import (
	"context"
	"time"

	"github.com/go-faster/sdk/zctx"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// Merge runs one size-tiered compaction cycle, dropping records older than retainFrom (retention;
// retainFrom ≤ 0 disables it). It compacts only a bounded group of similarly-sized parts plus any part
// retention must rewrite (see [selectMergeParts]) — not the whole part set — so a single merge's decoded
// working set is O(part size), not O(dataset). No-op when no tier has accumulated enough parts and no
// part needs retention. Records are append-only: a stream's records are concatenated across parts (no
// value dedup) and re-sorted by timestamp.
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

// merge compacts a bounded, size-tiered group of the engine's parts and returns the number of source
// parts compacted (0 ⇒ no-op). It does not re-read the whole part set: [selectMergeParts] picks only
// the parts worth merging this cycle (a same-size tier group plus any part retention must rewrite), so
// a single merge's working set is O(part size), not O(dataset). Phased like flush: the source-part
// reads, the compacted-part write/read-back, and the sidecar union happen off the engine lock; only the
// small metadata publish runs under it. The old parts are retired (not deleted inline) and reclaimed
// once their in-flight readers drain. Only the background maintenance task calls merge, so the parts
// mutation has a single writer.
func (e *Engine) merge(ctx context.Context, retainFrom int64) (int, error) {
	// Plan (under lock): snapshot the source parts (immutable backing) and reserve the sequence.
	e.mu.Lock()
	src := e.parts
	seq := e.nextSeq
	e.mu.Unlock()

	capRows := mergeCapRows(maxRowsPerPart(e.cfg.MaxPartBytes))

	selected := selectMergeParts(src, retainFrom, capRows)
	if len(selected) == 0 {
		e.reclaimRetired(ctx) // nothing to compact, but still sweep pending deletions

		return 0, nil
	}

	start := minInt64
	if retainFrom > 0 {
		start = retainFrom
	}

	// Build (lock-free): compact the selected parts into bounded output part(s), reading them back and
	// unioning the side-store sidecars. The selected parts stay live (not retired) until publish, so
	// they cannot be reclaimed underneath this read.
	newParts, err := e.compactParts(ctx, selected, start, seq, capRows)
	if err != nil {
		return 0, err
	}

	// Publish (under lock): swap the selected parts for the merged one(s) copy-on-write (keeping every
	// unselected part, including any a concurrent flush may have added), retire the sources, and persist
	// the index. The retired parts' objects are deleted by reclaimRetired once their readers drain.
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

// compactParts compacts the selected source parts into bounded output part(s): it decodes each part
// once (reused across all its streams), concatenates every stream's in-window records across parts
// (retention applied via start), re-sorts each stream by ts, and writes a new part whenever the
// accumulated rows reach capRows — so both the merge's decoded working set and each output part stay
// O(mergeHeight × MaxPartBytes), never O(dataset). When the engine has a side store (profiles) the
// output is a single part (no split) so the unioned symbol sidecar has one home. Returns the new parts
// (empty when retention dropped every record). Reads the parts off the engine lock; src is the
// immutable snapshot the caller planned over.
func (e *Engine) compactParts(ctx context.Context, src []*part, start int64, seq, capRows int) ([]*part, error) {
	// Decode each source part once, keeping byte columns dict-compressed (see decodedPart). A merge
	// reads every stream of every part, so decoding per-stream (the old appendWindow path) re-decoded
	// the whole part once per stream; decoding up front is O(selected parts), which selection bounds to
	// ≈ one sealed part's worth, and the dict-compressed byte columns keep the per-part constant small.
	decoded := make([]*decodedPart, len(src))
	for i, p := range src {
		d, err := p.readForMerge(ctx)
		if err != nil {
			return nil, err
		}

		decoded[i] = d
	}

	// Split output only when a part-size cap applies and there is no side store to anchor per-part.
	split := capRows > 0 && e.cfg.SideStore == nil

	newBuf := func() *flushColumns {
		return &flushColumns{cols: newRecordCols(e.cfg.Schema, 0, fullSel(e.cfg.Schema))}
	}

	var (
		newParts []*part
		buf      = newBuf()
	)

	emit := func() error {
		if buf.len() == 0 {
			return nil
		}

		p, err := e.writeMergedPart(ctx, src, buf, seq+len(newParts))
		if err != nil {
			return err
		}

		newParts = append(newParts, p)
		buf = newBuf()

		return nil
	}

	// One accumulator, re-armed per stream: a merge visits every stream of the selected parts (tens of
	// thousands on real log data), so allocating one per stream churned a fresh set of column buffers
	// — and their doubling growth — through the GC for each. [recordCols.prepare] keeps the backing
	// arrays.
	acc := newRecordCols(e.cfg.Schema, 0, fullSel(e.cfg.Schema))

	for _, id := range idSetOf(src) {
		acc.prepare(e.cfg.Schema, 0, fullSel(e.cfg.Schema))

		// Oldest → newest part order; records are append-only (no dedup), so the stream is just
		// concatenated across parts and re-sorted by ts below.
		for i, p := range src {
			rng, ok := p.ranges[id]
			if !ok {
				continue
			}

			appendMergeWindow(acc, decoded[i], rng, start, maxInt64)
		}

		if acc.len() == 0 {
			continue
		}

		acc.sortByTs()

		u := idToU128(id)
		for j := range acc.ts {
			buf.stream = append(buf.stream, u)
			buf.cols.appendRow(acc, j)
		}

		// Flush a full part once the buffer reaches the cap. A stream whose own run overshoots the cap is
		// split at the next stream boundary (parts are independent; the read seam concatenates a stream
		// spanning parts), keeping the buffer at ≈ one part regardless of a heavy stream.
		if split && buf.len() >= capRows {
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

// writeMergedPart writes f as the seq-th output part, reads it back, stamps its time bounds, and — when
// the engine has a side store — writes the union of the source parts' sidecars under it.
func (e *Engine) writeMergedPart(ctx context.Context, src []*part, f *flushColumns, seq int) (*part, error) {
	prefix := e.partPrefix(seq)
	// Compacted parts are the cold, long-lived data — block-compress them (typically ZSTD) so the
	// dict/DoD-coded columns are also entropy-coded. Defaults to AlgorithmNone (legacy, uncompressed).
	if err := writePart(ctx, e.cfg.Backend, e.cfg.Schema, prefix, f, e.cfg.MergeCompression, e.cfg.MergeCompressionLevel); err != nil {
		return nil, err
	}

	p, err := openPart(ctx, e.cfg.Backend, e.cfg.Schema, prefix)
	if err != nil {
		return nil, err
	}

	p.minTime, p.maxTime = colsTimeRange(f)

	if e.cfg.SideStore != nil {
		if err := e.mergeSidecars(ctx, src, prefix); err != nil {
			return nil, err
		}
	}

	return p, nil
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
