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
	return e.MergeWith(ctx, MergeOptions{RetainFrom: retainFrom})
}

// MergeOptions parameterizes a merge. The zero value is a plain compaction.
type MergeOptions struct {
	// RetainFrom drops records older than this absolute unix-nanosecond cutoff (retention);
	// ≤ 0 disables it.
	RetainFrom int64
	// Force compacts a bucket's unsealed parts whatever their tiers, instead of waiting for one tier
	// to accumulate minTierParts of them — the operator escape from a part set the tier rule will
	// never select. It bypasses the selection heuristic only: sealing, the time-bucket ladder, and
	// the cumulative-bytes cap still bound what one merge decodes and holds.
	Force bool
}

// MergeWith is [Engine.Merge] with the merge parameterized: the one background-merge entry point,
// so compaction, retention, and a forced compaction are the same pass over the immutable parts.
func (e *Engine) MergeWith(ctx context.Context, opts MergeOptions) error {
	retainFrom := opts.RetainFrom
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
		zap.Int64("retain_from", retainFrom), zap.Bool("force", opts.Force))

	compacted, err := e.merge(ctx, opts)
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
func (e *Engine) merge(ctx context.Context, opts MergeOptions) (int, error) {
	retainFrom := opts.RetainFrom

	e.flushMu.Lock()
	defer e.flushMu.Unlock()

	// Plan (under lock): snapshot the source parts (immutable backing). Output part sequences are
	// reserved one at a time, as the parts are written.
	e.mu.Lock()
	src := e.parts
	e.mu.Unlock()

	capBytes := e.mergeCapBytes()

	// Retention first, and without decoding: a part every one of whose records is older than the
	// cutoff is dropped whole rather than rewritten into nothing.
	src, dropped, err := e.dropExpired(ctx, src, retainFrom)
	if err != nil {
		return 0, err
	}

	selected := selectMergeParts(src, retainFrom, capBytes, opts.Force)
	if len(selected) == 0 {
		if dropped == 0 {
			// A no-op is indistinguishable from a healthy engine without the shape of what it looked
			// at; these are the exact inputs to that decision (mirrors the metric engine).
			sh := shapeOf(src, capBytes)
			zctx.From(ctx).Debug("merge selected nothing",
				zap.String("signal", e.cfg.Signal), zap.String("prefix", e.cfg.Prefix),
				zap.Int("parts", sh.Parts), zap.Int("sealed", sh.Sealed),
				zap.Int64("cap_bytes", sh.CapBytes), zap.Int("eligible", sh.Backlog),
				zap.Int("tiers", sh.Tiers), zap.Int("largest_tier_parts", sh.LargestTierParts))
		}

		e.reclaimRetired(ctx) // nothing to compact, but still sweep pending deletions

		return dropped, nil
	}

	start := minInt64
	if retainFrom > 0 {
		start = retainFrom
	}

	// Build (lock-free): compact the selected parts into bounded output part(s), reading them back and
	// unioning the side-store sidecars. The selected parts stay live (not retired) until publish, so
	// they cannot be reclaimed underneath this read.
	newParts, err := e.compactParts(ctx, selected, start, capBytes)
	if err != nil {
		return dropped, err
	}

	// Publish (under lock): swap the selected parts for the merged one(s) copy-on-write (keeping every
	// unselected part, including any a concurrent flush may have added) and persist the index. The
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

		return dropped + len(selected), err
	}

	e.retireLocked(selected)
	// Rows that did not survive the merge are retention's work: the records are gone, so the
	// identities naming them may be dead too. Merging without dropping rows leaves every identity
	// backed, so it arms nothing.
	if partRows(newParts) < partRows(selected) {
		e.identityDirty = true
	}

	e.mu.Unlock()

	e.reclaimRetired(ctx)

	return dropped + len(selected), nil
}

// dropExpired retires every part retention has emptied — one whose newest record is already older
// than the cutoff, so not a single row would survive a rewrite — and returns the parts that remain
// plus the number dropped. Retention on such a part is a manifest edit, not a decode: the merge
// path would otherwise read it whole, re-encode nothing, and write an empty result. It is what
// makes retention cost O(1) in the expired data rather than O(bytes).
//
// The drop publishes like any merge — copy-on-write swap, index commit, then retire — so a failed
// commit rolls back and the parts stay live. The retired parts' objects, including their side-store
// and bloom sidecars (all written under the part prefix), are reclaimed by [deletePart].
func (e *Engine) dropExpired(ctx context.Context, src []*part, retainFrom int64) ([]*part, int, error) {
	if retainFrom <= 0 {
		return src, 0, nil
	}

	var expired []*part

	for _, p := range src {
		if p.maxTime < retainFrom {
			expired = append(expired, p)
		}
	}

	if len(expired) == 0 {
		return src, 0, nil
	}

	removed := make(map[string]struct{}, len(expired))
	for _, p := range expired {
		removed[p.prefix] = struct{}{}
	}

	e.mu.Lock()
	committed := e.parts
	e.parts = replaceParts(e.parts, removed)

	if err := e.updateIndexLocked(ctx); err != nil {
		e.parts = committed
		e.mu.Unlock()

		return nil, 0, err
	}

	e.retireLocked(expired)
	// Every record these parts held is gone, so the identities naming them may be dead — the same
	// reasoning as a merge that drops rows, and the identity prune has something to find.
	e.identityDirty = true
	e.mu.Unlock()

	remaining := make([]*part, 0, len(src)-len(expired))

	for _, p := range src {
		if _, drop := removed[p.prefix]; !drop {
			remaining = append(remaining, p)
		}
	}

	zctx.From(ctx).Debug("dropped expired parts",
		zap.String("prefix", e.cfg.Prefix), zap.Int("parts", len(expired)),
		zap.Int64("retain_from", retainFrom))

	return remaining, len(expired), nil
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
// accumulated *decoded* bytes reach capBytes — so both the merge's decoded working set and each
// output part stay within the cap, never O(dataset). When the engine has a side store (profiles) the
// output is a single part (no split) so the unioned symbol sidecar has one home. Returns the new parts
// (empty when retention dropped every record). Reads the parts off the engine lock; src is the
// immutable snapshot the caller planned over.
func (e *Engine) compactParts(ctx context.Context, src []*part, start, capBytes int64) ([]*part, error) {
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
	split := capBytes > 0 && e.cfg.SideStore == nil

	// One output buffer, pre-sized from the sources and re-armed after each part instead of allocated
	// fresh. A byte column starting from nothing doubles its way to the seal threshold, re-copying a
	// part's worth of bodies at every step and leaving each intermediate blob for the GC — the single
	// largest allocation site in the engine, and most of the collector time a compaction spends.
	bufRows, bufBlob := decodedShape(decoded, capBytes)

	buf := &flushColumns{cols: newRecordCols(e.cfg.Schema, 0, fullSel(e.cfg.Schema))}
	buf.reset(e.cfg.Schema, bufRows, bufBlob)

	var newParts []*part

	emit := func() error {
		if buf.len() == 0 {
			return nil
		}

		p, err := e.writeMergedPart(ctx, src, buf, e.reserveSeq())
		if err != nil {
			return err
		}

		newParts = append(newParts, p)
		// The written part is read back from the backend, so nothing outlives the call holding the
		// buffer's arrays: the next part reuses them at the size the first one settled on.
		buf.reset(e.cfg.Schema, bufRows, bufBlob)

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
		for range acc.ts {
			buf.stream = append(buf.stream, u)
		}

		// The stream's rows are contiguous in both buffers, so they move as one blob copy per column
		// rather than a cell-at-a-time append.
		buf.cols.appendRange(acc, 0, acc.len())

		// Flush a full part once the buffer reaches the cap, measured in the decoded bytes it actually
		// holds rather than in rows times an assumed row size — records are variable-width, so a row
		// count is only as good as that assumption. A stream whose own run overshoots the cap is split
		// at the next stream boundary (parts are independent; the read seam concatenates a stream
		// spanning parts), keeping the buffer at ≈ one part regardless of a heavy stream.
		if split && buf.byteSize() >= capBytes {
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
	// The merged part's identities come from the resident index, which spans every live stream —
	// snapshotted here (a brief read lock, off the flush path) because the write itself is off-lock.
	if err := writePart(ctx, e.cfg.Backend, e.cfg.Schema, prefix, f, e.identitiesForColumn(f.stream),
		e.cfg.MergeCompression, e.cfg.MergeCompressionLevel,
		e.blooms()); err != nil {
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
