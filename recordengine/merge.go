package recordengine

import (
	"context"
	"iter"
	"time"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/oteldb/storage/encoding/compress"
	"github.com/oteldb/storage/internal/mergestream"
	"github.com/oteldb/storage/internal/timebucket"
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
	// Background marks this merge as the maintenance loop's own, where declining costs nothing: if
	// the process merge budget ([Config.MergeAdmission]) is fully committed the merge is skipped and
	// the next cycle retries it, rather than waiting.
	//
	// It is opt-in because waiting is the safe default. A caller that asked for a merge — an
	// operator command, a test, an embedder driving the engine itself — must get one, not a silent
	// no-op; the maintenance loop is the only caller for which the opposite is true, because it runs
	// on one goroutine that also services flush pressure and must not park on a busy budget.
	Background bool
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

	// A fenced engine would do the whole merge, and repair's peer fetches, only for the commit to
	// refuse them.
	if err := e.fence(); err != nil {
		return err
	}

	// Repair first: a part pulled back from a peer joins this cycle's compaction, and a merge that
	// cannot repair still compacts.
	e.repairWants(ctx)

	res, err := e.merge(ctx, opts)
	if err != nil {
		span.RecordError(err)
		log.Error("merge failed",
			zap.String("signal", e.cfg.Signal), zap.String("prefix", e.cfg.Prefix), zap.Error(err))

		return err
	}

	if res.parts > 0 {
		span.SetAttributes(attribute.Int("storage.merge.parts_in", res.parts),
			attribute.Int64("storage.merge.bytes_out", res.bytesOut))
		e.cfg.Obs.Merge.Record(ctx, e.cfg.Signal, time.Since(startNs), int64(res.parts), res.bytesIn, res.bytesOut)
		// deferred says those parts were dropped by retention, not compacted; see the metric engine.
		log.Debug("merged parts",
			zap.Bool("deferred", res.deferred),
			zap.String("signal", e.cfg.Signal), zap.String("prefix", e.cfg.Prefix),
			zap.Int("parts_in", res.parts), zap.Int64("bytes_in", res.bytesIn),
			zap.Int64("bytes_out", res.bytesOut), zap.Duration("took", time.Since(startNs)))
	} else if !res.deferred {
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
func (e *Engine) merge(ctx context.Context, opts MergeOptions) (mergeResult, error) {
	retainFrom := opts.RetainFrom

	e.flushMu.Lock()
	defer e.flushMu.Unlock()

	// Plan (under lock): snapshot the source parts (immutable backing). Output part ids are minted one
	// at a time, as the parts are written.
	e.mu.Lock()
	src := e.parts
	e.mu.Unlock()

	capBytes := e.mergeCapBytes()

	// Retention first, and without decoding: a part every one of whose records is older than the
	// cutoff is dropped whole rather than rewritten into nothing.
	src, dropped, err := e.dropExpired(ctx, src, retainFrom)
	if err != nil {
		return mergeResult{}, err
	}

	selected := selectMergeParts(src, retainFrom, capBytes, opts.Force)
	if len(selected) == 0 {
		if dropped == 0 {
			// A no-op is indistinguishable from a healthy engine without the shape of what it looked
			// at; these are the exact inputs to that decision (mirrors the metric engine).
			sh := shapeOf(src, retainFrom, capBytes)
			zctx.From(ctx).Debug("merge selected nothing",
				zap.String("signal", e.cfg.Signal), zap.String("prefix", e.cfg.Prefix),
				zap.Int("parts", sh.Parts), zap.Int("sealed", sh.Sealed),
				zap.Int64("cap_bytes", sh.CapBytes), zap.Int("eligible", sh.Backlog),
				zap.Int("tiers", sh.Tiers), zap.Int("largest_tier_parts", sh.LargestTierParts))
		}

		e.mergeDeferred.Store(false)
		e.reclaimRetired(ctx) // nothing to compact, but still sweep pending deletions

		return mergeResult{parts: dropped}, nil
	}

	// Past here the merge reads the selected parts and writes its output, so this is where its memory
	// allowance must be one it actually holds rather than one it assumed.
	release, admitted, err := e.admitMerge(ctx, opts.Background)
	if err != nil {
		e.mergeDeferred.Store(false)

		return mergeResult{parts: dropped}, err
	}

	if !admitted {
		// Remembered so the facade can order this engine first next cycle; see the metric engine.
		e.mergeDeferred.Store(true)
		e.cfg.Obs.Merge.Deferred(ctx, e.cfg.Signal)
		zctx.From(ctx).Debug("merge deferred; the process merge budget is fully committed",
			zap.String("signal", e.cfg.Signal), zap.String("prefix", e.cfg.Prefix),
			zap.Int("selected", len(selected)))
		e.reclaimRetired(ctx)

		return mergeResult{deferred: true, parts: dropped}, nil
	}

	e.mergeDeferred.Store(false)

	defer release()

	start := minInt64
	if retainFrom > 0 {
		start = retainFrom
	}

	// Build (lock-free): compact the selected parts into bounded output part(s), reading them back and
	// unioning the side-store sidecars. The selected parts stay live (not retired) until publish, so
	// they cannot be reclaimed underneath this read.
	bytesIn := partsBytes(selected)

	newParts, err := e.compactParts(ctx, selected, start, capBytes)
	if err != nil {
		return mergeResult{parts: dropped}, err
	}

	planMergeBlocks(selected, newParts)

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

		return mergeResult{parts: dropped + len(selected), bytesIn: bytesIn, bytesOut: partsBytes(newParts)}, err
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

	return mergeResult{parts: dropped + len(selected), bytesIn: bytesIn, bytesOut: partsBytes(newParts)}, nil
}

// mergeResult is what one merge moved: the source parts it compacted (including those retention
// dropped whole) and the bytes it read and wrote. bytesOut over bytesIn is the merge's write
// amplification, which no part count shows.
type mergeResult struct {
	parts             int
	bytesIn, bytesOut int64
	// deferred reports that a run was selected but the process merge budget had nothing to give it,
	// so nothing was compacted. It is not an error: the next cycle retries.
	deferred bool
}

// partsBytes sums the decoded footprint of ps ([part.sizeBytes]), not its on-disk size — unlike the
// metric engine's namesake.
func partsBytes(ps []*part) int64 {
	var n int64
	for _, p := range ps {
		n += p.sizeBytes()
	}

	return n
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

// mergeSidecars unions the side-store sidecars of the compacted parts, keeps what the new part's
// refs reach, and writes the result under the new part. No-op when the engine has no side store.
// Content-addressing makes the union a plain dedup — no id remap.
func (e *Engine) mergeSidecars(ctx context.Context, old []*part, newPrefix string, refs iter.Seq[[]byte]) error {
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

	if merged, err = e.cfg.SideStore.Retained(merged, refs); err != nil {
		return err
	}

	stored, err := e.cfg.SideStore.Stored(merged)
	if err != nil {
		return err
	}

	return writeSidecars(ctx, e.cfg.Backend, newPrefix, stored)
}

// compactParts compacts the selected source parts into bounded output part(s), dropping records older
// than start (retention). It streams both sides: each source is read forward a granule at a time per
// column ([partCursor]) unless its layout rules that out, when it is decoded whole; each stream's rows
// are merged across the sources by a k-way heap on (timestamp, source), and routed by timestamp to a
// writer per day ([timebucket.Router]) that encodes them as they arrive
// ([recordPartStreamWriter]). A writer is sealed once it has taken capBytes of decoded rows, and the
// largest open writer once the writers together hold the merge's admitted share in RAM, both checked
// after every append of at most a granule — so either bound is overshot by at most one append, a
// stream may continue in the next part, and the merge never holds a stream or a part.
// When the engine has a side store (profiles) the cap does not split a day, but the resident bound
// still can, and every part it writes carries the symbols its own rows reach. Returns the new parts
// (empty when retention dropped every record). Reads the parts off the engine lock; src is the
// immutable snapshot the caller planned over.
func (e *Engine) compactParts(ctx context.Context, src []*part, start, capBytes int64) ([]*part, error) {
	for {
		out, err := e.compactStreamed(ctx, src, start, capBytes)

		var disorder *sourceDisorderError
		if !errors.As(err, &disorder) {
			return out, err
		}

		// Rows of the disordered stream may already be written out of order, so the attempt is
		// dropped whole and redone with that source decoded whole, which sorts what it serves. The
		// parts it sealed are removed now; an object that is not is an orphan the next open sweeps.
		src[disorder.src].tsDisorder.Store(true)

		for _, p := range out {
			_ = deletePart(ctx, e.cfg.Backend, p.prefix)
		}
	}
}

// compactStreamed is one attempt at [Engine.compactParts]. On a [sourceDisorderError] it returns the
// parts it had already sealed with the error.
func (e *Engine) compactStreamed(ctx context.Context, src []*part, start, capBytes int64) ([]*part, error) {
	bounds := e.mergeBounds(capBytes)

	// One compressor for every day writer of the merge: a zstd encoder at the best level holds
	// ~24 MiB while its pool keeps it, and a pool per writer keeps one per open day.
	comp := compress.NewCompressor(e.cfg.MergeCompression, e.cfg.MergeCompressionLevel)

	sources, err := e.openMergeSources(ctx, src)
	if err != nil {
		return nil, err
	}

	var newParts []*part

	router := timebucket.Router[*recordPartStreamWriter]{
		Open: func(int64) (*recordPartStreamWriter, error) {
			return newRecordPartStreamWriter(ctx, e, src, comp)
		},
		Finish: func(w *recordPartStreamWriter) error {
			p, err := w.finish(ctx)
			if err != nil {
				return err
			}

			newParts = append(newParts, p)

			return nil
		},
		Resident:      (*recordPartStreamWriter).residentBytes,
		MaxOpen:       timebucket.MaxOpenWriters,
		ResidentLimit: bounds.residentBytes,
	}

	// A part under way holds column objects the backend has not published yet; leaving on any path
	// but Close must release them rather than strand them.
	defer router.Drop((*recordPartStreamWriter).abort)

	var (
		keys mergestream.Keys
		heap runHeap
	)

	mergeKeys(src, &keys)

	runBytes := bounds.partBytes / mergeRunFraction

	for keys.Next() {
		id := keys.Key()
		if mergeSkipStream != nil && mergeSkipStream(id) {
			continue
		}

		if err := heap.reset(sources, id, start); err != nil {
			return newParts, err
		}

		u := idToU128(id)

		for heap.len() > 0 {
			ts := heap.items[0].ts
			dayEnd := timebucket.End(ts, timebucket.Top())

			err := router.Append(ts, func(w *recordPartStreamWriter) (bool, error) {
				if err := heap.fill(w, u, dayEnd, runBytes); err != nil {
					return false, err
				}

				return bounds.partBytes > 0 && w.decodedBytes() >= bounds.partBytes, nil
			})
			if err != nil {
				return newParts, err
			}
		}
	}

	if err := checkDrained(src, sources); err != nil {
		return nil, err
	}

	if err := router.Close(); err != nil {
		return nil, err
	}

	if mergeResidentObserver != nil {
		peak, run := router.Peak()
		mergeResidentObserver(peak, run, bounds.residentBytes)
	}

	return newParts, nil
}

// mergeResidentObserver, when non-nil, receives after each merge the most its open writers held in
// RAM together, the most one append added, and the resident limit. Test seam only.
var mergeResidentObserver func(peak, run, limit int64)

// mergeRunFraction is how much of the part bound one append to a day's writer may add before the
// writer is checked against it: a part reaches at most the bound plus this share of it, however many
// rows one stream holds in one day.
const mergeRunFraction = 4

// Close flushes any buffered records to a part and closes the WAL. It does not stop a background
// loop — the owner ([storage.Storage]) does that before calling Close.
func (e *Engine) Close(ctx context.Context) error {
	if _, _, err := e.flush(ctx); err != nil {
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
