package engine

import (
	"context"
	"encoding/binary"
	"slices"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"go.uber.org/zap"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/block"
	"github.com/oteldb/storage/signal"
)

// The engine maintains a [bucketindex] alongside its parts so the part set is durable and a
// node can reconstruct it from the object store without local state (the object-store-native
// read path). The index is rewritten on every flush and merge, through the backend's
// compare-and-swap, so two writers over one prefix cannot overwrite each other's parts.

// indexKey is the backend key of this engine's bucket index.
func (e *Engine) indexKey() string {
	return e.cfg.Prefix + "/" + bucketindex.Object
}

// seriesKey is the backend key of this engine's persisted identity index (the series
// labels). Parts store only series ids; the labels needed to resolve matchers and to label
// fetched batches live here, so a stateless reader can rebuild the postings/series index
// without the (local) WAL.
func (e *Engine) seriesKey() string { return e.cfg.Prefix + "/series.bin" }

// indexCommitAttempts bounds the reload-and-retry loop of [Engine.updateIndexLocked]. A retry
// only happens after a rival writer committed, so the loop makes progress; the bound is there so
// pathological contention ends in a reported error instead of an unbounded spin. Exhausting it
// fails the flush or merge that asked for the commit, which is the point: a part whose entry
// never landed is unreachable, and reporting success over it is what loses data.
const indexCommitAttempts = 8

// updateIndexLocked commits a bucket index matching the engine's current parts, conditionally on
// the version this engine last saw. A rival writer over the same prefix (a shared object store,
// where every replica of a shard writes one index object) makes the commit fail rather than
// overwrite: the loser then rebases on what was committed and tries again, so neither writer's
// part is dropped from the index that survives. It is a no-op for a head-only engine (no
// backend). Caller holds e.mu.
func (e *Engine) updateIndexLocked(ctx context.Context) error {
	if e.cfg.Backend == nil {
		return nil
	}

	for range indexCommitAttempts {
		ix := e.nextIndexLocked(ctx)

		version, err := ix.Save(ctx, e.cfg.Backend, e.indexKey(), e.indexVersion)
		if err == nil {
			e.indexVersion = version
			// Only a commit that landed discharges a want: the obligation is dropped from the
			// engine's own list here, never while building an index that may not be written.
			e.wants = ix.Wanted
			e.pendingWants = nil
			// Same rule for the blocks this attempt allocated: a part numbered before its CAS
			// landed would hold a block the winner took, and the retry would not re-allocate.
			for i := range e.pendingBlocks {
				a := &e.pendingBlocks[i]
				a.part.blocks, a.part.claim, a.part.level, a.part.pending = a.blocks, a.claim, a.level, nil
			}

			e.pendingBlocks = nil
			e.holes, e.lostParts = ix.Holes(), ix.LostParts
			e.allocated = ix.AllocatedBlocks
			e.pendingHoles = nil

			return nil
		}

		if !errors.Is(err, bucketindex.ErrConflict) {
			return errors.Wrap(err, "save bucket index")
		}

		if err := e.adoptIndexLocked(ctx); err != nil {
			return err
		}
	}

	return errors.Wrapf(bucketindex.ErrConflict,
		"commit bucket index after %d attempts", indexCommitAttempts)
}

// adoptIndexLocked rebases this engine on the index a rival writer committed: it takes that
// index's version (so the retry conditions on it), raises the generation above the rival's (so
// the retry supersedes rather than reads as stale), and records the entries the rival added as
// [Engine.foreign] so the retry carries them instead of dropping them. Caller holds e.mu.
func (e *Engine) adoptIndexLocked(ctx context.Context) error {
	ix, version, err := bucketindex.LoadVersioned(ctx, e.cfg.Backend, e.indexKey())
	if err != nil {
		return errors.Wrap(err, "reload bucket index")
	}

	e.indexVersion = version
	if ix.Generation.Compare(e.generation) > 0 {
		e.generation = ix.Generation
	}

	// The rival's slots, not this engine's watermark: e.flushedEpoch counts this node's own
	// flushes and nothing another writer committed can say anything about it.
	e.epochs, e.anonEpoch = ix.Epochs, ix.FlushedEpoch

	// The rival's wants are obligations over the same prefix, and this engine's commit is about to
	// rewrite the object holding them. They are unioned with this engine's rather than replacing
	// them: the losing commit never landed, so whatever it was going to record is still owed. A
	// want either side has already met is dropped again by the trim in [Engine.nextIndexLocked].
	merged := bucketindex.Index{Wanted: e.wants}
	for i := range ix.Wanted {
		merged.RecordWant(ix.Wanted[i])
	}

	e.wants = merged.Wanted

	// A rival's holes are this shard's losses too, and its loss count is a fact this commit must
	// not walk back: the counter only ever rises.
	e.holes = mergeHoles(e.holes, ix.Holes())
	e.lostParts = max(e.lostParts, ix.LostParts)
	e.allocated = max(e.allocated, ix.AllocatedBlocks)

	e.foreign = foreignEntries(ix.Entries, e.indexed, e.removals, e.wants)
	e.openForeignLocked(ctx)

	return nil
}

// mergeHoles unions the holes a rival writer committed into this engine's, keyed by prefix.
func mergeHoles(cur, other []bucketindex.Entry) []bucketindex.Entry {
	ix := bucketindex.Index{Entries: slices.Clone(cur)}
	for i := range other {
		ix.Add(other[i])
	}

	return ix.Entries
}

// openForeignLocked opens the parts of the entries this engine adopted, so the part set it can
// serve matches the index it is about to commit — the index names them, and until they are open
// only [Engine.LoadParts] would make them readable (#398).
//
// Already-open handles are reused, so a commit retrying under contention re-reads nothing, and one
// that cannot be opened (a rival that merged it away between the two commits) is left out of the
// readable set but kept in the index: the entry is what keeps the part reachable, and only its
// writer knows whether it is still live. Caller holds e.mu.
func (e *Engine) openForeignLocked(ctx context.Context) {
	if len(e.foreign) == 0 {
		e.foreignParts = nil

		return
	}

	open := make(map[string]*part, len(e.foreign))

	for i := range e.foreign {
		ent := &e.foreign[i]
		if p, ok := e.foreignParts[ent.Prefix]; ok {
			open[ent.Prefix] = p

			continue
		}

		p, err := openPart(ctx, e.cfg.Backend, ent.Prefix)
		if err != nil {
			zctx.From(ctx).Debug("adopted part is not readable here",
				zap.String("prefix", ent.Prefix), zap.Error(err))

			continue
		}

		p.minTime, p.maxTime = ent.MinTime, ent.MaxTime
		open[ent.Prefix] = p

		// Without its identities the part is open but unresolvable: matchers resolve through the
		// head's identity index, so the rows would be there and nothing would name them.
		if _, err := e.registerPartIdentitiesLocked(ctx, ent.Prefix); err != nil {
			zctx.From(ctx).Debug("adopted part identities are not readable here",
				zap.String("prefix", ent.Prefix), zap.Error(err))
		}
	}

	e.foreignParts = open
}

// readablePartsLocked is the part set a query may read: this engine's own, plus the ones it
// adopted from a rival writer's index. The two are kept apart because only the first are this
// engine's to merge, remove or delete — but both are named by the index it committed, so both must
// answer a read. Caller holds e.mu.
func (e *Engine) readablePartsLocked() []*part {
	if len(e.foreignParts) == 0 {
		return e.parts
	}

	out := make([]*part, 0, len(e.parts)+len(e.foreignParts))
	out = append(out, e.parts...)

	for _, p := range e.foreignParts {
		out = append(out, p)
	}

	return out
}

// nextIndexLocked builds the index state this engine wants committed and advances the
// bookkeeping the next one is diffed against. Caller holds e.mu.
func (e *Engine) nextIndexLocked(ctx context.Context) *bucketindex.Index {
	// The generation advances on every write, including one that only removes parts — which is
	// the whole point of it, since that is exactly the rewrite the part names cannot express.
	e.generation = e.generation.Next(e.term())

	ix := &bucketindex.Index{
		// The watermark goes into this writer's own slot, and every other writer's is carried
		// through untouched: they count flushes of WALs this engine has never seen, and a commit
		// that stamped its own number over one of them would make that node replay records its
		// parts already hold, or skip records it still holds only in memory (#397).
		FlushedEpoch: e.anonEpoch,
		Epochs:       slices.Clone(e.epochs),
		Generation:   e.generation,
	}
	ix.SetWriterEpoch(e.cfg.WriterID, e.flushedEpoch, e.generation)
	ix.Epochs = bucketindex.TrimWriters(ix.Epochs, e.cfg.WriterID, bucketindex.MaxWriters)
	e.epochs, e.anonEpoch = ix.Epochs, ix.FlushedEpoch

	// A rival writer's entries go in first, so this engine's own parts win any prefix collision.
	for i := range e.foreign {
		ix.Add(e.foreign[i])
	}

	// Block numbers are allocated here, per attempt, because a rival's entries are only known
	// after a rebase: allocating once and reusing it across retries would hand a part a block the
	// winner already claimed. The assignments are held, not applied — see [Engine.updateIndexLocked].
	next := e.nextBlockLocked(ix)
	groups := make(map[*splitGroup]*groupRun)

	var assigned []blockAssignment

	live := make(map[string]struct{}, len(e.parts))
	for _, p := range e.parts {
		blocks, claim, level := p.blocks, p.claim, p.level
		if id := p.pending; id != nil {
			blocks, claim = allocateBlocks(id, &next, groups)
			level = id.level

			assigned = append(assigned, blockAssignment{part: p, blocks: blocks, claim: claim, level: level})
		}

		ix.Add(bucketindex.Entry{
			Prefix: p.prefix, MinTime: p.minTime, MaxTime: p.maxTime,
			Blocks: blocks, Claim: claim, Level: level,
		})
		live[p.prefix] = struct{}{}
	}

	// One above the last block handed out, over every source this attempt considered — so the mark
	// only ever rises, and a shard whose live set has since emptied never renumbers over a part it
	// once held.
	ix.AllocatedBlocks = next - 1

	e.pendingBlocks = assigned

	// A part the last index named and this one does not was removed here, and says so. Absence
	// on its own is not evidence — a part missing from an index is either one a merge consumed
	// or one the node lost, and a reader cannot tell those apart without being told.
	for prefix := range e.indexed {
		if _, ok := live[prefix]; !ok {
			e.removals = append(e.removals, bucketindex.Removal{Prefix: prefix, Generation: e.generation})
		}
	}

	// A hole is revoked by the part turning up, so the trim runs against the entries this commit
	// publishes: every path that commits the data back — a repair fetch, a rival's entry adopted
	// under CAS, a merge — replaces the hole as a side effect of the commit. What survives counts
	// as indexed, or the next commit would read it as a removal.
	ix.LostParts = e.lostParts

	trimmed := bucketindex.TrimHoles(slices.Clone(e.holes), ix.Entries)
	for i := range trimmed {
		ix.Add(trimmed[i])
		live[trimmed[i].Prefix] = struct{}{}
	}

	// Acknowledging a loss is one index mutation, so the hole, the want it discharges and the
	// data-loss counter all land in the same CAS commit — or none of them do.
	for i := range e.pendingHoles {
		w := &e.pendingHoles[i]
		live[w.Prefix] = struct{}{}
		ix.RecordHole(*w)
	}

	e.removals = bucketindex.TrimRemovals(e.removals, live, bucketindex.MaxRemovals)
	ix.Removed = e.removals
	e.indexed = live

	// The same commit that drops an unreadable part from Entries states the obligation to get it
	// back: a part leaves Entries only into Removed or into Wanted, and one CAS commit carries both
	// halves, so no crash can land the drop without the want.
	ix.Wanted = slices.Clone(e.wants)

	for i := range e.pendingWants {
		w := e.pendingWants[i]
		w.Generation = e.generation
		ix.RecordWant(w)
	}

	// Committing a part is what discharges a want, so the trim runs against the entries this
	// commit publishes: a want naming a part the index holds again, or one a live part contains,
	// is repaired by the act of writing this index.
	ix.Wanted = bucketindex.TrimWants(ix.Wanted, ix.Entries)

	// Past the horizon the node needs the wholesale adoption cluster/partsync performs, not
	// part-by-part repair. The wants stay either way: each is the only record that its part is
	// owed, and the read policy disclaims over them.
	if len(ix.Wanted) > bucketindex.MaxWants {
		zctx.From(ctx).Warn("outstanding repairs exceed the part-by-part repair horizon",
			zap.String("prefix", e.cfg.Prefix), zap.Int("wanted", len(ix.Wanted)),
			zap.Int("horizon", bucketindex.MaxWants))
	}

	return ix
}

// foreignEntries returns the entries of a committed index that belong to another writer: neither
// a part this engine has ever indexed nor one it removed. Only that writer knows whether they are
// still live, so this engine's job is to carry them across its own commits — an entry dropped
// here leaves durable part objects unreferenced, and the next open-time orphan sweep deletes them.
func foreignEntries(
	entries []bucketindex.Entry, indexed map[string]struct{},
	removals []bucketindex.Removal, wants []bucketindex.Want,
) []bucketindex.Entry {
	var out []bucketindex.Entry

	for i := range entries {
		e := &entries[i]

		// A hole is carried through [Engine.holes], which is also what re-attempts and revokes it;
		// letting one in here would commit it twice and try to open objects that do not exist.
		if e.Hole {
			continue
		}

		if _, ours := indexed[e.Prefix]; ours {
			continue
		}

		if slices.ContainsFunc(removals, func(r bucketindex.Removal) bool { return r.Prefix == e.Prefix }) {
			continue
		}

		// A part this engine already found unreadable is not adopted back: carrying its entry
		// forward would put it in Entries again and discharge the want that names it, leaving the
		// index pointing at bytes no one has.
		if slices.ContainsFunc(wants, func(w bucketindex.Want) bool { return w.Prefix == e.Prefix }) {
			continue
		}

		out = append(out, *e)
	}

	return out
}

// LoadParts reconstructs the engine's durable state from the object store: the part set from
// the bucket index, and the series identity index (postings + labels) from the persisted
// identity object. It is how a fresh engine over an existing prefix serves reads with no
// in-memory state carried over from the writer (the stateless read path); typically called
// once after [New] during recovery. WAL [Engine.Replay] is complementary — it restores the
// unflushed head samples — but is not required to query flushed data.
//
// It replaces any current parts. A head-only engine (no backend) is a no-op. It assumes this node owns the prefix — it
// sweeps the part objects the index does not name (see [Engine.sweepOrphansLocked]) and commits a
// want for each part the index names that is not there.
func (e *Engine) LoadParts(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.loadPartsLocked(ctx, loadOwner)
}

// LoadPartsUnclaimed is [Engine.LoadParts] for a node whose authority over the prefix is not yet
// established (a cluster node recovering before it holds any claim): a part the index names but the
// backend lacks becomes a pending want, committed by the first commit this engine makes as a writer
// rather than now. Reads over it disclaim meanwhile ([Engine.WantOverlaps]).
func (e *Engine) LoadPartsUnclaimed(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.loadPartsLocked(ctx, loadUnclaimed)
}

// loadMode is what a load may do about a part the index names but the backend does not hold.
type loadMode uint8

const (
	// loadReplica keeps the part as a pending want and sweeps nothing: a replica shares the prefix
	// with the owner, whose in-flight (not yet committed) part must not be deleted underneath it.
	// The want reaches an index only through the first commit this engine makes as a writer.
	loadReplica loadMode = iota
	// loadUnclaimed sweeps orphans and keeps the part as a pending want for the engine's first
	// commit, which only an owner makes.
	loadUnclaimed
	// loadOwner sweeps orphans and commits the want at once.
	loadOwner
)

// livePart resolves the handle for ent, taking it out of open (the previous load's part set) when
// that handle is still good. Parts are immutable and the prefix is their identity, so an open handle
// for a prefix the reloaded index still names is still valid — reusing it keeps the caches it built,
// above all the per-series watermarks whose alternative is a whole timestamp column per part per
// refresh. Reuse cannot skip noticing that the part's objects went away, though, so the handle is
// probed first; a part that is gone is left in open (it counts as dropped) and reported through
// [partGone], which the caller turns into a repair want.
func (e *Engine) livePart(
	ctx context.Context, ent *bucketindex.Entry, open map[string]*part,
) (*part, error) {
	if p := open[ent.Prefix]; p != nil {
		live, err := block.PartPresent(ctx, e.cfg.Backend, ent.Prefix)
		if err != nil {
			return nil, errors.Wrapf(err, "probe part %q", ent.Prefix)
		}

		if live {
			delete(open, ent.Prefix)

			return p, nil
		}
	}

	p, err := openPart(ctx, e.cfg.Backend, ent.Prefix)
	if err != nil {
		return nil, errors.Wrapf(err, "open part %q", ent.Prefix)
	}

	return p, nil
}

// loadPartsLocked is [Engine.LoadParts] under a [loadMode]. Caller holds e.mu.
func (e *Engine) loadPartsLocked(ctx context.Context, mode loadMode) error {
	sweep := mode != loadReplica

	if e.cfg.Backend == nil {
		return nil
	}

	ix, version, err := bucketindex.LoadVersioned(ctx, e.cfg.Backend, e.indexKey())
	if err != nil {
		return errors.Wrap(err, "load bucket index")
	}

	// The loaded state is now the one this engine's next commit conditions on, and it accounts
	// for every entry the index names, so nothing is foreign any more.
	e.indexVersion = version
	e.foreign = nil
	// Everything the index names is opened below, so the adopted handles have no separate life
	// left. Their objects belong to their writer and are not deleted here.
	e.foreignParts = nil

	parts := make([]*part, 0, len(ix.Entries))

	var (
		holes []bucketindex.Entry
		lost  []bucketindex.Want
	)

	// The previous load's handles, which [Engine.livePart] reuses and takes out of the map; each is
	// therefore claimed by at most one entry, and whatever is left over was dropped by this index.
	open := make(map[string]*part, len(e.parts))
	for _, p := range e.parts {
		open[p.prefix] = p
	}

	for i := range ix.Entries {
		ent := &ix.Entries[i]

		// A hole names no objects, so there is nothing to open: it is carried as what it is, an
		// acknowledged loss the next repair pass re-attempts.
		if ent.Hole {
			holes = append(holes, *ent)

			continue
		}

		p, err := e.livePart(ctx, ent, open)
		if err != nil {
			if !partGone(err) {
				return err
			}

			zctx.From(ctx).Error("part named by the index is gone; recording a repair",
				zap.String("prefix", ent.Prefix), zap.Error(err))

			lost = append(lost, bucketindex.WantOf(*ent, e.generation))

			continue
		}

		p.minTime, p.maxTime = ent.MinTime, ent.MaxTime
		p.blocks, p.claim, p.level = ent.Blocks, ent.Claim, ent.Level
		// The index names the part, so whatever identity it was waiting for has been committed.
		p.pending = nil
		parts = append(parts, p)
	}

	e.holes, e.lostParts, e.allocated = holes, ix.LostParts, ix.AllocatedBlocks

	// A part disappearing means identities may have died with it, which is what arms the identity
	// prune on a node that never merges (a replica adopting the owner's part set).
	if len(open) > 0 {
		e.identityDirty = true
	}

	e.parts = parts
	e.flushedEpoch = ix.WriterEpoch(e.cfg.WriterID)
	e.epochs, e.anonEpoch = ix.Epochs, ix.FlushedEpoch
	e.generation = ix.Generation
	e.removals = ix.Removed
	e.wants, e.pendingWants = ix.Wanted, lost
	e.indexed = make(map[string]struct{}, len(parts))

	// Only the parts that opened: a lost one left out of e.indexed is what keeps the commit below
	// from also calling it a removal, which would restate a loss as a deliberate deletion.
	for _, p := range parts {
		e.indexed[p.prefix] = struct{}{}
	}

	// New head records belong to the generation past the recovered watermark; replay (which the
	// facade runs next) then skips everything the loaded parts already hold.
	if e.cfg.WAL != nil {
		e.cfg.WAL.SetEpoch(e.flushedEpoch + 1)
	}

	if sweep {
		if err := e.sweepOrphansLocked(ctx); err != nil {
			return err
		}
	}

	// One commit drops the gone parts from Entries and states the wants that replace them. It runs
	// before the rest of the load so the obligation is durable even if identity recovery fails.
	if len(lost) > 0 && mode == loadOwner {
		if err := e.updateIndexLocked(ctx); err != nil {
			return errors.Wrap(err, "record repair wants")
		}
	}

	complete, err := e.loadIdentitiesLocked(ctx, parts)
	if err != nil {
		return err
	}

	// Once every live part carries its own identities, the legacy whole-set object holds nothing
	// live that the parts do not — only identities whose data is gone. Deleting it completes the
	// migration for a prefix written by an older build, and stops recovery from resurrecting dead
	// identities on every restart.
	if complete && sweep {
		if err := e.cfg.Backend.Delete(ctx, e.seriesKey()); err != nil && !errors.Is(err, backend.ErrNotExist) {
			return errors.Wrap(err, "delete legacy series index")
		}
	}

	return nil
}

// RefreshReplica brings a replica node's view up to date with the shared object store: it
// reconstructs the flushed parts from the bucket index and trims its head to the
// still-unflushed window — samples a primary has already flushed (covered by a part) are
// dropped, bounding replica memory. With no shared store (this node cannot see the parts), it
// is a safe no-op: nothing loads, so nothing is trimmed. A part the index names but the store
// lacks is not an error: it leaves the part set and is counted as a pending want ([Stats.WantedParts],
// [Engine.WantOverlaps]) until a refresh finds it again or this node commits as an owner.
func (e *Engine) RefreshReplica(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if err := e.loadPartsLocked(ctx, loadReplica); err != nil {
		return err
	}

	if len(e.parts) == 0 {
		return nil
	}

	covered := make(map[signal.SeriesID]int64)

	// Each series is trimmed against its own newest flushed timestamp, not the newest in the part
	// set: a series absent from every part keeps its whole head (a late-registered series whose
	// timestamps overlap already-flushed data — ordinary backfill — would otherwise lose its only
	// replicated copies), and one present keeps everything past what is durable *for it*.
	for _, p := range e.parts {
		err := p.forEachSeriesMaxTime(ctx, func(id signal.SeriesID, t int64) {
			if prev, ok := covered[id]; !ok || t > prev {
				covered[id] = t
			}
		})
		if err != nil {
			return errors.Wrapf(err, "series times of part %q", p.prefix)
		}
	}

	e.head.trimBelowCovered(covered)

	return nil
}

// loadSeriesIndexLocked registers the identities of the **legacy** whole-set object written by
// builds before identity was part-scoped. A missing object — every prefix this build wrote — is a
// no-op. It is read once per open and deleted as soon as every live part carries its own
// identities (see [Engine.loadIdentitiesLocked]). Caller holds e.mu.
func (e *Engine) loadSeriesIndexLocked(ctx context.Context) error {
	data, err := backend.ReadUncached(ctx, e.cfg.Backend, e.seriesKey())
	if err != nil {
		if errors.Is(err, backend.ErrNotExist) {
			return nil
		}

		return errors.Wrap(err, "read series index")
	}

	if err := decodeSeriesSet(data, e.head.registerSeries); err != nil {
		return errors.Wrap(err, "decode series index")
	}

	return nil
}

// decodeSeriesSet parses the legacy whole-set identity object, calling fn for each identity. It is
// defensive against truncated input. Nothing writes this format any more; it is read to migrate a
// prefix written by an older build.
func decodeSeriesSet(data []byte, fn func(signal.Series)) error {
	count, n := binary.Uvarint(data)
	if n <= 0 {
		return errors.Wrap(signal.ErrMalformed, "series count")
	}
	data = data[n:]

	for range count {
		l, n := binary.Uvarint(data)
		if n <= 0 || l > uint64(len(data)-n) {
			return errors.Wrap(signal.ErrMalformed, "series length")
		}
		data = data[n:]

		s, _, err := signal.DecodeSeries(data[:l])
		if err != nil {
			return errors.Wrap(err, "decode series")
		}

		fn(s)
		data = data[l:]
	}

	return nil
}

// colsTimeRange returns the inclusive min/max timestamp across cols (which has ≥ 1 sample
// when a part is written).
func colsTimeRange(cols *flushColumns) (minTime, maxTime int64) {
	minTime, maxTime = maxInt64, minInt64
	for _, t := range cols.ts {
		if t < minTime {
			minTime = t
		}

		if t > maxTime {
			maxTime = t
		}
	}

	return minTime, maxTime
}
