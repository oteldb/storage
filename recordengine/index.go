package recordengine

import (
	"context"
	"encoding/binary"
	"slices"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"go.uber.org/zap"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/signal"
)

// The engine keeps a [bucketindex] of its parts and a persisted stream-identity object alongside
// them, so a stateless node reconstructs both the part set and the postings/labels from the object
// store with no local state. Both are rewritten on every flush and merge; the bucket index goes
// through the backend's compare-and-swap, so two writers over one prefix cannot overwrite each
// other's parts.

func (e *Engine) indexKey() string  { return e.cfg.Prefix + "/" + bucketindex.Object }
func (e *Engine) streamKey() string { return e.cfg.Prefix + "/streams.bin" }

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
			e.holes, e.lostParts = ix.Holes(), ix.LostParts
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
	for _, w := range ix.Wanted {
		merged.RecordWant(w)
	}

	e.wants = merged.Wanted

	// A rival's holes are this shard's losses too, and its loss count is a fact this commit must
	// not walk back: the counter only ever rises.
	e.holes = mergeHoles(e.holes, ix.Holes())
	e.lostParts = max(e.lostParts, ix.LostParts)

	e.foreign = foreignEntries(ix.Entries, e.indexed, e.removals, e.wants)
	e.openForeignLocked(ctx)

	return nil
}

// mergeHoles unions the holes a rival writer committed into this engine's, keyed by prefix.
func mergeHoles(cur, other []bucketindex.Entry) []bucketindex.Entry {
	ix := bucketindex.Index{Entries: slices.Clone(cur)}
	for _, h := range other {
		ix.Add(h)
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

	for _, ent := range e.foreign {
		if p, ok := e.foreignParts[ent.Prefix]; ok {
			open[ent.Prefix] = p

			continue
		}

		p, err := openPart(ctx, e.cfg.Backend, e.cfg.Schema, ent.Prefix)
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
	for _, ent := range e.foreign {
		ix.Add(ent)
	}

	live := make(map[string]struct{}, len(e.parts))
	for _, p := range e.parts {
		ix.Add(bucketindex.Entry{
			Prefix: p.prefix, MinTime: p.minTime, MaxTime: p.maxTime,
			Blocks: p.blocks, Level: p.level,
		})
		live[p.prefix] = struct{}{}
	}

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

	for _, h := range bucketindex.TrimHoles(slices.Clone(e.holes), ix.Entries) {
		ix.Add(h)
		live[h.Prefix] = struct{}{}
	}

	// Acknowledging a loss is one index mutation, so the hole, the want it discharges and the
	// data-loss counter all land in the same CAS commit — or none of them do.
	for _, w := range e.pendingHoles {
		live[w.Prefix] = struct{}{}
		ix.RecordHole(w)
	}

	e.removals = bucketindex.TrimRemovals(e.removals, live, bucketindex.MaxRemovals)
	ix.Removed = e.removals
	e.indexed = live

	// The same commit that drops an unreadable part from Entries states the obligation to get it
	// back: a part leaves Entries only into Removed or into Wanted, and one CAS commit carries both
	// halves, so no crash can land the drop without the want.
	ix.Wanted = slices.Clone(e.wants)

	for _, w := range e.pendingWants {
		w.Generation = e.generation
		ix.RecordWant(w)
	}

	// Committing a part is what discharges a want, so the trim runs against the entries this
	// commit publishes: a want naming a part the index holds again, or one a live part contains,
	// is repaired by the act of writing this index.
	kept, dropped := bucketindex.TrimWants(ix.Wanted, ix.Entries, bucketindex.MaxWants)
	if len(dropped) > 0 {
		// Past the bound a node can no longer repair part by part and needs a wholesale reseed,
		// which does not exist yet; losing the obligation quietly is what must not happen.
		zctx.From(ctx).Warn("outstanding repairs exceed the index bound",
			zap.String("prefix", e.cfg.Prefix), zap.Int("dropped", len(dropped)),
			zap.Int("kept", len(kept)))
	}

	ix.Wanted = kept

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

	for _, e := range entries {
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

		out = append(out, e)
	}

	return out
}

// LoadParts reconstructs the engine's durable state from the object store: the part set from the
// bucket index and the stream identity index from the persisted object. A head-only engine is a
// no-op. Replaces current parts. It assumes this node owns the prefix — it
// sweeps the part objects the index does not name (see [Engine.sweepOrphansLocked]).
func (e *Engine) LoadParts(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.loadPartsLocked(ctx, true)
}

// loadPartsLocked is [Engine.LoadParts] with the orphan sweep made optional: a replica shares the
// prefix with the owner, whose in-flight (not yet committed) part must not be deleted underneath it.
// Caller holds e.mu.
func (e *Engine) loadPartsLocked(ctx context.Context, sweep bool) error {
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

	for _, ent := range ix.Entries {
		// A hole names no objects, so there is nothing to open: it is carried as what it is, an
		// acknowledged loss the next repair pass re-attempts.
		if ent.Hole {
			holes = append(holes, ent)

			continue
		}

		p, err := openPart(ctx, e.cfg.Backend, e.cfg.Schema, ent.Prefix)
		if err != nil {
			if !sweep || !partGone(err) {
				return errors.Wrapf(err, "open part %q", ent.Prefix)
			}

			zctx.From(ctx).Error("part named by the index is gone; recording a repair",
				zap.String("prefix", ent.Prefix), zap.Error(err))

			lost = append(lost, bucketindex.WantOf(ent, e.generation))

			continue
		}

		p.minTime, p.maxTime = ent.MinTime, ent.MaxTime
		p.blocks, p.level = ent.Blocks, ent.Level
		parts = append(parts, p)
	}

	e.holes, e.lostParts = holes, ix.LostParts

	// A part disappearing means identities may have died with it, which is what arms the identity
	// prune on a node that never merges (a replica adopting the owner's part set).
	for _, p := range e.parts {
		if !slices.ContainsFunc(parts, func(n *part) bool { return n.prefix == p.prefix }) {
			e.identityDirty = true

			break
		}
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

	if sweep {
		if err := e.sweepOrphansLocked(ctx); err != nil {
			return err
		}
	}

	// One commit drops the gone parts from Entries and states the wants that replace them. It runs
	// before the rest of the load so the obligation is durable even if identity recovery fails.
	if len(lost) > 0 {
		if err := e.updateIndexLocked(ctx); err != nil {
			return errors.Wrap(err, "record repair wants")
		}
	}

	// New head records belong to the generation past the recovered watermark; replay (which the
	// facade runs next) then skips everything the loaded parts already hold.
	if e.cfg.WAL != nil {
		e.cfg.WAL.SetEpoch(e.flushedEpoch + 1)
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
		if err := e.cfg.Backend.Delete(ctx, e.streamKey()); err != nil && !errors.Is(err, backend.ErrNotExist) {
			return errors.Wrap(err, "delete legacy stream index")
		}
	}

	return nil
}

// RefreshReplica brings a replica node's view up to date with the shared object store: it
// reconstructs the flushed parts and trims its head to the still-unflushed window, stream by stream
// (see [head.trimBelowCovered]). With no shared store, a safe no-op.
func (e *Engine) RefreshReplica(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if err := e.loadPartsLocked(ctx, false); err != nil {
		return err
	}

	if len(e.parts) == 0 {
		return nil
	}

	covered := make(map[signal.SeriesID]int64)

	// Each stream is trimmed against its own newest flushed timestamp, not the newest in the part
	// set: a stream absent from every part keeps its whole head (the primary would otherwise be the
	// sole holder of quorum-acked records), and one present keeps everything past what is durable
	// *for it*.
	for _, p := range e.parts {
		maxTS, err := p.streamMaxTimes(ctx)
		if err != nil {
			return err
		}

		for i, sr := range p.ranges {
			if t, ok := covered[sr.id]; !ok || maxTS[i] > t {
				covered[sr.id] = maxTS[i]
			}
		}
	}

	e.head.trimBelowCovered(covered)

	return nil
}

// loadStreamIndexLocked registers the identities of the **legacy** whole-set object written by
// builds before identity was part-scoped. A missing object — every prefix this build wrote — is a
// no-op. It is read once per open and deleted as soon as every live part carries its own
// identities (see [Engine.loadIdentitiesLocked]). Caller holds e.mu.
func (e *Engine) loadStreamIndexLocked(ctx context.Context) error {
	data, err := e.cfg.Backend.Read(ctx, e.streamKey())
	if err != nil {
		if errors.Is(err, backend.ErrNotExist) {
			return nil
		}

		return errors.Wrap(err, "read stream index")
	}

	if err := decodeSeriesSet(data, e.head.registerStream); err != nil {
		return errors.Wrap(err, "decode stream index")
	}

	return nil
}

// decodeSeriesSet parses the legacy whole-set stream identity object, calling fn for each identity.
// Nothing writes this format any more; it is read to migrate a prefix written by an older build.
func decodeSeriesSet(data []byte, fn func(signal.Series)) error {
	count, n := binary.Uvarint(data)
	if n <= 0 {
		return errors.Wrap(signal.ErrMalformed, "stream count")
	}

	data = data[n:]

	for range count {
		l, n := binary.Uvarint(data)
		if n <= 0 || l > uint64(len(data)-n) {
			return errors.Wrap(signal.ErrMalformed, "stream length")
		}

		data = data[n:]

		s, _, err := signal.DecodeSeries(data[:l])
		if err != nil {
			return errors.Wrap(err, "decode stream")
		}

		fn(s)
		data = data[l:]
	}

	return nil
}
