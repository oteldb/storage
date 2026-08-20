package recordengine

import (
	"context"
	"encoding/binary"
	"slices"

	"github.com/go-faster/errors"

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
		version, err := e.nextIndexLocked().Save(ctx, e.cfg.Backend, e.indexKey(), e.indexVersion)
		if err == nil {
			e.indexVersion = version

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

	e.foreign = foreignEntries(ix.Entries, e.indexed, e.removals)

	return nil
}

// nextIndexLocked builds the index state this engine wants committed and advances the
// bookkeeping the next one is diffed against. Caller holds e.mu.
func (e *Engine) nextIndexLocked() *bucketindex.Index {
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
		ix.Add(bucketindex.Entry{Prefix: p.prefix, MinTime: p.minTime, MaxTime: p.maxTime})
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

	e.removals = bucketindex.TrimRemovals(e.removals, live, bucketindex.MaxRemovals)
	ix.Removed = e.removals
	e.indexed = live

	return ix
}

// foreignEntries returns the entries of a committed index that belong to another writer: neither
// a part this engine has ever indexed nor one it removed. Only that writer knows whether they are
// still live, so this engine's job is to carry them across its own commits — an entry dropped
// here leaves durable part objects unreferenced, and the next open-time orphan sweep deletes them.
func foreignEntries(
	entries []bucketindex.Entry, indexed map[string]struct{}, removals []bucketindex.Removal,
) []bucketindex.Entry {
	var out []bucketindex.Entry

	for _, e := range entries {
		if _, ours := indexed[e.Prefix]; ours {
			continue
		}

		if slices.ContainsFunc(removals, func(r bucketindex.Removal) bool { return r.Prefix == e.Prefix }) {
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

	parts := make([]*part, 0, len(ix.Entries))

	for _, ent := range ix.Entries {
		p, err := openPart(ctx, e.cfg.Backend, e.cfg.Schema, ent.Prefix)
		if err != nil {
			return errors.Wrapf(err, "open part %q", ent.Prefix)
		}

		p.minTime, p.maxTime = ent.MinTime, ent.MaxTime
		parts = append(parts, p)
	}

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
	e.indexed = make(map[string]struct{}, len(ix.Entries))

	for _, entry := range ix.Entries {
		e.indexed[entry.Prefix] = struct{}{}
	}

	if sweep {
		if err := e.sweepOrphansLocked(ctx); err != nil {
			return err
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
// reconstructs the flushed parts and trims its head to the still-unflushed window. With no shared
// store, a safe no-op.
func (e *Engine) RefreshReplica(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if err := e.loadPartsLocked(ctx, false); err != nil {
		return err
	}

	if len(e.parts) == 0 {
		return nil
	}

	maxT := minInt64
	covered := make(map[signal.SeriesID]struct{})

	for _, p := range e.parts {
		if p.maxTime > maxT {
			maxT = p.maxTime
		}

		// Trim only streams the parts hold (in-memory ranges — no I/O); an unflushed stream's
		// head must survive the refresh, or the primary becomes its sole holder.
		for _, sr := range p.ranges {
			covered[sr.id] = struct{}{}
		}
	}

	e.head.trimBelowCovered(maxT, covered)

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
