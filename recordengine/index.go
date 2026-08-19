package recordengine

import (
	"context"
	"encoding/binary"
	"path"
	"slices"
	"strconv"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/signal"
)

// The engine keeps a [bucketindex] of its parts and a persisted stream-identity object alongside
// them, so a stateless node reconstructs both the part set and the postings/labels from the object
// store with no local state. Both are rewritten on every flush and merge.

func (e *Engine) indexKey() string  { return e.cfg.Prefix + "/" + bucketindex.Object }
func (e *Engine) streamKey() string { return e.cfg.Prefix + "/streams.bin" }

// updateIndexLocked commits a bucket index matching the engine's current parts. It is a no-op for
// a head-only engine (no backend). Caller holds e.mu.
//
// The commit claims a generation-named object, so a peer writing the same prefix — every replica
// of a shard does, over a shared store — cannot silently overwrite it. Losing the claim reloads
// and rebuilds on what did get committed, which is why the build is a closure ([bucketindex.Commit]
// runs it once per attempt); an exhausted retry returns an error, so a flush never reports a
// success whose index is not in the store.
func (e *Engine) updateIndexLocked(ctx context.Context) error {
	if e.cfg.Backend == nil {
		return nil
	}

	// The part set this engine last saw committed, held across retries: entries it names that
	// this engine no longer has are the ones *it* removed, whichever index a retry rebuilds on.
	known := make(map[string]struct{})
	if e.committed != nil {
		for _, ent := range e.committed.Entries {
			known[ent.Prefix] = struct{}{}
		}
	}

	ix, err := bucketindex.Commit(ctx, e.cfg.Backend, e.indexKey(), e.term(), e.committed,
		func(base *bucketindex.Index, g bucketindex.Generation) *bucketindex.Index {
			return e.buildIndexLocked(base, g, known)
		})
	if err != nil {
		return errors.Wrap(err, "commit bucket index")
	}

	e.committed = ix

	return nil
}

// buildIndexLocked renders the index this engine's parts imply on top of base, the state actually
// committed. known is the part set this engine last saw committed. Caller holds e.mu.
func (e *Engine) buildIndexLocked(
	base *bucketindex.Index,
	g bucketindex.Generation,
	known map[string]struct{},
) *bucketindex.Index {
	ix := &bucketindex.Index{FlushedEpoch: e.flushedEpoch, Generation: g}

	live := make(map[string]struct{}, len(e.parts))
	for _, p := range e.parts {
		ix.Add(bucketindex.Entry{Prefix: p.prefix, MinTime: p.minTime, MaxTime: p.maxTime})
		live[p.prefix] = struct{}{}
	}

	removals := slices.Clone(base.Removed)

	// A part the last index named and this one does not was removed here, and says so. Absence
	// on its own is not evidence — a part missing from an index is either one a merge consumed
	// or one the node lost, and a reader cannot tell those apart without being told.
	for prefix := range known {
		if _, ok := live[prefix]; !ok {
			removals = append(removals, bucketindex.Removal{Prefix: prefix, Generation: g})
		}
	}

	// Everything else the committed index names belongs to another writer over the same prefix,
	// and is carried forward: this engine cannot serve those parts, but rewriting the index from
	// its own part set alone would unlink parts whose objects are in the store, and the next
	// orphan sweep would then delete them.
	for _, ent := range base.Entries {
		if _, ok := live[ent.Prefix]; ok {
			continue
		}

		if _, ok := known[ent.Prefix]; ok {
			continue
		}

		ix.Add(ent)
		live[ent.Prefix] = struct{}{}
	}

	ix.Removed = bucketindex.TrimRemovals(removals, live, bucketindex.MaxRemovals)

	return ix
}

// LoadParts reconstructs the engine's durable state from the object store: the part set from the
// bucket index and the stream identity index from the persisted object. A head-only engine is a
// no-op. Replaces current parts and advances the sequence. It assumes this node owns the prefix — it
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

	ix, err := bucketindex.Load(ctx, e.cfg.Backend, e.indexKey())
	if err != nil {
		return errors.Wrap(err, "load bucket index")
	}

	parts := make([]*part, 0, len(ix.Entries))
	maxSeq := -1

	for _, ent := range ix.Entries {
		p, err := openPart(ctx, e.cfg.Backend, e.cfg.Schema, ent.Prefix)
		if err != nil {
			return errors.Wrapf(err, "open part %q", ent.Prefix)
		}

		p.minTime, p.maxTime = ent.MinTime, ent.MaxTime
		parts = append(parts, p)

		if s := seqOfPrefix(ent.Prefix); s > maxSeq {
			maxSeq = s
		}
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
	e.nextSeq = maxSeq + 1
	e.flushedEpoch = ix.FlushedEpoch
	e.committed = ix

	if sweep {
		next, err := e.sweepOrphansLocked(ctx, maxSeq)
		if err != nil {
			return err
		}

		e.nextSeq = next
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

// seqOfPrefix parses the trailing sequence number of a part prefix, or -1 if not numeric.
func seqOfPrefix(prefix string) int {
	n, err := strconv.Atoi(path.Base(prefix))
	if err != nil {
		return -1
	}

	return n
}
