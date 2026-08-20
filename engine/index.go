package engine

import (
	"context"
	"encoding/binary"
	"slices"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/signal"
)

// The engine maintains a [bucketindex] alongside its parts so the part set is durable and a
// node can reconstruct it from the object store without local state (the object-store-native
// read path). The index is rewritten on every flush and merge; a single-node engine
// overwrites it (multi-writer commits would use backend CAS, a later milestone).

// indexKey is the backend key of this engine's bucket index.
func (e *Engine) indexKey() string {
	return e.cfg.Prefix + "/" + bucketindex.Object
}

// seriesKey is the backend key of this engine's persisted identity index (the series
// labels). Parts store only series ids; the labels needed to resolve matchers and to label
// fetched batches live here, so a stateless reader can rebuild the postings/series index
// without the (local) WAL.
func (e *Engine) seriesKey() string { return e.cfg.Prefix + "/series.bin" }

// updateIndexLocked rewrites the bucket index to match the engine's current parts. It is a
// no-op for a head-only engine (no backend). Caller holds e.mu.
func (e *Engine) updateIndexLocked(ctx context.Context) error {
	if e.cfg.Backend == nil {
		return nil
	}

	// The generation advances on every write, including one that only removes parts — which is
	// the whole point of it, since that is exactly the rewrite the part names cannot express.
	e.generation = e.generation.Next(e.term())

	ix := &bucketindex.Index{FlushedEpoch: e.flushedEpoch, Generation: e.generation}

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

	if err := ix.Save(ctx, e.cfg.Backend, e.indexKey()); err != nil {
		return errors.Wrap(err, "save bucket index")
	}

	return nil
}

// LoadParts reconstructs the engine's durable state from the object store: the part set from
// the bucket index, and the series identity index (postings + labels) from the persisted
// identity object. It is how a fresh engine over an existing prefix serves reads with no
// in-memory state carried over from the writer (the stateless read path); typically called
// once after [New] during recovery. WAL [Engine.Replay] is complementary — it restores the
// unflushed head samples — but is not required to query flushed data.
//
// It replaces any current parts. A head-only engine (no backend) is a no-op. It assumes this node owns the prefix — it
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

	for _, ent := range ix.Entries {
		p, err := openPart(ctx, e.cfg.Backend, ent.Prefix)
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
	e.flushedEpoch = ix.FlushedEpoch
	e.generation = ix.Generation
	e.removals = ix.Removed
	e.indexed = make(map[string]struct{}, len(ix.Entries))

	for _, entry := range ix.Entries {
		e.indexed[entry.Prefix] = struct{}{}
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
// is a safe no-op: nothing loads, so nothing is trimmed.
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

		// The trim must only touch series the parts actually hold: a head series that is not
		// flushed anywhere (a late-registered series whose timestamps overlap already-flushed
		// data — ordinary backfill) would otherwise lose its only replicated copies, leaving the
		// primary as the sole holder of quorum-acked samples until its next flush.
		if err := p.index.forEachID(ctx, func(id signal.SeriesID) { covered[id] = struct{}{} }); err != nil {
			return errors.Wrapf(err, "enumerate series of part %q", p.prefix)
		}
	}

	e.head.trimBelowCovered(maxT, covered)

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
