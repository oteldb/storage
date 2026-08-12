package engine

import (
	"context"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"go.uber.org/zap"

	"github.com/oteldb/storage/index/series"
	"github.com/oteldb/storage/signal"
)

// Identity pruning bounds the resident index, which otherwise spans every series the process has
// ever seen: retention drops samples and whole parts, but nothing removed the identities that named
// them, so a tenant churning series (instance-id-shaped attributes) accumulated symbols, index
// entries, postings and OOO watermarks for the process' lifetime — memory `HeadBytes` does not
// report and no flush returns (see `Stats.IdentityBytes`).
//
// The live set needs no new on-disk format: a part already carries its sorted series ids in the
// `sidx` sidecar, so the identities still backed by data are the union of every live part's id set
// with the in-memory tiers (head buffers, a mid-flush detachment, the recent tier).
//
// Symbols are dense ids referenced by the postings lists, so nothing can be deleted in place —
// dropping one symbol would renumber the rest. The prune therefore **rebuilds**: fresh symbol
// table, series index and postings, populated from the surviving identities and swapped in.
// Prometheus' `MemPostings.Delete` (with its periodic lock yielding) is the alternative shape; it
// does not apply here because Prometheus has no head symbol table to renumber.
//
// The rebuild is far too expensive to hold the engine lock for — re-interning every survivor costs
// seconds at a million series — so it runs **off-lock** against the series index's append-only
// entry log ([series.Index.Snapshot]): registration only ever appends, so a snapshot of that log is
// immutable without the lock, and whatever registers meanwhile is picked up as the tail past its
// length when the result is installed. What the lock actually covers is the snapshot, the tail
// replay and the swap — work proportional to concurrent registrations, not to cardinality.
//
// The per-series out-of-order watermarks are the exception: an ordinary map, so they are pruned by
// deleting the dead keys in place rather than rebuilt.

const (
	// pruneMinIdentities is the identity count below which a prune is never worth its rebuild: a
	// small index is not the memory problem, and the rebuild is O(live identities).
	pruneMinIdentities = 1 << 12
	// pruneDeadNum / pruneDeadDen is the share of the identity set that must be dead before a
	// rebuild runs. Under it the reclaim does not pay for re-interning the survivors.
	pruneDeadNum = 1
	pruneDeadDen = 4
)

// PruneIdentities drops the identities no live data names any more — those the retention side of a
// merge left behind — rebuilding the resident index around the survivors and shrinking the durable
// identity object to match. It returns the number of identities removed (0 when nothing could have
// died, when too few have, or when the engine holds no parts).
//
// Only the owner of the data may call it: the live set is derived from *this node's* parts, so a
// replica that has not yet synced a part would prune identities the owner still serves. It takes
// the same flush/merge exclusion as those paths, so no publish can add an identity underneath it,
// and it holds the engine lock across the rebuild — the same order as the full-set walk a flush
// already performs under that lock, and the swap must be atomic against ingest regardless.
func (e *Engine) PruneIdentities(ctx context.Context) (int, error) {
	if e.cfg.Backend == nil {
		return 0, nil // head-only: nothing is flushed, so no identity can have outlived its data
	}

	e.flushMu.Lock()
	defer e.flushMu.Unlock()

	e.mu.RLock()
	dirty, parts, total := e.identityDirty, e.parts, e.head.series.Len()
	e.mu.RUnlock()

	// Identities die only when retention drops rows or parts, so an engine that has merged nothing
	// away since the last prune skips even the live-set walk. An idle tenant pays nothing.
	if !dirty || total < pruneMinIdentities {
		return 0, nil
	}

	live, err := e.liveIdentities(ctx, parts)
	if err != nil {
		return 0, err
	}

	e.mu.RLock()
	snap := e.head.series.Snapshot()
	e.mu.RUnlock()

	dead := deadPositions(snap, live)
	if len(dead)*pruneDeadDen < total*pruneDeadNum {
		// Too little to reclaim for the rebuild's cost. The flag stays clear: the next merge that
		// drops data sets it again, and until then the answer cannot change.
		e.clearIdentityDirty()

		return 0, nil
	}

	// The expensive part — re-interning every survivor into fresh structures — runs here, off the
	// lock, over the immutable snapshot. Ingest and queries proceed against the current index
	// throughout.
	rebuilt := rebuildIdentity(snap, live)

	e.mu.Lock()
	// Identities registered while the rebuild ran sit past the snapshot's end in the same
	// append-only log; a series that merely regained samples leaves no such entry, so the swap
	// re-checks the dead set against the live buffers too.
	e.head.swapIdentity(rebuilt, snap, e.head.series.Snapshot()[len(snap):], dead)
	// The durable object is no longer a superset of what it should hold, and the "identity count
	// unchanged ⇒ already byte-identical" skip cannot see a set that shrank and regrew to the same
	// size, so the next write is forced rather than inferred.
	e.seriesWritten = -1
	err = e.writeSeriesIndexLocked(ctx)
	e.identityDirty = false
	remaining := e.head.series.Len()
	e.mu.Unlock()

	removed := len(dead)

	if err != nil {
		return removed, err
	}

	zctx.From(ctx).Debug("pruned dead identities",
		zap.String("prefix", e.cfg.Prefix), zap.Int("removed", removed), zap.Int("live", remaining))

	return removed, nil
}

// deadPositions returns the positions in snap of the identities live does not hold. Positions
// rather than ids or entries: the set can be as large as the whole index, and four bytes each keeps
// the prune's own working set negligible next to what it reclaims.
func deadPositions(snap []series.Entry, live map[signal.SeriesID]struct{}) []int32 {
	var dead []int32

	for i := range snap {
		if _, ok := live[snap[i].ID]; !ok {
			dead = append(dead, int32(i))
		}
	}

	return dead
}

// liveIdentities collects the identities still backed by data: every series id in a live part, plus
// the in-memory tiers. Parts are read off the engine lock — the sidecar walk is I/O — over the
// caller's immutable part snapshot.
func (e *Engine) liveIdentities(ctx context.Context, parts []*part) (map[signal.SeriesID]struct{}, error) {
	live := make(map[signal.SeriesID]struct{})

	for _, p := range parts {
		if err := p.index.forEachID(ctx, func(id signal.SeriesID) { live[id] = struct{}{} }); err != nil {
			return nil, errors.Wrapf(err, "enumerate series of part %q", p.prefix)
		}
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	// A buffered sample is live data whether or not any part holds the series: the head, the
	// buffers a flush has detached but not yet published, and the recent tier.
	for id := range e.head.samples {
		live[id] = struct{}{}
	}

	for id := range e.flushing {
		live[id] = struct{}{}
	}

	for id := range e.recent {
		live[id] = struct{}{}
	}

	return live, nil
}

// clearIdentityDirty records that the current part set has been checked and holds too little dead
// identity to rebuild for.
func (e *Engine) clearIdentityDirty() {
	e.mu.Lock()
	e.identityDirty = false
	e.mu.Unlock()
}
