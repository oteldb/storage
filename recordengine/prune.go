package recordengine

import (
	"context"

	"github.com/go-faster/sdk/zctx"
	"go.uber.org/zap"

	"github.com/oteldb/storage/index/series"
	"github.com/oteldb/storage/signal"
)

// Identity pruning bounds the resident index, which otherwise spans every stream the process has
// ever seen: retention drops records and whole parts, but nothing removed the identities that named
// them, so a tenant churning streams (the `instance.id`-shaped resource attributes that turned ~24
// stable streams into ~15k) accumulated symbols, index entries, postings and OOO watermarks for the
// process' lifetime — memory `HeadBytes` does not report and no flush returns (`Stats.IdentityBytes`).
//
// It mirrors the metrics engine's prune, with one simplification: a record part keeps its stream →
// row-range map resident (`part.ranges`), so the live set costs no I/O at all.
//
// Symbols are dense ids referenced by the postings lists, so nothing can be deleted in place —
// dropping one symbol would renumber the rest. The prune therefore **rebuilds**, off the engine
// lock, against the series index's append-only entry log, and swaps the result in under the lock.

const (
	// pruneMinIdentities is the identity count below which a prune is never worth its rebuild.
	pruneMinIdentities = 1 << 12
	// pruneDeadNum / pruneDeadDen is the share of the identity set that must be dead before a
	// rebuild runs (a quarter).
	pruneDeadNum = 1
	pruneDeadDen = 4
)

// PruneOptions tunes an identity prune.
type PruneOptions struct {
	// Force runs the prune even when the background thresholds would skip it — an engine that has
	// merged nothing away since the last pass, or one whose dead set is too small to pay for the
	// rebuild. It is what an operator-triggered sweep wants.
	Force bool
}

// PruneIdentities drops the stream identities no live data names any more — those the retention side
// of a merge left behind — rebuilding the resident index around the survivors. It returns the number
// removed (0 when nothing could have died, or when too little has).
//
// Every node may call it, owner or replica: identity is scoped to the part that holds it, so the
// live set is derived from *this node's* parts and means exactly "what this node can still serve".
// A part a replica has not yet synced brings its identities with it when it arrives. It takes the
// same flush/merge exclusion as those paths, so no publish can add an identity underneath it.
func (e *Engine) PruneIdentities(ctx context.Context) (int, error) {
	return e.PruneIdentitiesWith(ctx, PruneOptions{})
}

// PruneIdentitiesWith is [Engine.PruneIdentities] with explicit options.
func (e *Engine) PruneIdentitiesWith(ctx context.Context, opts PruneOptions) (int, error) {
	if e.cfg.Backend == nil {
		return 0, nil // head-only: nothing is flushed, so no identity can have outlived its data
	}

	e.flushMu.Lock()
	defer e.flushMu.Unlock()

	e.mu.RLock()
	dirty, total := e.identityDirty, e.head.series.Len()
	live := e.liveIdentitiesLocked()
	snap := e.head.series.Snapshot()
	e.mu.RUnlock()

	// Identities die only when retention drops rows or parts, so an engine that has merged nothing
	// away since the last prune skips the work entirely. An idle tenant pays nothing.
	if !opts.Force && (!dirty || total < pruneMinIdentities) {
		return 0, nil
	}

	dead := deadPositions(snap, live)
	if len(dead) == 0 {
		e.clearIdentityDirty()

		return 0, nil
	}

	if !opts.Force && len(dead)*pruneDeadDen < total*pruneDeadNum {
		// Too little to reclaim for the rebuild's cost. The flag stays clear: the next merge that
		// drops data sets it again, and until then the answer cannot change.
		e.clearIdentityDirty()

		return 0, nil
	}

	// The expensive part — re-interning every survivor into fresh structures — runs off the lock,
	// over the immutable snapshot. Ingest and queries proceed against the current index throughout.
	rebuilt := rebuildIdentity(e.cfg.Schema, snap, live)

	e.mu.Lock()
	// Identities registered while the rebuild ran sit past the snapshot's end in the same
	// append-only log; a stream that merely regained records leaves no such entry, so the swap
	// re-checks the dead set against the live buffers too.
	e.head.swapIdentity(rebuilt, snap, e.head.series.Snapshot()[len(snap):], dead)
	e.identityDirty = false
	remaining := e.head.series.Len()
	e.mu.Unlock()

	zctx.From(ctx).Debug("pruned dead stream identities",
		zap.String("signal", e.cfg.Signal), zap.String("prefix", e.cfg.Prefix),
		zap.Int("removed", len(dead)), zap.Int("live", remaining))

	return len(dead), nil
}

// liveIdentitiesLocked collects the identities still backed by data: every stream a live part holds
// (from its resident row-range map — no I/O), plus the in-memory tiers. Caller holds e.mu.
func (e *Engine) liveIdentitiesLocked() map[signal.SeriesID]struct{} {
	live := make(map[signal.SeriesID]struct{})

	for _, p := range e.parts {
		for id := range p.ranges {
			live[id] = struct{}{}
		}
	}

	// A buffered record is live data whether or not any part holds the stream: the head, and the
	// buffers a flush has detached but not yet published.
	for id := range e.head.records {
		live[id] = struct{}{}
	}

	for id := range e.flushing {
		live[id] = struct{}{}
	}

	return live
}

// deadPositions returns the positions in snap of the identities live does not hold. Positions rather
// than ids or entries: the set can be as large as the whole index, and four bytes each keeps the
// prune's own working set negligible next to what it reclaims.
func deadPositions(snap []series.Entry, live map[signal.SeriesID]struct{}) []int32 {
	var dead []int32

	for i := range snap {
		if _, ok := live[snap[i].ID]; !ok {
			dead = append(dead, int32(i))
		}
	}

	return dead
}

// clearIdentityDirty records that the current part set has been checked and holds too little dead
// identity to rebuild for.
func (e *Engine) clearIdentityDirty() {
	e.mu.Lock()
	e.identityDirty = false
	e.mu.Unlock()
}
