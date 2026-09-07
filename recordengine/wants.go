package recordengine

import (
	"slices"

	"github.com/oteldb/storage/backend/bucketindex"
)

// WantOverlaps reports whether an outstanding repair obligation covers any of [start, end]: the
// engine holds an index entry for a part in that range that it cannot read and has not got back, so
// a read of that window here is short by whatever the part held.
//
// Pending wants count: a want a load discovered but no commit has published yet names data that is
// already unreadable. A hole does not — acknowledging a loss discharges its want, which is what lets
// reads resume once repair or an operator accepts the loss.
func (e *Engine) WantOverlaps(start, end int64) bool {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return bucketindex.WantsOverlap(e.wants, start, end) ||
		bucketindex.WantsOverlap(e.pendingWants, start, end) ||
		bucketindex.WantsOverlap(e.adoptedWants, start, end)
}

// HasWants reports whether any repair obligation is outstanding, without a window — the read seam's
// build-time check, so a fully repaired shard keeps the bare engine as its fetcher.
func (e *Engine) HasWants() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return len(e.wants)+len(e.pendingWants)+len(e.adoptedWants) > 0
}

// AdoptWants records repair obligations discovered outside the engine: parts a peer holds that this
// engine's index does not account for and cannot learn of on its own, since a part it never named
// is one it can never report losing. The next commit publishes them and the repair pass fetches the
// objects; one already outstanding or already satisfied is dropped.
func (e *Engine) AdoptWants(ws []bucketindex.Want) {
	if len(ws) == 0 {
		return
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	for i := range ws {
		p := ws[i].Prefix
		if _, ok := e.indexed[p]; ok {
			continue
		}

		if slices.ContainsFunc(e.adoptedWants, func(h bucketindex.Want) bool { return h.Prefix == p }) {
			continue
		}

		e.adoptedWants = append(e.adoptedWants, ws[i])
	}
}
