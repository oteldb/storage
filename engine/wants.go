package engine

import "github.com/oteldb/storage/backend/bucketindex"

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
		bucketindex.WantsOverlap(e.pendingWants, start, end)
}

// HasWants reports whether any repair obligation is outstanding, without a window — the read seam's
// build-time check, so a fully repaired shard keeps the bare engine as its fetcher.
func (e *Engine) HasWants() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return len(e.wants)+len(e.pendingWants) > 0
}
