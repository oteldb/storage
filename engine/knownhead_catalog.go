package engine

import (
	"context"
	"slices"

	"github.com/oteldb/storage/signal"
)

// verifyKnownCatalog prevents publication under roots that no longer prove the buffered identity.
func (e *Engine) verifyKnownCatalog(ctx context.Context, ids []signal.SeriesID) error {
	e.mu.RLock()
	source := e.head.known.snapshot
	current := e.seriesTable
	current.acquire()
	e.mu.RUnlock()
	defer current.release()
	return verifyKnownSnapshots(ctx, source, current, ids)
}

func verifyKnownSnapshots(ctx context.Context, source, current *seriesSnapshot, ids []signal.SeriesID) error {
	same := current != nil
	for _, root := range source.roots {
		if current == nil || !slices.ContainsFunc(current.roots, func(h *seriesRootHandle) bool { return h.binding == root.binding }) {
			same = false
			break
		}
	}
	if same {
		return nil
	}
	return compareKnownCatalog(ctx, source, current, ids)
}
