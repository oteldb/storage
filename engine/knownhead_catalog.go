package engine

import (
	"bytes"
	"context"
	"slices"

	"github.com/go-faster/errors"

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
	for _, id := range ids {
		found, err := source.lookup(ctx, id, func(raw []byte) error {
			ok, err := current.lookup(ctx, id, func(next []byte) error {
				if !bytes.Equal(raw, next) {
					return errors.New("engine: known head canonical identity changed")
				}
				return nil
			})
			if err != nil {
				return err
			}
			if !ok {
				return errors.New("engine: current catalog lacks known head identity")
			}
			return nil
		})
		if err != nil {
			return err
		}
		if !found {
			return errors.New("engine: pinned known head identity missing")
		}
	}
	return nil
}
