package engine

import (
	"context"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"
)

func (e *Engine) replayDescriptorReference(ctx context.Context, id signal.SeriesID) error {
	if !e.compactMetricMetadata || !e.cfg.SeparateSeries {
		return errors.Wrap(wal.ErrCorrupt, "descriptor reference requires a confirmed series catalog")
	}
	if e.head.series.Has(id) {
		return nil
	}
	if err := e.replayResidentGuard(0); err != nil {
		return err
	}
	found, err := e.seriesTable.lookup(ctx, id, func(raw []byte) error {
		if signal.HashBytes(raw) != id {
			return errors.Wrap(wal.ErrCorrupt, "referenced descriptor ID mismatch")
		}
		e.head.rememberHistorical(id, minInt64)
		e.head.registerEncoded(id, raw)
		return nil
	})
	if err != nil {
		return err
	}
	if !found {
		return errors.Wrapf(wal.ErrCorrupt, "missing descriptor for series %v", id)
	}
	return nil
}
