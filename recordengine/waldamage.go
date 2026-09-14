package recordengine

import (
	"context"

	"go.uber.org/zap"

	"github.com/oteldb/storage/internal/obs"
	"github.com/oteldb/storage/wal"
)

// salvageHandlers is [Engine.replayHandlers] for a WAL directory: a damaged region is counted, logged
// and skipped, so the engine opens with the records that survive instead of refusing to.
func (e *Engine) salvageHandlers(ctx context.Context, dir string) wal.Handlers {
	h := e.replayHandlers()
	h.OnDamage = func(d wal.Damage) error {
		ctx := e.cfg.Obs.Base(ctx)
		e.cfg.Obs.Corruption.Detected(ctx, "wal", obs.CorruptTolerated)
		e.cfg.Obs.Logger(ctx).Warn("wal damage skipped on replay; the records in it are lost",
			zap.String("dir", dir), zap.String("segment", d.Segment), zap.Int("offset", d.Offset),
			zap.Int("bytes", d.Length), zap.String("kept", d.Kept), zap.Error(d.Err))

		return nil
	}

	return h
}
