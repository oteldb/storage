package engine

import (
	"context"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/internal/watermark"
	"github.com/oteldb/storage/signal"
)

func (p *part) legacyDiskWatermark(ctx context.Context, id signal.SeriesID) (int64, bool, error) {
	ctx = p.reader.ReaderContext(ctx)
	p.diskWatermarkMu.Lock()
	reader := p.diskWatermark
	if reader == nil {
		var err error
		reader, err = watermark.OpenReader(ctx, p.be, watermark.Key(p.prefix), p.index.seriesCount())
		if err != nil {
			p.diskWatermarkMu.Unlock()
			if errors.Is(err, backend.ErrNotExist) || errors.Is(err, watermark.ErrCorrupt) || errors.Is(err, watermark.ErrUnbounded) {
				return 0, false, nil
			}
			return 0, false, err
		}
		p.diskWatermark = reader
	}
	p.diskWatermarkMu.Unlock()
	value, ok, err := reader.Lookup(ctx, id)
	if err != nil {
		return 0, false, err
	}
	return value, ok, nil
}
