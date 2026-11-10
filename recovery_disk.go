package storage

import (
	"context"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/internal/repair"
	"github.com/oteldb/storage/internal/seriesindex"
)

func (s *Storage) loadRecoveredParts(ctx context.Context, e partLoader) error {
	load := func() error {
		switch {
		case s.opts.ReadOnly:
			return e.LoadPartsReadOnly(ctx)
		case s.opts.Cluster != nil:
			return e.LoadPartsUnclaimed(ctx)
		default:
			return e.LoadParts(ctx)
		}
	}
	_, metric := e.(*engine.Engine)
	// The engine retains the existing repeated-corruption confirmation before recording a want.
	// Startup must reach that outcome before the process can join its peers and request repair.
	for attempt := 0; ; attempt++ {
		err := load()
		if !metric || !s.opts.MetricDiskSeries || !errors.Is(err, seriesindex.ErrCorrupt) ||
			errors.Is(err, seriesindex.ErrUnsupportedVersion) || attempt >= repair.HoleConfirmations {
			return err
		}
	}
}
