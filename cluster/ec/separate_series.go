package ec

import (
	"context"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/block"
)

// ErrSeparateSeries rejects conversion until EC supports shared descriptor dependencies.
var ErrSeparateSeries = errors.New("ec: separate series parts are unsupported")

func rejectSeparateSeries(ctx context.Context, be backend.Backend, prefix string) error {
	if _, err := backend.ReadView(ctx, be, prefix+"/series-ref"); err == nil {
		return ErrSeparateSeries
	} else if !errors.Is(err, backend.ErrNotExist) {
		return errors.Wrap(err, "check numeric series reference")
	}
	raw, err := backend.ReadView(ctx, be, prefix+"/manifest")
	if errors.Is(err, backend.ErrNotExist) {
		return nil
	}
	if err != nil {
		return errors.Wrap(err, "check metadata run manifest")
	}
	m, err := block.DecodeManifest(raw)
	if err != nil {
		// The generic converter also accepts opaque objects written outside block.
		return nil //nolint:nilerr // Opaque manifests do not identify a shared metadata run.
	}
	if len(m.Columns) == 1 && m.Columns[0].Name == "series" && m.Columns[0].Kind == block.KindInt128 {
		return ErrSeparateSeries
	}
	return nil
}
