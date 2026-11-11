package seriesindex

import (
	"bytes"
	"context"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/signal"
)

// RawReader owns operation-local ID and descriptor page scratch.
type RawReader struct {
	index   *Index
	ids     *Cursor
	records *SeriesCursor
}

// RawReader returns a reader whose payload views expire at its next Lookup.
func (r *Index) RawReader(ctx context.Context) *RawReader {
	return &RawReader{index: r, ids: r.table.Cursor(ctx), records: r.SeriesCursor(ctx)}
}

// Lookup returns a borrowed canonical descriptor after checking its ID and inline metadata.
func (r *RawReader) Lookup(id signal.SeriesID) ([]byte, Metadata, bool, error) {
	key := idKey(id)
	if !r.ids.Seek(key) {
		return nil, Metadata{}, false, r.ids.Err()
	}
	if !bytes.Equal(r.ids.Key(), key) {
		return nil, Metadata{}, false, nil
	}
	ref, inlineMeta, inline, err := decodeIDValue(r.ids.Value())
	if err != nil {
		return nil, Metadata{}, false, err
	}
	got, raw, meta, err := r.records.At(ref)
	if err != nil {
		return nil, Metadata{}, false, err
	}
	if got != id || inline && !equalMetadata(inlineMeta, meta) {
		return nil, Metadata{}, false, errors.Wrap(ErrCorrupt, "raw descriptor reference mismatch")
	}
	return raw, meta, true, nil
}
