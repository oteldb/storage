package seriesindex

import (
	"bytes"
	"context"
	"encoding/binary"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/signal"
)

// SeriesCursor reuses the descriptor page while walking an ordered source during compaction.
type SeriesCursor struct {
	index  *Index
	cursor *Cursor
}

// SeriesCursor returns a scoped descriptor cursor with no permanently cached identity payloads.
func (r *Index) SeriesCursor(ctx context.Context) *SeriesCursor {
	return &SeriesCursor{index: r, cursor: r.table.Cursor(ctx)}
}

// At returns a borrowed canonical identity. Views expire at the next call to At.
func (c *SeriesCursor) At(ref uint32) (signal.SeriesID, []byte, Metadata, error) {
	if ref >= c.index.count {
		return signal.SeriesID{}, nil, Metadata{}, errors.Wrap(ErrCorrupt, "descriptor reference")
	}
	var key [5]byte
	key[0] = 's'
	binary.BigEndian.PutUint32(key[1:], ref)
	if !c.cursor.Seek(key[:]) {
		if c.cursor.Err() != nil {
			return signal.SeriesID{}, nil, Metadata{}, c.cursor.Err()
		}
		return signal.SeriesID{}, nil, Metadata{}, errors.Wrap(ErrCorrupt, "descriptor missing")
	}
	if !bytes.Equal(c.cursor.Key(), key[:]) {
		return signal.SeriesID{}, nil, Metadata{}, errors.Wrap(ErrCorrupt, "descriptor missing")
	}
	id, raw, meta, err := decodeRecord(c.cursor.Value())
	if err != nil {
		return signal.SeriesID{}, nil, Metadata{}, err
	}
	if signal.HashBytes(raw) != id {
		return signal.SeriesID{}, nil, Metadata{}, errors.Wrap(ErrCorrupt, "descriptor hash")
	}
	return id, raw, meta, nil
}
