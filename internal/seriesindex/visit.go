package seriesindex

import (
	"bytes"
	"context"
	"encoding/binary"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/index/postings"
	"github.com/oteldb/storage/signal"
)

// Visit consumes sorted local references, borrowing one page at a time. Payload views expire
// when the callback returns; a caller can reserve and copy only its selected identities.
func (r *Index) Visit(ctx context.Context, refs postings.Postings,
	visit func(signal.SeriesID, []byte, Metadata) error,
) error {
	c := r.table.Cursor(ctx)
	var key [5]byte
	key[0] = 's'
	for refs.Next() {
		ref := refs.At()
		if ref.Hi != 0 || ref.Lo >= uint64(r.count) {
			return errors.Wrap(ErrCorrupt, "selected reference")
		}
		binary.BigEndian.PutUint32(key[1:], uint32(ref.Lo))
		if !c.Seek(key[:]) {
			if c.Err() != nil {
				return c.Err()
			}
			return errors.Wrap(ErrCorrupt, "selected identity missing")
		}
		if !bytes.Equal(c.Key(), key[:]) {
			return errors.Wrap(ErrCorrupt, "selected identity missing")
		}
		id, data, meta, err := decodeRecord(c.Value())
		if err != nil {
			return err
		}
		if signal.HashBytes(data) != id {
			return errors.Wrap(ErrCorrupt, "selected identity hash")
		}
		if err := visit(id, data, meta); err != nil {
			return err
		}
	}
	return refs.Err()
}

// Metadata finds a series' covered timestamp without materializing attributes.
func (r *Index) Metadata(ctx context.Context, id signal.SeriesID) (Metadata, bool, error) {
	if err := r.validateReferences(ctx); err != nil {
		return Metadata{}, false, err
	}
	c := r.table.Cursor(ctx)
	key := idKey(id)
	if !c.Seek(key) {
		return Metadata{}, false, c.Err()
	}
	if !bytes.Equal(c.Key(), key) {
		return Metadata{}, false, nil
	}
	ref, inlineMeta, inline, err := decodeIDValue(c.Value())
	if err != nil {
		return Metadata{}, false, err
	}
	if ref >= r.count {
		return Metadata{}, false, errors.Wrap(ErrCorrupt, "metadata reference bounds")
	}
	if inline {
		return inlineMeta, true, nil
	}
	key = recordKey(ref)
	if !c.Seek(key) {
		if c.Err() != nil {
			return Metadata{}, false, c.Err()
		}
		return Metadata{}, false, errors.Wrap(ErrCorrupt, "metadata identity missing")
	}
	if !bytes.Equal(c.Key(), key) {
		return Metadata{}, false, errors.Wrap(ErrCorrupt, "metadata identity missing")
	}
	got, data, meta, err := decodeRecord(c.Value())
	if err != nil {
		return Metadata{}, false, err
	}
	if got != id || signal.HashBytes(data) != id {
		return Metadata{}, false, errors.Wrap(ErrCorrupt, "metadata identity hash")
	}
	return meta, true, nil
}
