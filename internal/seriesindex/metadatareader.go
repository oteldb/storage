package seriesindex

import (
	"bytes"
	"context"
	"encoding/binary"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/signal"
)

// MetadataReader reuses separate ID and descriptor pages for operation-local watermark probes.
type MetadataReader struct {
	index        *Index
	ids, records *Cursor
	idKey        [17]byte
	recordKey    [5]byte
}

// MetadataReader returns a scoped reader without caching historical identity payloads globally.
func (r *Index) MetadataReader(ctx context.Context) *MetadataReader {
	return &MetadataReader{index: r, ids: r.table.Cursor(ctx), records: r.table.Cursor(ctx)}
}

// Lookup returns one identity's covered timestamp and optional aggregate without decoding attributes.
func (r *MetadataReader) Lookup(id signal.SeriesID) (Metadata, bool, error) {
	r.idKey[0] = 'i'
	binary.BigEndian.PutUint64(r.idKey[1:9], id.Hi)
	binary.BigEndian.PutUint64(r.idKey[9:], id.Lo)
	if !r.ids.Seek(r.idKey[:]) {
		return Metadata{}, false, r.ids.Err()
	}
	if !bytes.Equal(r.ids.Key(), r.idKey[:]) {
		return Metadata{}, false, nil
	}
	if len(r.ids.Value()) != 4 {
		return Metadata{}, false, errors.Wrap(ErrCorrupt, "metadata reference")
	}
	ref := binary.BigEndian.Uint32(r.ids.Value())
	if ref >= r.index.count {
		return Metadata{}, false, errors.Wrap(ErrCorrupt, "metadata reference bounds")
	}
	r.recordKey[0] = 's'
	binary.BigEndian.PutUint32(r.recordKey[1:], ref)
	if !r.records.Seek(r.recordKey[:]) {
		if r.records.Err() != nil {
			return Metadata{}, false, r.records.Err()
		}
		return Metadata{}, false, errors.Wrap(ErrCorrupt, "metadata identity missing")
	}
	if !bytes.Equal(r.records.Key(), r.recordKey[:]) {
		return Metadata{}, false, errors.Wrap(ErrCorrupt, "metadata identity missing")
	}
	got, data, meta, err := decodeRecord(r.records.Value())
	if err != nil {
		return Metadata{}, false, err
	}
	if got != id || signal.HashBytes(data) != id {
		return Metadata{}, false, errors.Wrap(ErrCorrupt, "metadata identity hash")
	}
	return meta, true, nil
}
