package seriesindex

import (
	"bytes"
	"context"
	"encoding/binary"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/signal"
)

// MetadataReader reuses the compact ID page for operation-local watermark probes.
// Legacy four-byte references allocate a separate descriptor cursor on first use.
type MetadataReader struct {
	index        *Index
	ids, records *Cursor
	idKey        [17]byte
	recordKey    [5]byte
}

// MetadataReader returns a scoped reader without caching historical identity payloads globally.
func (r *Index) MetadataReader(ctx context.Context) *MetadataReader {
	reader := &MetadataReader{}
	reader.Reset(ctx, r)
	return reader
}

// Reset rebinds the reader while retaining page scratch for this operation only.
// A nil index clears the old owner; Lookup requires a subsequent non-nil Reset.
// Retained scratch follows the operation's largest page, not the new index's MaxPageBytes.
func (r *MetadataReader) Reset(ctx context.Context, index *Index) {
	r.index = index
	var table *Table
	if index != nil {
		table = index.table
	}
	if r.ids == nil && index != nil {
		r.ids = &Cursor{}
	}
	if r.ids != nil {
		r.ids.reset(ctx, table)
	}
	if r.records != nil {
		r.records.reset(ctx, table)
	}
	r.idKey = [17]byte{}
	r.recordKey = [5]byte{}
}

// Lookup returns one identity's covered timestamp and optional aggregate without decoding attributes.
func (r *MetadataReader) Lookup(id signal.SeriesID) (Metadata, bool, error) {
	if r.index == nil {
		return Metadata{}, false, errors.New("metadata reader is unbound")
	}
	if err := r.index.validateReferences(r.ids.ctx); err != nil {
		return Metadata{}, false, err
	}
	r.idKey[0] = 'i'
	binary.BigEndian.PutUint64(r.idKey[1:9], id.Hi)
	binary.BigEndian.PutUint64(r.idKey[9:], id.Lo)
	if !r.ids.Seek(r.idKey[:]) {
		return Metadata{}, false, r.ids.Err()
	}
	if !bytes.Equal(r.ids.Key(), r.idKey[:]) {
		return Metadata{}, false, nil
	}
	ref, inlineMeta, inline, err := decodeIDValue(r.ids.Value())
	if err != nil {
		return Metadata{}, false, err
	}
	if ref >= r.index.count {
		return Metadata{}, false, errors.Wrap(ErrCorrupt, "metadata reference bounds")
	}
	if inline {
		return inlineMeta, true, nil
	}
	if r.records == nil {
		r.records = r.index.table.Cursor(r.ids.ctx)
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
