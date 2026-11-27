package seriesindex

import (
	"bytes"
	"context"
	"encoding/binary"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/signal"
)

// RawReader owns operation-local ID and descriptor page scratch.
type RawReader struct {
	index   *Index
	ids     *Cursor
	records *SeriesCursor
	idKey   [17]byte
}

// RawReader returns a reader whose payload views expire at its next Lookup.
func (r *Index) RawReader(ctx context.Context) *RawReader {
	return &RawReader{index: r, ids: r.table.Cursor(ctx), records: r.SeriesCursor(ctx)}
}

// Lookup returns a borrowed canonical descriptor after checking its ID and inline metadata.
func (r *RawReader) Lookup(id signal.SeriesID) ([]byte, Metadata, bool, error) {
	_, raw, meta, found, err := r.LookupRef(id)
	return raw, meta, found, err
}

// LookupRef also returns the checked descriptor ordinal.
func (r *RawReader) LookupRef(id signal.SeriesID) (uint32, []byte, Metadata, bool, error) {
	entry, found, err := r.lookupID(id)
	if err != nil || !found {
		return 0, nil, Metadata{}, false, err
	}
	got, raw, meta, err := r.records.At(entry.ref)
	if err != nil {
		return 0, nil, Metadata{}, false, err
	}
	if got != id || entry.inline && !equalMetadata(entry.meta, meta) {
		return 0, nil, Metadata{}, false, errors.Wrap(ErrCorrupt, "raw descriptor reference mismatch")
	}
	return entry.ref, raw, meta, true, nil
}

// HasID checks the exact canonical ID using only the index key section.
func (r *RawReader) HasID(id signal.SeriesID) (bool, error) {
	_, found, err := r.lookupID(id)
	return found, err
}

// LookupOrdinal reads a descriptor after a validated ID array has resolved its ordinal.
func (r *RawReader) LookupOrdinal(id signal.SeriesID, ref uint32) ([]byte, Metadata, error) {
	got, raw, meta, err := r.records.At(ref)
	if err != nil {
		return nil, Metadata{}, err
	}
	if got != id {
		return nil, Metadata{}, errors.Wrap(ErrCorrupt, "cached descriptor ordinal mismatch")
	}
	return raw, meta, nil
}

// StreamPages retains writable page scratch instead of expanding pages for random seeks.
func (r *RawReader) StreamPages() {
	r.ids.streaming = true
	r.records.cursor.streaming = true
}

type idReference struct {
	ref    uint32
	meta   Metadata
	inline bool
}

func (r *RawReader) lookupID(id signal.SeriesID) (idReference, bool, error) {
	r.idKey[0] = 'i'
	binary.BigEndian.PutUint64(r.idKey[1:9], id.Hi)
	binary.BigEndian.PutUint64(r.idKey[9:], id.Lo)
	key := r.idKey[:]
	if !r.ids.Seek(key) {
		return idReference{}, false, r.ids.Err()
	}
	if !bytes.Equal(r.ids.Key(), key) {
		return idReference{}, false, nil
	}
	ref, meta, inline, err := decodeIDValue(r.ids.Value())
	if err != nil {
		return idReference{}, false, err
	}
	if ref >= r.index.count {
		return idReference{}, false, errors.Wrap(ErrCorrupt, "ID reference out of range")
	}
	return idReference{ref: ref, meta: meta, inline: inline}, true, nil
}
