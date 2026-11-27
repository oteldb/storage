package series

import (
	"math"

	"github.com/oteldb/storage/signal"
)

const encodedContext = math.MaxUint32

// AddEncoded owns an already encoded canonical descriptor and returns its ordinal.
// The caller supplies the ID and descriptor produced together by the WAL encoder.
func (ix *Index) AddEncoded(id signal.SeriesID, raw []byte) uint32 {
	if ref, ok := ix.byID.get(id, ix.rows); ok {
		return ref
	}
	if uint64(len(ix.rows)) > math.MaxUint32 || uint64(len(raw)) > math.MaxUint32 {
		panic("series: encoded reference exhausted")
	}
	ref := uint32(len(ix.rows))
	offset := uint64(len(ix.encoded))
	ix.encoded = append(ix.encoded, raw...)
	ix.byID.put(id, ref, ix.rows)
	ix.rows = append(ix.rows, row{id: id, context: encodedContext, offset: offset, keys: uint32(len(raw))})
	return ref
}

// EncodedDescriptor borrows a WAL-encoded descriptor, if present.
func (v View) EncodedDescriptor(i int) []byte {
	r := v.rows[i]
	if r.context != encodedContext {
		return nil
	}
	return v.encoded[r.offset : r.offset+uint64(r.keys)]
}

func decodeEncodedDescriptor(dst *signal.Series, raw []byte) {
	if n, err := signal.DecodeSeriesInto(dst, raw); err != nil || n != len(raw) {
		panic("series: invalid owned WAL descriptor")
	}
}
