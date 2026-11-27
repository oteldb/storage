package series

import (
	"math"

	"github.com/oteldb/storage/signal"
)

const (
	encodedContext   = math.MaxUint32
	encodedSlabBytes = 16 << 10
)

// AddEncoded owns an already encoded canonical descriptor and returns its ordinal.
// The caller supplies the ID and descriptor produced together by the WAL encoder.
func (ix *Index) AddEncoded(id signal.SeriesID, raw []byte) uint32 {
	ref, _ := ix.InternEncoded(id, raw)
	return ref
}

// InternEncoded returns the ordinal and whether it appended a new descriptor.
func (ix *Index) InternEncoded(id signal.SeriesID, raw []byte) (uint32, bool) {
	if ref, ok := ix.byID.get(id, ix.rows); ok {
		return ref, false
	}
	if uint64(len(ix.rows)) > math.MaxUint32 || uint64(len(raw)) > math.MaxUint32 {
		panic("series: encoded reference exhausted")
	}
	ref := uint32(len(ix.rows))
	page := len(ix.encoded) - 1
	if page < 0 || len(raw) > len(ix.encoded[page])-ix.encodedUsed {
		slab := make([]byte, max(encodedSlabBytes, len(raw)))
		ix.encoded = append(ix.encoded, slab)
		ix.encodedBytes += int64(len(slab))
		ix.encodedUsed = 0
		page++
	}
	offset := uint64(page)<<32 | uint64(ix.encodedUsed)
	// Slab headers are immutable once published in a View. Each append writes only its own range.
	copy(ix.encoded[page][ix.encodedUsed:], raw)
	ix.encodedUsed += len(raw)
	ix.byID.put(id, ref, ix.rows)
	ix.rows = append(ix.rows, row{id: id, context: encodedContext, offset: offset, keys: uint32(len(raw))})
	return ref, true
}

// EncodedDescriptor borrows a WAL-encoded descriptor, if present.
func (v View) EncodedDescriptor(i int) []byte {
	r := v.rows[i]
	if r.context != encodedContext {
		return nil
	}
	slab := v.encoded[r.offset>>32]
	start := uint64(uint32(r.offset))
	end := start + uint64(r.keys)
	return slab[start:end:end]
}

func decodeEncodedDescriptor(dst *signal.Series, raw []byte) {
	if n, err := signal.DecodeSeriesInto(dst, raw); err != nil || n != len(raw) {
		panic("series: invalid owned WAL descriptor")
	}
}
