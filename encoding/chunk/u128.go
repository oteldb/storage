package chunk

import (
	"encoding/binary"
)

// U128 is an unsigned 128-bit value (a SeriesID at the storage boundary). Comparable, so
// runs of equal ids are detected directly.
type U128 struct {
	Hi, Lo uint64
}

// EncodeU128 appends a run-length-encoded [U128] column to dst (CodecID128). The id sort
// key of a metric part is long runs of one id (all of a series' samples are contiguous),
// so RLE stores a distinct id + run length per run — far smaller than storing every row.
//
// Layout: [uvarint rows] then per run [u128 id big-endian][uvarint runLength].
func EncodeU128(dst []byte, vals []U128) []byte {
	w, _ := writeHeader(dst, len(vals))

	for i := 0; i < len(vals); {
		j := i + 1
		for j < len(vals) && vals[j] == vals[i] {
			j++
		}

		b := w.AppendBytes(16)
		binary.BigEndian.PutUint64(b[:8], vals[i].Hi)
		binary.BigEndian.PutUint64(b[8:], vals[i].Lo)
		w.WriteUvarint(uint64(j - i))

		i = j
	}

	w.PadToByte()

	return w.Bytes()
}

// DecodeU128 decodes a [U128] column into dst (reusing its capacity), returning the
// result and bytes consumed.
func DecodeU128(dst []U128, src []byte) ([]U128, int, error) {
	r, rows, consumed, err := readHeader(src)
	if err != nil {
		return dst, 0, err
	}

	// RLE packs many rows into few bytes, so the stream length gives no bound on rows; cap defensively
	// so a corrupt header can't drive a giant make. Each run is also bounded against rows below.
	if err := boundRows(rows, maxColumnRows); err != nil {
		return dst, 0, err
	}

	dst = dst[:0]
	if cap(dst) < rows {
		dst = make([]U128, 0, rows)
	}

	for len(dst) < rows {
		raw, err := r.ReadBytesView(16)
		if err != nil {
			return dst, 0, err
		}

		id := U128{Hi: binary.BigEndian.Uint64(raw[:8]), Lo: binary.BigEndian.Uint64(raw[8:])}

		runLen, err := r.ReadUvarint()
		if err != nil {
			return dst, 0, err
		}

		if runLen == 0 || runLen > uint64(rows-len(dst)) {
			return dst, 0, errUnexpectedEOF
		}

		for range runLen {
			dst = append(dst, id)
		}
	}

	return dst, consumed + r.ConsumedBytes(), nil
}
