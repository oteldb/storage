package chunk

import (
	"encoding/binary"
	"math/bits"

	"github.com/oteldb/storage/encoding/bitstream"
)

// t64BlockSize is the transpose block width: 64 values → 64×64-bit matrix.
const t64BlockSize = 64

// EncodeIntsT64 appends a T64-encoded int64 column to dst (DESIGN.md §6, §14 M0;
// ClickHouse T64: bit-transpose + crop of unused high bits).
//
// Layout: [uvarint rows] [8 bytes min] [8 bytes max] [transposed valuable bits]
//
// The valuable-bit count is numBits = 64 - clz(min ^ max). If min == max (numBits==0),
// only the header is stored (constant column). Otherwise the values are processed in
// blocks of 64: each block is transposed so that bit j of all 64 values sits in a
// single uint64, and only the lowest numBits transposed rows are emitted (the high
// zero bits are cropped). On decode the cropped high bits are restored by OR-ing the
// common high prefix (min & ~((1<<numBits)-1)).
//
// This codec is lossless. It is a preprocessor: the output is designed to be fed to a
// general compressor (ZSTD/LZ4) which benefits from the transposed, repetitive layout.
func EncodeIntsT64(dst []byte, vals []int64) []byte {
	w := bitstream.NewWriter(dst)
	w.WriteUvarint(uint64(len(vals)))

	if len(vals) == 0 {
		w.PadToByte()

		return w.Bytes()
	}

	// Find min/max.
	minVal, maxVal := vals[0], vals[0]
	for _, v := range vals[1:] {
		if v < minVal {
			minVal = v
		}

		if v > maxVal {
			maxVal = v
		}
	}

	// Write the 16-byte header (raw little-endian, byte-aligned).
	header := make([]byte, 16)
	binary.LittleEndian.PutUint64(header[0:8], uint64(minVal))
	binary.LittleEndian.PutUint64(header[8:16], uint64(maxVal))

	for _, b := range header {
		_ = w.WriteByte(b)
	}

	umin := uint64(minVal)
	umax := uint64(maxVal)

	numBits := valuableBits(umin, umax, minVal < 0 && maxVal >= 0)
	if numBits == 0 {
		w.PadToByte()

		return w.Bytes() // constant column
	}

	// Process in blocks of 64.
	for i := 0; i < len(vals); i += t64BlockSize {
		end := min(i+t64BlockSize, len(vals))

		t64EncodeBlock(w, vals[i:end], numBits)
	}

	w.PadToByte()

	return w.Bytes()
}

// DecodeIntsT64 decodes a T64-encoded int64 column from src into dst.
func DecodeIntsT64(dst []int64, src []byte) ([]int64, int, error) {
	r, rows, consumed, err := readHeader(src)
	if err != nil {
		return dst, 0, err
	}

	if rows == 0 {
		return dst, consumed, nil
	}

	if cap(dst) < rows {
		dst = resize(dst, rows)
	}

	dst = dst[:rows]

	// Read the 16-byte header.
	minBytes := make([]byte, 8)

	for i := range 8 {
		b, err := r.ReadBits(8)
		if err != nil {
			return dst, 0, err
		}

		minBytes[i] = byte(b)
	}

	maxBytes := make([]byte, 8)

	for i := range 8 {
		b, err := r.ReadBits(8)
		if err != nil {
			return dst, 0, err
		}

		maxBytes[i] = byte(b)
	}

	minVal := int64(binary.LittleEndian.Uint64(minBytes))
	maxVal := int64(binary.LittleEndian.Uint64(maxBytes))

	umin := uint64(minVal)
	umax := uint64(maxVal)
	numBits := valuableBits(umin, umax, minVal < 0 && maxVal >= 0)

	if numBits == 0 {
		// Constant column.
		for i := range rows {
			dst[i] = minVal
		}

		return dst, consumed + r.ConsumedBytes(), nil
	}

	// Read blocks.
	idx := 0
	for idx < rows {
		end := min(idx+t64BlockSize, rows)

		err := t64DecodeBlock(r, dst[idx:end], numBits, minVal, maxVal)
		if err != nil {
			return dst, 0, err
		}

		idx = end
	}

	return dst, consumed + r.ConsumedBytes(), nil
}

// valuableBits returns the number of bits needed to represent the varying portion of
// the values: 64 - clz(min ^ max). For signed straddle (min<0, max>=0), +1 bit for
// the sign.
func valuableBits(umin, umax uint64, signedStraddle bool) uint8 {
	if umin == umax {
		return 0
	}

	xor := umin ^ umax

	nb := uint8(64 - bits.LeadingZeros64(xor))
	if signedStraddle {
		nb++
		if nb > 64 {
			nb = 64
		}
	}

	return nb
}

// t64EncodeBlock transposes a block of ≤64 int64 values and writes the lowest numBits
// transposed rows. The transpose: bit j of value i → bit i of row j. Only rows 0..numBits-1
// are written; the high zero bits are cropped.
func t64EncodeBlock(w *bitstream.Writer, vals []int64, numBits uint8) {
	// Transpose: for each bit position p (0..numBits-1), collect bit p of all vals
	// into a uint64 and write it.
	for p := range numBits {
		var row uint64

		for i, v := range vals {
			if uint64(v)&(1<<p) != 0 {
				row |= 1 << i
			}
		}

		_ = w.WriteByte(byte(row))
		for b := 1; b < 8; b++ {
			_ = w.WriteByte(byte(row >> (8 * b)))
		}
	}
}

// t64DecodeBlock reads numBits transposed rows and reverses the transpose.
func t64DecodeBlock(r *bitstream.Reader, dst []int64, numBits uint8, minVal, maxVal int64) error {
	// Read the transposed rows.
	rows := make([]uint64, numBits)
	for p := range numBits {
		var v uint64

		for b := range 8 {
			by, err := r.ReadBits(8)
			if err != nil {
				return err
			}

			v |= by << (8 * b)
		}

		rows[p] = v
	}

	// Reverse transpose: bit p of dst[i] = bit i of rows[p].
	// Restore the cropped high bits.
	upperMin := uint64(minVal) & ^((uint64(1) << numBits) - 1)
	signedStraddle := minVal < 0 && maxVal >= 0
	upperMax := uint64(maxVal) & ^((uint64(1) << numBits) - 1)
	signBit := uint64(1) << (numBits - 1)

	for i := range dst {
		var v uint64

		for p := range numBits {
			if rows[p]&(1<<i) != 0 {
				v |= 1 << p
			}
		}
		// Restore cropped high bits.
		if signedStraddle {
			if v&signBit != 0 {
				v |= upperMin // negative
			} else {
				v |= upperMax // positive
			}
		} else {
			v |= upperMin
		}

		dst[i] = int64(v)
	}

	return nil
}
