package chunk

import "github.com/oteldb/storage/encoding/bitstream"

// EncodeTimestamps appends a delta-of-delta encoded timestamp column to dst and
// returns the extended slice (DESIGN.md §6, §14 M0; Prometheus-style).
//
// Layout: [uvarint rows] [bitstream payload]:
//
//	row 0:  varint(t0)                 // signed zigzag varint, absolute timestamp
//	row 1:  uvarint(t1 - t0)           // unsigned varint, first delta
//	row 2+: dod(t_n)                   // delta-of-delta, 5-case prefix (see below)
//
// The DoD bit layout (dod = tDelta_n - tDelta_{n-1}):
//
//	0b0              + 0  bits  → dod == 0            (1 bit total)
//	0b10             + 14 bits  → |dod| ≤ 2^13        (16 bits)
//	0b110            + 17 bits  → |dod| ≤ 2^16        (20 bits)
//	0b1110           + 20 bits  → |dod| ≤ 2^19        (24 bits)
//	0b1111           + 64 bits  → escape, full int64  (68 bits)
//
// The 14/17/20-bit values are stored as unsigned two's-complement and sign-extended
// on decode (values ≥ 1<<(n-1) are negative). The 64-bit escape is a raw int64 cast.
// Timestamps must be non-decreasing for optimal compression; decreasing timestamps
// still round-trip but produce 68-bit escapes.
func EncodeTimestamps(dst []byte, ts []int64) []byte {
	w, out := writeHeader(dst, len(ts))
	if len(ts) == 0 {
		return out
	}

	// Row 0: absolute timestamp as a signed varint.
	w.WriteVarint(ts[0])

	if len(ts) == 1 {
		w.PadToByte()

		return w.Bytes()
	}

	// Row 1: first delta as an unsigned varint.
	tDelta := ts[1] - ts[0]
	w.WriteUvarint(uint64(tDelta))

	// Row 2+: delta-of-delta.
	prevDelta := tDelta
	for i := 2; i < len(ts); i++ {
		tDelta = ts[i] - ts[i-1]
		dod := tDelta - prevDelta
		prevDelta = tDelta

		writeDoD(w, dod)
	}

	w.PadToByte()

	return w.Bytes()
}

// DecodeTimestamps decodes a DoD-encoded timestamp column from src into dst (growing
// it as needed) and returns the result with the number of source bytes consumed.
func DecodeTimestamps(dst []int64, src []byte) ([]int64, int, error) {
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

	// Row 0.
	t0, err := r.ReadVarint()
	if err != nil {
		return dst, 0, err
	}

	dst[0] = t0
	if rows == 1 {
		return dst, consumed + r.ConsumedBytes(), nil
	}

	// Row 1.
	td64, err := r.ReadUvarint()
	if err != nil {
		return dst, 0, err
	}

	tDelta := int64(td64)
	dst[1] = dst[0] + tDelta

	// Row 2+.
	for i := 2; i < rows; i++ {
		dod, err := readDoD(r)
		if err != nil {
			return dst, 0, err
		}

		tDelta += dod
		dst[i] = dst[i-1] + tDelta
	}

	return dst, consumed + r.ConsumedBytes(), nil
}

// writeDoD writes a delta-of-delta value using the 5-case prefix.
func writeDoD(w *bitstream.Writer, dod int64) {
	switch {
	case dod == 0:
		w.WriteBit(false)
	case bitRange(dod, 14):
		w.WriteBits(0b10, 2)
		w.WriteBits(uint64(dod)&0x3fff, 14)
	case bitRange(dod, 17):
		w.WriteBits(0b110, 3)
		w.WriteBits(uint64(dod)&0x1ffff, 17)
	case bitRange(dod, 20):
		w.WriteBits(0b1110, 4)
		w.WriteBits(uint64(dod)&0xfffff, 20)
	default:
		w.WriteBits(0b1111, 4)
		w.WriteBits(uint64(dod), 64)
	}
}

// readDoD reads a delta-of-delta value.
func readDoD(r *bitstream.Reader) (int64, error) {
	// Read the unary-ish prefix: count the leading 1 bits up to 4.
	var d uint8

	for range 4 {
		bit, err := r.ReadBit()
		if err != nil {
			return 0, err
		}

		d <<= 1

		if !bit {
			break
		}

		d |= 1
	}

	switch d {
	case 0b0:
		return 0, nil
	case 0b10:
		bits, err := r.ReadBits(14)
		if err != nil {
			return 0, err
		}

		return signExtend(bits, 14), nil
	case 0b110:
		bits, err := r.ReadBits(17)
		if err != nil {
			return 0, err
		}

		return signExtend(bits, 17), nil
	case 0b1110:
		bits, err := r.ReadBits(20)
		if err != nil {
			return 0, err
		}

		return signExtend(bits, 20), nil
	case 0b1111:
		bits, err := r.ReadBits(64)
		if err != nil {
			return 0, err
		}

		return int64(bits), nil
	default:
		// 0b110 or 0b111 would not reach here due to the break-on-zero loop,
		// but guard against malformed input.
		return 0, errUnexpectedEOF
	}
}

// bitRange returns whether x fits in the signed n-bit range used by DoD:
// -(2^(n-1)-1) ≤ x ≤ 2^(n-1). The asymmetric range matches Prometheus (the positive
// side gets one extra value).
func bitRange(x int64, nbits uint8) bool {
	hi := int64(1) << (nbits - 1)

	return -(hi-1) <= x && x <= hi
}

// signExtend interprets the low nbits of u as a two's-complement signed value.
// Note: uses strict `>` (not `>=`) so that the max positive value 1<<(nbits-1) is
// preserved as positive — matching the asymmetric DoD range (positive side gets one
// extra value). See Prometheus chunkenc/xor.go:388-393.
func signExtend(u uint64, nbits uint8) int64 {
	// u holds nbits significant bits (nbits ≤ 20 here, never the 64-bit DoD case),
	// so int64(u) is non-negative and the signed subtraction below stays well within
	// int64 range. Doing the negative branch in signed arithmetic avoids the unsigned
	// wraparound that uint64 subtraction would rely on, while giving identical results.
	if u > uint64(1)<<(nbits-1) {
		return int64(u) - int64(1)<<nbits
	}

	return int64(u)
}

// resize grows dst to at least n capacity. It reuses the backing array when possible.
func resize[T any](dst []T, n int) []T {
	if cap(dst) >= n {
		return dst[:n]
	}

	newCap := max(max(n, cap(dst)*2), n)

	out := make([]T, n, newCap)
	copy(out, dst)

	return out
}
