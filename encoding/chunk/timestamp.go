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
	return encodeTimestamps(dst, ts, false)
}

// EncodeTimestampsScaled appends a [CodecDoDScaled] timestamp column to dst: [EncodeTimestamps]
// with the column's timestamp scale (see tsScale) factored out of every delta, written after row 0.
//
// Layout: [uvarint rows] [varint t0] [uvarint scale] [uvarint (t1-t0)/scale] [dod…]. A column of
// fewer than two rows has no deltas and no scale, so it is byte-identical to [EncodeTimestamps].
//
// A timestamp stored at ns but produced at ms carries six decimal zeros in every delta, which DoD
// spends ~20 bits a row on; dividing them out is lossless and costs nothing when the source really
// is ns-entropic, since tsScale bails on the first delta that is not a multiple of 1000.
func EncodeTimestampsScaled(dst []byte, ts []int64) []byte {
	return encodeTimestamps(dst, ts, true)
}

func encodeTimestamps(dst []byte, ts []int64, scaled bool) []byte {
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

	scale := int64(1)
	if scaled {
		scale = tsScale(ts)
		w.WriteUvarint(uint64(scale))
	}

	// The unit scale keeps its own loop: a division per row would tax every ns-entropic column, and
	// [CodecDoD] columns, for nothing.
	if scale == 1 {
		writeDeltas(w, ts)
	} else {
		writeScaledDeltas(w, ts, scale)
	}

	w.PadToByte()

	return w.Bytes()
}

// writeDeltas writes row 1 as an unsigned varint delta and rows 2+ as delta-of-delta.
func writeDeltas(w *bitstream.Writer, ts []int64) {
	tDelta := ts[1] - ts[0]
	w.WriteUvarint(uint64(tDelta))

	prevDelta := tDelta
	for i := 2; i < len(ts); i++ {
		tDelta = ts[i] - ts[i-1]
		dod := tDelta - prevDelta
		prevDelta = tDelta

		writeDoD(w, dod)
	}
}

// writeScaledDeltas is [writeDeltas] over deltas divided by scale, which divides every one of them.
func writeScaledDeltas(w *bitstream.Writer, ts []int64, scale int64) {
	tDelta := (ts[1] - ts[0]) / scale
	w.WriteUvarint(uint64(tDelta))

	prevDelta := tDelta
	for i := 2; i < len(ts); i++ {
		tDelta = (ts[i] - ts[i-1]) / scale
		dod := tDelta - prevDelta
		prevDelta = tDelta

		writeDoD(w, dod)
	}
}

// tsScale returns the largest of 10^9, 10^6, 10^3 and 1 that divides every first difference of ts.
//
// It is a fixed ladder rather than a gcd: the only scales worth finding are the units timestamps are
// produced in, and testing three constants lets the compiler turn each modulo into a multiply. The
// scale only ever steps down, so an ns-entropic column is settled by its first delta.
//
// Deltas are computed with int64 wraparound, exactly as the decoder recomputes them, so a delta that
// overflows still round-trips: d%scale == 0 means d == (d/scale)*scale with no overflow.
func tsScale(ts []int64) int64 {
	scale := int64(1e9)
	for i := 1; i < len(ts); i++ {
		d := ts[i] - ts[i-1]
		for !divides(scale, d) {
			scale /= 1000
		}

		if scale == 1 {
			return 1
		}
	}

	return scale
}

func divides(scale, d int64) bool {
	switch scale {
	case 1e9:
		return d%1e9 == 0
	case 1e6:
		return d%1e6 == 0
	case 1e3:
		return d%1e3 == 0
	default:
		return true
	}
}

func validScale(scale uint64) bool {
	return scale == 1 || scale == 1e3 || scale == 1e6 || scale == 1e9
}

// DecodeTimestamps decodes a DoD-encoded timestamp column from src into dst (growing
// it as needed) and returns the result with the number of source bytes consumed.
func DecodeTimestamps(dst []int64, src []byte) ([]int64, int, error) {
	return decodeTimestamps(dst, src, false)
}

// DecodeTimestampsScaled decodes a [CodecDoDScaled] column written by [EncodeTimestampsScaled].
func DecodeTimestampsScaled(dst []int64, src []byte) ([]int64, int, error) {
	return decodeTimestamps(dst, src, true)
}

func decodeTimestamps(dst []int64, src []byte, scaled bool) ([]int64, int, error) {
	r, rows, consumed, err := readHeader(src)
	if err != nil {
		return dst, 0, err
	}

	if rows == 0 {
		return dst, consumed, nil
	}

	// DoD is bit-packed (≥1 bit/row), so a count above 8×remaining bytes is a corrupt header.
	if err := boundRows(rows, 8*(len(src)-consumed)); err != nil {
		return dst, 0, err
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

	scale := int64(1)
	if scaled {
		s, err := r.ReadUvarint()
		if err != nil {
			return dst, 0, err
		}

		if !validScale(s) {
			return dst, 0, errUnexpectedEOF
		}

		scale = int64(s)
	}

	if scale == 1 {
		err = readDeltas(r, dst)
	} else {
		err = readScaledDeltas(r, dst, scale)
	}

	if err != nil {
		return dst, 0, err
	}

	return dst, consumed + r.ConsumedBytes(), nil
}

// readDeltas fills dst[1:] from the row-1 delta and the rows-2+ delta-of-delta stream; dst[0] is set.
func readDeltas(r *bitstream.Reader, dst []int64) error {
	td64, err := r.ReadUvarint()
	if err != nil {
		return err
	}

	tDelta := int64(td64)
	dst[1] = dst[0] + tDelta

	for i := 2; i < len(dst); i++ {
		dod, err := readDoD(r)
		if err != nil {
			return err
		}

		tDelta += dod
		dst[i] = dst[i-1] + tDelta
	}

	return nil
}

// readScaledDeltas is [readDeltas] for deltas stored divided by scale.
func readScaledDeltas(r *bitstream.Reader, dst []int64, scale int64) error {
	td64, err := r.ReadUvarint()
	if err != nil {
		return err
	}

	tDelta := int64(td64)
	dst[1] = dst[0] + tDelta*scale

	for i := 2; i < len(dst); i++ {
		dod, err := readDoD(r)
		if err != nil {
			return err
		}

		tDelta += dod
		dst[i] = dst[i-1] + tDelta*scale
	}

	return nil
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

// readDoDPrefix reads the variable-length DoD case selector ("0", "10", "110", "1110", "1111"),
// returning it in the same low-bit form writeDoD emits. The fast path peeks the ≤4 prefix bits in
// one shot (no per-bit call); the slow path (a buffer boundary mid-prefix) counts bits with ReadBit.
func readDoDPrefix(r *bitstream.Reader) (uint8, error) {
	if r.Buffered() >= 4 {
		p := uint8(r.Peek(4)) // the 4 high bits, right-justified (0..15)

		switch {
		case p < 0b1000: // "0"
			r.Skip(1)

			return 0b0, nil
		case p < 0b1100: // "10"
			r.Skip(2)

			return 0b10, nil
		case p < 0b1110: // "110"
			r.Skip(3)

			return 0b110, nil
		case p == 0b1110: // "1110"
			r.Skip(4)

			return 0b1110, nil
		default: // "1111"
			r.Skip(4)

			return 0b1111, nil
		}
	}

	// Boundary fallback: count the leading 1 bits up to 4, bit at a time.
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

	return d, nil
}

// readDoD reads a delta-of-delta value.
func readDoD(r *bitstream.Reader) (int64, error) {
	d, err := readDoDPrefix(r)
	if err != nil {
		return 0, err
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
