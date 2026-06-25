package chunk

import (
	"math"
	"math/bits"

	"github.com/oteldb/storage/encoding/bitstream"
)

// EncodeFloats appends a Gorilla XOR encoded float64 column to dst and returns the
// extended slice (DESIGN.md §6, §14 M0; Prometheus/Gorilla-style).
//
// Layout: [uvarint rows] [bitstream payload]:
//
//	row 0:  raw64(Float64bits(v0))    // 64 raw bits, MSB-first
//	row 1+: xor-delta(v_n, v_{n-1})  // per the 3-case prefix below
//
// The XOR control-bit cases (delta = Float64bits(new) XOR Float64bits(prev)):
//
//	0b0                    → delta == 0, value unchanged         (1 bit)
//	0b10 + M bits          → reuse prev leading/trailing, M = 64-leading-trailing
//	0b11 + 5b lead + 6b sig + S bits → new leading/trailing, S = sig (0→64 sentinel)
//
// Leading-zero count is clamped to 31 (5-bit field). The significant-bit count is
// stored in 6 bits; 0 is a sentinel meaning 64 (when leading==trailing==0). The
// meaningful XOR bits are stored right-shifted by trailing (the trailing zeros are
// dropped and re-shifted on decode).
func EncodeFloats(dst []byte, vals []float64) []byte {
	w, out := writeHeader(dst, len(vals))
	if len(vals) == 0 {
		return out
	}

	// Row 0: full 64 bits.
	w.WriteBits(math.Float64bits(vals[0]), 64)

	// Row 1+: XOR delta.
	var leading, trailing uint8 = 0xff, 0
	for i := 1; i < len(vals); i++ {
		xorWrite(w, vals[i], vals[i-1], &leading, &trailing)
	}

	w.PadToByte()
	return w.Bytes()
}

// DecodeFloats decodes a Gorilla XOR encoded float64 column from src into dst.
func DecodeFloats(dst []float64, src []byte) ([]float64, int, error) {
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
	v0, err := r.ReadBits(64)
	if err != nil {
		return dst, 0, err
	}
	dst[0] = math.Float64frombits(v0)

	// Row 1+: XOR delta. Carry the previous value forward as the XOR base.
	var leading, trailing uint8 = 0xff, 0
	for i := 1; i < rows; i++ {
		dst[i] = dst[i-1] // XOR is relative to the previous value
		if err := xorRead(r, &dst[i], &leading, &trailing); err != nil {
			return dst, 0, err
		}
	}

	return dst, consumed + r.ConsumedBytes(), nil
}

// xorWrite writes a single XOR delta. State (leading, trailing) is carried between
// samples; the caller initializes leading=0xff to force the first delta into the
// "new leading/trailing" case.
func xorWrite(w *bitstream.Writer, newVal, curVal float64, leading, trailing *uint8) {
	delta := math.Float64bits(newVal) ^ math.Float64bits(curVal)
	if delta == 0 {
		w.WriteBit(false)
		return
	}
	w.WriteBit(true)

	newLeading := uint8(bits.LeadingZeros64(delta))
	newTrailing := uint8(bits.TrailingZeros64(delta))
	// Clamp to 31 to fit the 5-bit field.
	if newLeading >= 32 {
		newLeading = 31
	}

	if *leading != 0xff && newLeading >= *leading && newTrailing >= *trailing {
		// Reuse previous leading/trailing.
		w.WriteBit(false)
		sigbits := 64 - int(*leading) - int(*trailing)
		w.WriteBits(delta>>*trailing, sigbits)
		return
	}

	// New leading/trailing.
	*leading, *trailing = newLeading, newTrailing
	w.WriteBit(true)
	w.WriteBits(uint64(newLeading), 5)

	sigbits := 64 - newLeading - newTrailing
	// sigbits==0 is a sentinel for 64 (when leading==trailing==0).
	w.WriteBits(uint64(sigbits), 6)
	w.WriteBits(delta>>newTrailing, int(sigbits))
}

// xorRead reads a single XOR delta and updates the value. State (leading, trailing)
// is carried between samples.
func xorRead(r *bitstream.Reader, val *float64, leading, trailing *uint8) error {
	bit, err := r.ReadBit()
	if err != nil {
		return err
	}
	if !bit {
		return nil // value unchanged
	}

	bit, err = r.ReadBit()
	if err != nil {
		return err
	}

	var newLeading, newTrailing, sigbits uint8
	if !bit {
		// Reuse previous leading/trailing.
		newLeading, newTrailing = *leading, *trailing
		sigbits = 64 - newLeading - newTrailing
	} else {
		// New leading/trailing.
		lbits, err := r.ReadBits(5)
		if err != nil {
			return err
		}
		newLeading = uint8(lbits)

		sbits, err := r.ReadBits(6)
		if err != nil {
			return err
		}
		sigbits = uint8(sbits)
		if sigbits == 0 {
			sigbits = 64 // sentinel
		}
		newTrailing = 64 - newLeading - sigbits
		*leading, *trailing = newLeading, newTrailing
	}

	mbits, err := r.ReadBits(sigbits)
	if err != nil {
		return err
	}
	vbits := math.Float64bits(*val)
	vbits ^= mbits << newTrailing
	*val = math.Float64frombits(vbits)
	return nil
}
