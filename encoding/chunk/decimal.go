package chunk

import (
	"math"
	"math/bits"

	"github.com/oteldb/storage/encoding/bitstream"
)

// EncodeFloatsDecimal appends a scaled-decimal + nearest-delta encoded float64 column
// to dst (DESIGN.md §6, §14 M0; VictoriaMetrics-style). precisionBits controls the
// lossiness: 64 = lossless (no bit zeroing), <64 = lossy (zeros trailing low bits of
// deltas so the varint stream is ZSTD-compressible, reaching ~0.4–0.8 B/point).
//
// Layout: [uvarint rows] [varint exponent] [uvarint precisionBits]
//
//	[zigzag-varint v0] [zigzag-varint delta1] ... [zigzag-varint deltaN-1]
//
// Pipeline:
//  1. Convert each float64 to a scaled int64 with a shared exponent e: f ≈ v * 10^e.
//     The mantissa is minimized (trailing decimal zeros stripped into e).
//  2. Compute first differences: d_n = v_n - v_{n-1}.
//  3. If precisionBits < 64, zero the trailing (originBits - precisionBits) low bits
//     of each delta (the "nearest-delta" step), with counter-reset hysteresis.
//  4. Zigzag-varint encode the deltas; v0 is stored out-of-band.
//
// With precisionBits=64 the round-trip is exact for integer-valued floats and
// near-exact (within float64 rounding) for fractional values. With precisionBits<64
// the codec is deliberately lossy.
func EncodeFloatsDecimal(dst []byte, vals []float64, precisionBits uint8) []byte {
	w := bitstream.NewWriter(dst)
	w.WriteUvarint(uint64(len(vals)))
	if len(vals) == 0 {
		w.PadToByte()
		return w.Bytes()
	}

	if precisionBits < 1 {
		precisionBits = 64
	}
	w.WriteUvarint(uint64(precisionBits))

	// Step 1: batch-convert to scaled int64s with a shared exponent.
	scaled, exp := floatsToDecimal(vals)
	w.WriteVarint(int64(exp))

	// Step 2-4: delta + nearest-delta + zigzag varint.
	w.WriteVarint(scaled[0])
	prevTZ := 0
	for i := 1; i < len(scaled); i++ {
		d := scaled[i] - scaled[i-1]
		if precisionBits < 64 && d != 0 {
			d, prevTZ = nearestDelta(d, scaled[i], precisionBits, prevTZ)
		}
		w.WriteVarint(d)
	}

	w.PadToByte()
	return w.Bytes()
}

// DecodeFloatsDecimal decodes a scaled-decimal + nearest-delta encoded float64 column.
func DecodeFloatsDecimal(dst []float64, src []byte) ([]float64, int, error) {
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

	pb, err := r.ReadUvarint()
	if err != nil {
		return dst, 0, err
	}
	precisionBits := uint8(pb)

	exp64, err := r.ReadVarint()
	if err != nil {
		return dst, 0, err
	}
	exp := int(exp64)

	v0, err := r.ReadVarint()
	if err != nil {
		return dst, 0, err
	}
	scaled := make([]int64, rows)
	scaled[0] = v0
	for i := 1; i < rows; i++ {
		d, err := r.ReadVarint()
		if err != nil {
			return dst, 0, err
		}
		scaled[i] = scaled[i-1] + d
	}

	// Convert back to floats.
	scale := math.Pow(10, float64(exp))
	for i, v := range scaled {
		dst[i] = decimalToFloat(v, scale)
	}
	_ = precisionBits // precision only affects encoding (lossy); decode is the same
	return dst, consumed + r.ConsumedBytes(), nil
}

// floatsToDecimal converts a batch of float64 to scaled int64s with a shared exponent.
// It minimizes each mantissa (strips trailing decimal zeros) then aligns all values to
// the minimum exponent across the batch (upscaling where it won't overflow int64).
func floatsToDecimal(vals []float64) ([]int64, int) {
	scaled := make([]int64, len(vals))
	exps := make([]int, len(vals))

	minExp := math.MaxInt
	for i, f := range vals {
		v, e := floatToDecimal(f)
		scaled[i] = v
		exps[i] = e
		if e < minExp {
			minExp = e
		}
	}

	// Align all to minExp by upscaling (multiply by 10^(e - minExp)).
	for i := range vals {
		shift := exps[i] - minExp
		for shift > 0 {
			scaled[i] *= 10
			shift--
		}
	}

	return scaled, minExp
}

// floatToDecimal converts a single float64 to a (mantissa, exponent) pair with the
// mantissa minimized (trailing decimal zeros stripped). f ≈ v * 10^e.
// Special values: ±Inf → max/min int64; NaN → 0.
func floatToDecimal(f float64) (int64, int) {
	if math.IsNaN(f) {
		return 0, 0
	}
	if math.IsInf(f, 1) {
		return math.MaxInt64, 0
	}
	if math.IsInf(f, -1) {
		return math.MinInt64, 0
	}

	negative := f < 0
	if negative {
		f = -f
	}

	// Fast path: integer-valued floats that fit in int64.
	if f < 9.2e18 { // int64 max ≈ 9.22e18
		u := int64(f)
		if float64(u) == f {
			// Strip trailing decimal zeros into the exponent.
			e := 0
			for u != 0 && u%10 == 0 {
				u /= 10
				e++
			}
			if negative {
				u = -u
			}
			return u, e
		}
	}

	// Slow path: fractional values. Scale up to an integer.
	// Use a precision that won't overflow int64 for the given magnitude.
	var scaled int64
	var e int
	if f < 1e6 {
		scaled = int64(f * 1e12)
		e = -12
	} else {
		scaled = int64(f * 1e6)
		e = -6
	}

	// Strip trailing zeros.
	for scaled != 0 && scaled%10 == 0 {
		scaled /= 10
		e++
	}

	if negative {
		scaled = -scaled
	}
	return scaled, e
}

// decimalToFloat converts (mantissa, scale) back to a float64.
func decimalToFloat(v int64, scale float64) float64 {
	// Handle special sentinel values from floatToDecimal.
	switch v {
	case math.MaxInt64:
		return math.Inf(1)
	case math.MinInt64:
		return math.Inf(-1)
	case 0:
		return 0
	}
	return float64(v) * scale
}

// nearestDelta zeros the trailing low bits of a delta to make the varint stream
// ZSTD-compressible (the VictoriaMetrics "nearest-delta" step). It returns the
// zeroed delta and the new trailingZeros count. Counter-reset hysteresis: if
// trailingZeros jumps by more than ±4, the full-precision delta is emitted and the
// running trailingZeros is nudged by ±2 (to avoid a counter reset poisoning the
// zeroing width for subsequent samples).
func nearestDelta(d, origin int64, precisionBits uint8, prevTZ int) (int64, int) {
	if d == 0 {
		return 0, prevTZ
	}
	absOrigin := origin
	if absOrigin < 0 {
		absOrigin = -absOrigin
	}
	originBits := 64 - bits.LeadingZeros64(uint64(absOrigin))
	if originBits <= int(precisionBits) {
		return d, prevTZ // already small enough
	}
	tz := originBits - int(precisionBits)
	if tz < 0 {
		tz = 0
	}

	// Counter-reset hysteresis.
	if tz-prevTZ > 4 {
		// Sudden increase — likely a counter reset. Emit full precision, nudge tz.
		return d, prevTZ + 2
	}
	if prevTZ-tz > 4 {
		return d, prevTZ - 2
	}

	// Zero the low tz bits of d.
	minus := d < 0
	if minus {
		d = -d
	}
	mask := uint64(^uint64(0)) << tz
	d = int64(uint64(d) & mask)
	if minus {
		d = -d
	}
	return d, tz
}
