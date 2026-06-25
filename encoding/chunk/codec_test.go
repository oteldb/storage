package chunk

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCodecString(t *testing.T) {
	t.Parallel()

	cases := []struct {
		c    Codec
		want string
	}{
		{CodecNone, "none"},
		{CodecDoD, "dod"},
		{CodecGorilla, "gorilla"},
		{CodecDict, "dict"},
		{CodecT64, "t64"},
		{CodecDecimal, "decimal"},
		{Codec(99), "unknown"},
	}
	for _, tc := range cases {
		assert.Equalf(t, tc.want, tc.c.String(), "Codec(%d).String()", tc.c)
	}
}

func TestDoDAllCases(t *testing.T) {
	t.Parallel()
	// Exercise each DoD bit-width case explicitly.
	ts := []int64{
		0,                                    // row 0
		1000,                                 // row 1 (first delta = 1000)
		1000,                                 // dod = 0 → case 0b0
		1000 + 5000,                          // dod = 5000 → fits in 14 bits → case 0b10
		1000 + 5000 + 60000,                  // dod = 55000 → fits in 17 bits → case 0b110
		1000 + 5000 + 60000 + 500000,         // dod = 500000 → fits in 20 bits → case 0b1110
		1000 + 5000 + 60000 + 500000 + 1<<40, // dod = 2^40 → escape → case 0b1111
	}
	enc := EncodeTimestamps(nil, ts)

	got, _, err := DecodeTimestamps(nil, enc)
	require.NoError(t, err)
	assert.Equal(t, ts, got)
}

func TestDoDNegativeCases(t *testing.T) {
	t.Parallel()
	// Negative dod values in each bit-width case.
	ts := []int64{
		0,                            // row 0
		1000,                         // row 1
		1000,                         // dod = 0
		1000 - 5000,                  // dod = -5000 → 14 bits
		1000 - 5000 - 60000,          // dod = -60000 → 17 bits
		1000 - 5000 - 60000 - 500000, // dod = -500000 → 20 bits
	}
	enc := EncodeTimestamps(nil, ts)

	got, _, err := DecodeTimestamps(nil, enc)
	require.NoError(t, err)
	assert.Equal(t, ts, got)
}

func TestNearestDelta(t *testing.T) {
	t.Parallel()
	// With precisionBits=64, nearestDelta returns d unchanged.
	d, tz := nearestDelta(100, 200, 64, 0)
	assert.Equal(t, int64(100), d, "precisionBits=64")
	assert.Equal(t, 0, tz, "precisionBits=64")

	// With precisionBits=8, a large origin zeros trailing bits.
	// Use prevTZ=10 so the counter-reset hysteresis (±4) doesn't fire.
	d, tz = nearestDelta(12345, 1000000, 8, 10)
	assert.Positive(t, tz, "precisionBits=8: expected tz > 0")
	// The zeroed delta should have low bits cleared.
	if d != 0 {
		assert.Zerof(t, d&((1<<tz)-1), "zeroed delta has low bits set: d=%d tz=%d", d, tz)
	}

	// d == 0 fast path.
	d, tz = nearestDelta(0, 100, 16, 5)
	assert.Equal(t, int64(0), d, "d=0 fast path")
	assert.Equal(t, 5, tz, "d=0 fast path")

	// Counter-reset hysteresis: sudden tz jump.
	d, _ = nearestDelta(100, 1<<60, 16, 0)
	assert.Equal(t, int64(100), d, "counter reset (jump up): want full precision")
	// Counter-reset hysteresis: sudden tz drop.
	d, _ = nearestDelta(100, 100, 16, 30)
	assert.Equal(t, int64(100), d, "counter reset (jump down): want full precision")
}

func TestFloatToDecimalFractional(t *testing.T) {
	t.Parallel()
	// Fractional value that exercises the slow path.
	v, e := floatToDecimal(1.5)
	assert.Equal(t, int64(15), v, "floatToDecimal(1.5)")
	assert.Equal(t, -1, e, "floatToDecimal(1.5)")

	v, e = floatToDecimal(0.5)
	assert.Equal(t, int64(5), v, "floatToDecimal(0.5)")
	assert.Equal(t, -1, e, "floatToDecimal(0.5)")
}

func TestErrorStrings(t *testing.T) {
	t.Parallel()

	require.EqualError(t, errEOF, "chunk: unexpected end of stream")
	require.EqualError(t, errUnexpectedEOF, "chunk: truncated stream")
}

func TestIsEOF(t *testing.T) {
	t.Parallel()

	assert.True(t, IsEOF(errEOF), "IsEOF(errEOF)")
	assert.False(t, IsEOF(errUnexpectedEOF), "IsEOF(errUnexpectedEOF)")
}

func TestGorillaReuseCase(t *testing.T) {
	t.Parallel()
	// Force the "reuse" case: values where consecutive XORs have leading/trailing
	// within the previous window.
	vals := []float64{
		1.0, 1.0, 1.0, // delta=0 for second → unchanged case
		2.0, 2.0, 2.0, // XOR then unchanged
		3.0, 3.0, 3.0, // XOR then unchanged
	}
	enc := EncodeFloats(nil, vals)

	got, _, err := DecodeFloats(nil, enc)
	require.NoError(t, err)
	assert.Equal(t, vals, got)
}

func TestGorillaNaNRoundTrip(t *testing.T) {
	t.Parallel()

	vals := []float64{1.0, math.NaN(), 2.0, math.NaN()}
	enc := EncodeFloats(nil, vals)

	got, _, err := DecodeFloats(nil, enc)
	require.NoError(t, err)

	assert.InDelta(t, 1.0, got[0], 0, "non-NaN value at index 0")
	assert.InDelta(t, 2.0, got[2], 0, "non-NaN value at index 2")
}

func TestGorillaAllBitsChanged(t *testing.T) {
	t.Parallel()
	// XOR with no leading/trailing zeros → sigbits=64 → sentinel case.
	vals := []float64{0.0, math.Float64frombits(0x8000000000000001)} // -5e-324 (denorm)
	enc := EncodeFloats(nil, vals)

	got, _, err := DecodeFloats(nil, enc)
	require.NoError(t, err)

	assert.Equal(t, math.Float64bits(vals[0]), math.Float64bits(got[0]))
	assert.Equal(t, math.Float64bits(vals[1]), math.Float64bits(got[1]))
}
