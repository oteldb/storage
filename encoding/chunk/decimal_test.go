package chunk

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDecimalRoundTripLossless verifies that with precisionBits=64, integer-valued
// floats round-trip exactly.
func TestDecimalRoundTripLossless(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		vals []float64
	}{
		{"empty", nil},
		{"single", []float64{42.0}},
		{"integers", []float64{0, 1, 2, 3, 100, 1000, 1_000_000, 1e15}},
		{"negative-ints", []float64{-1, -100, -1000, -1e10}},
		{"constant", makeConstantFloats(100, 42.0)},
		{"monotonic", []float64{10, 20, 30, 40, 50, 60, 70, 80, 90, 100}},
		{"zeros", []float64{0, 0, 0, 0, 0}},
		{"trailing-zeros", []float64{100, 200, 300, 400}}, // mantissa gets stripped
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			enc := EncodeFloatsDecimal(nil, tc.vals, 64)

			got, _, err := DecodeFloatsDecimal(nil, enc)
			require.NoError(t, err)
			assert.Equal(t, tc.vals, got)
		})
	}
}

// TestDecimalRoundTripApproximate verifies fractional floats round-trip within a
// small relative tolerance (the scaled-decimal conversion introduces float rounding).
func TestDecimalRoundTripApproximate(t *testing.T) {
	t.Parallel()

	vals := []float64{0.1, 0.2, 0.3, 0.4, 0.5, 1.5, 2.5, 3.14, 2.718}
	enc := EncodeFloatsDecimal(nil, vals, 64)

	got, _, err := DecodeFloatsDecimal(nil, enc)
	require.NoError(t, err)
	require.Len(t, got, len(vals))

	for i := range vals {
		assert.InDeltaf(t, vals[i], got[i], 1e-9, "vals[%d]", i)
	}
}

// TestDecimalLossy verifies precisionBits < 64 is lossy but within the expected bound.
func TestDecimalLossy(t *testing.T) {
	t.Parallel()

	vals := makeConstantFloats(100, 42.0)
	vals[50] = 42.001                         // small perturbation
	enc := EncodeFloatsDecimal(nil, vals, 16) // 16 bits of precision

	got, _, err := DecodeFloatsDecimal(nil, enc)
	require.NoError(t, err)
	require.Len(t, got, len(vals))
	// With 16 bits of precision, the low bits are zeroed; the perturbation may
	// be partially lost. Check the constant values are preserved and the
	// perturbation is within a reasonable bound.
	for i := range vals {
		assert.InEpsilonf(t, vals[i], got[i], 0.1, "vals[%d]", i)
	}
}

// TestDecimalSpecialValues verifies Inf/NaN handling.
func TestDecimalSpecialValues(t *testing.T) {
	t.Parallel()

	vals := []float64{math.Inf(1), math.Inf(-1), 0, 42}
	enc := EncodeFloatsDecimal(nil, vals, 64)

	got, _, err := DecodeFloatsDecimal(nil, enc)
	require.NoError(t, err)

	assert.True(t, math.IsInf(got[0], 1), "vals[0] want +Inf")
	assert.True(t, math.IsInf(got[1], -1), "vals[1] want -Inf")
	assert.InDelta(t, 0.0, got[2], 0, "vals[2] want 0")
	assert.InDelta(t, 42.0, got[3], 0, "vals[3] want 42")
}

func TestFloatToDecimal(t *testing.T) {
	t.Parallel()

	cases := []struct {
		f     float64
		wantV int64
		wantE int
	}{
		{0, 0, 0},
		{1, 1, 0},
		{10, 1, 1}, // trailing zero stripped
		{100, 1, 2},
		{1000, 1, 3},
		{-10, -1, 1},
		{42, 42, 0}, // no trailing zeros
	}
	for _, tc := range cases {
		v, e := floatToDecimal(tc.f)
		assert.Equalf(t, tc.wantV, v, "floatToDecimal(%v) value", tc.f)
		assert.Equalf(t, tc.wantE, e, "floatToDecimal(%v) exp", tc.f)
	}
}
