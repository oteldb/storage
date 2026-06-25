package chunk

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGorillaRoundTrip verifies EncodeFloats∘DecodeFloats == identity.
func TestGorillaRoundTrip(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		vals []float64
	}{
		{"empty", nil},
		{"single", []float64{42.0}},
		{"two", []float64{42.0, 43.0}},
		{"constant", makeConstantFloats(100, 42.0)},
		{"slowly-changing", makeSlowFloats(100, 42.0, 0.001)},
		{"integers", []float64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}},
		{"large-range", []float64{0, 1e10, 1e20, 1e100, -1e100, 1e300}},
		{"nan-inf", []float64{math.NaN(), math.Inf(1), math.Inf(-1), 0.0}},
		{"stale-nan", []float64{42.0, math.NaN(), 42.0, math.NaN()}}, // Prometheus stale marker
		{"alternating", []float64{1.0, -1.0, 1.0, -1.0, 1.0, -1.0}},
		{"precision", []float64{0.1, 0.2, 0.3, 0.4, 0.5}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			enc := EncodeFloats(nil, tc.vals)

			got, _, err := DecodeFloats(nil, enc)
			require.NoError(t, err)
			require.Len(t, got, len(tc.vals))

			for i := range tc.vals {
				if math.IsNaN(tc.vals[i]) {
					assert.Truef(t, math.IsNaN(got[i]), "vals[%d] = %v, want NaN", i, got[i])

					continue
				}

				assert.InDeltaf(t, tc.vals[i], got[i], 0, "vals[%d]", i)
			}
		})
	}
}

// TestGorillaConstantCompression verifies constant values achieve ~1 bit/sample.
func TestGorillaConstantCompression(t *testing.T) {
	t.Parallel()

	vals := makeConstantFloats(1000, 42.0)
	enc := EncodeFloats(nil, vals)
	// 1000 constant floats → 1000 bits (all delta==0 → single 0 bit) → ~125 bytes.
	maxBytes := 200
	assert.LessOrEqualf(t, len(enc), maxBytes, "compressed size for 1000 constant floats")
}

func makeConstantFloats(n int, v float64) []float64 {
	vals := make([]float64, n)
	for i := range n {
		vals[i] = v
	}

	return vals
}

func makeSlowFloats(n int, start, delta float64) []float64 {
	vals := make([]float64, n)

	v := start
	for i := range n {
		vals[i] = v
		v += delta
	}

	return vals
}
