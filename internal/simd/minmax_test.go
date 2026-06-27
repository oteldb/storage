package simd

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

// fbits canonicalizes a float64 to its bit pattern for exact comparison, normalizing -0 to +0 (the
// two are equal in IEEE and min/max may legitimately produce either). Lets tests assert exact
// float equality as a uint64 comparison.
func fbits(f float64) uint64 {
	if f == 0 {
		return 0
	}

	return math.Float64bits(f)
}

func naiveMinMax(s []int64) (mn, mx int64) {
	mn, mx = s[0], s[0]
	for _, v := range s {
		mn = min(mn, v)
		mx = max(mx, v)
	}

	return mn, mx
}

func TestMinMaxInt64(t *testing.T) {
	t.Parallel()

	cases := [][]int64{
		{42},
		{3, 1, 2},
		{-5, -1, -9, -3},
		{math.MaxInt64, math.MinInt64, 0},
		{1, 1, 1, 1, 1, 1, 1, 1, 1}, // > 8 elements, all equal
		{9, 8, 7, 6, 5, 4, 3, 2, 1, 0, -1, -2, -3}, // spans vector body + tail
	}

	for _, s := range cases {
		mn, mx := MinMaxInt64(s)
		wmn, wmx := naiveMinMax(s)
		assert.Equalf(t, wmn, mn, "min of %v", s)
		assert.Equalf(t, wmx, mx, "max of %v", s)
	}

	// Empty slice is defined to return (0, 0).
	mn, mx := MinMaxInt64(nil)
	assert.Zero(t, mn)
	assert.Zero(t, mx)
}

func TestMinMaxFloat64(t *testing.T) {
	t.Parallel()

	inf, ninf, nan := math.Inf(1), math.Inf(-1), math.NaN()

	cases := []struct {
		s              []float64
		wantMn, wantMx float64
	}{
		{[]float64{42}, 42, 42},
		{[]float64{3, 1, 2}, 1, 3},
		{[]float64{-5.5, -1, -9, -3}, -9, -1},
		{[]float64{1, nan, 2, nan, 3}, 1, 3},                        // NaN ignored
		{[]float64{nan, nan, nan}, inf, ninf},                       // all-NaN ⇒ sentinel (min > max)
		{[]float64{ninf, 0, inf}, ninf, inf},                        // real infinities kept
		{[]float64{5, 4, 3, 2, 1, 0, -1, -2, -3, nan, -10}, -10, 5}, // vector body + tail + NaN
		{[]float64{2, 2, 2, 2, 2, 2, 2, 2, 2}, 2, 2},                // >8 equal
	}

	for _, tc := range cases {
		mn, mx := MinMaxFloat64(tc.s)
		// Exact comparison (not epsilon) via bit patterns: min/max must be the precise value.
		assert.Equalf(t, fbits(tc.wantMn), fbits(mn), "min of %v", tc.s)
		assert.Equalf(t, fbits(tc.wantMx), fbits(mx), "max of %v", tc.s)
	}

	// Empty ⇒ the (+Inf, -Inf) sentinel.
	mn, mx := MinMaxFloat64(nil)
	assert.Equal(t, fbits(inf), fbits(mn), "empty min")
	assert.Equal(t, fbits(ninf), fbits(mx), "empty max")
}

// FuzzMinMaxFloat64 checks the AVX2 kernel against the pure-Go reference on arbitrary inputs (the
// dispatch routes len>=8 to AVX2 on an AVX2 CPU), so the assembly's NaN handling is verified.
func FuzzMinMaxFloat64(f *testing.F) {
	f.Add([]byte{1, 2, 3, 4, 5, 6, 7, 8})
	f.Add(make([]byte, 8*10))

	f.Fuzz(func(t *testing.T, data []byte) {
		s := make([]float64, len(data)/8)
		for i := range s {
			s[i] = math.Float64frombits(binary.LittleEndian.Uint64(data[i*8:]))
		}

		mn, mx := MinMaxFloat64(s)
		wmn, wmx := minMaxFloat64Generic(s)

		// Exact comparison via bit patterns (±0 normalized; results are never NaN).
		assert.Equalf(t, fbits(wmn), fbits(mn), "min mismatch for %v", s)
		assert.Equalf(t, fbits(wmx), fbits(mx), "max mismatch for %v", s)
	})
}

func BenchmarkMinMaxFloat64(b *testing.B) {
	s := make([]float64, 4096)
	for i := range s {
		s[i] = float64(i*2654435761^i) * 0.5
	}

	b.SetBytes(int64(len(s)) * 8)
	b.ReportAllocs()

	for b.Loop() {
		MinMaxFloat64(s)
	}
}

func BenchmarkMinMaxInt64(b *testing.B) {
	s := make([]int64, 4096)
	for i := range s {
		s[i] = int64(i*2654435761) ^ int64(i)
	}

	b.SetBytes(int64(len(s)) * 8)
	b.ReportAllocs()

	for b.Loop() {
		MinMaxInt64(s)
	}
}
