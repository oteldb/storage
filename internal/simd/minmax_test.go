package simd

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

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
