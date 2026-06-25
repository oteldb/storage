package chunk

import (
	"math"
	"testing"
)

// TestT64RoundTrip verifies EncodeIntsT64∘DecodeIntsT64 == identity (lossless).
func TestT64RoundTrip(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		vals []int64
	}{
		{"empty", nil},
		{"single", []int64{42}},
		{"constant", []int64{100, 100, 100, 100}},
		{"small-range", []int64{0, 1, 2, 3, 4, 5, 6, 7}},
		{"bools", makeBools(64)},
		{"full-block", makeRange(0, 64)},
		{"two-blocks", makeRange(0, 128)},
		{"partial-block", makeRange(0, 100)},
		{"large-values", []int64{1 << 60, (1 << 60) + 1, (1 << 60) + 2}},
		{"negative", []int64{-1, -2, -3, -4}},
		{"signed-straddle", []int64{-100, -50, 0, 50, 100}},
		{"max-min", []int64{math.MaxInt64, math.MinInt64}},
		{"uint8-range", makeRange(0, 256)},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			enc := EncodeIntsT64(nil, tc.vals)
			got, _, err := DecodeIntsT64(nil, enc)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if len(got) != len(tc.vals) {
				t.Fatalf("len = %d, want %d", len(got), len(tc.vals))
			}
			for i := range tc.vals {
				if got[i] != tc.vals[i] {
					t.Fatalf("vals[%d] = %d, want %d", i, got[i], tc.vals[i])
				}
			}
		})
	}
}

// TestT64ConstantCompression verifies a constant column stores only the header.
func TestT64ConstantCompression(t *testing.T) {
	t.Parallel()
	vals := makeConstantInts(1000, 42)
	enc := EncodeIntsT64(nil, vals)
	// numBits=0 → only [uvarint rows][16 bytes header] → ~18 bytes.
	maxBytes := 25
	if len(enc) > maxBytes {
		t.Fatalf("compressed size = %d for 1000 constant ints, want ≤ %d", len(enc), maxBytes)
	}
}

// TestT64BoolCompression verifies 0/1 values collapse to ~1 bit/value.
func TestT64BoolCompression(t *testing.T) {
	t.Parallel()
	vals := makeBools(640) // 10 blocks of 64
	enc := EncodeIntsT64(nil, vals)
	// numBits=1 → 1 transposed row × 8 bytes per block of 64 → 80 bytes + header.
	maxBytes := 100
	if len(enc) > maxBytes {
		t.Fatalf("compressed size = %d for 640 bools, want ≤ %d", len(enc), maxBytes)
	}
}

func makeRange(start, n int) []int64 {
	vals := make([]int64, n)
	for i := range n {
		vals[i] = int64(start + i)
	}
	return vals
}

func makeBools(n int) []int64 {
	vals := make([]int64, n)
	for i := range n {
		vals[i] = int64(i % 2)
	}
	return vals
}

func makeConstantInts(n int, v int64) []int64 {
	vals := make([]int64, n)
	for i := range n {
		vals[i] = v
	}
	return vals
}

func TestValuableBits(t *testing.T) {
	t.Parallel()
	cases := []struct {
		min, max uint64
		straddle bool
		want     uint8
	}{
		{0, 0, false, 0},                   // constant
		{0, 1, false, 1},                   // 1 bit
		{0, 255, false, 8},                 // 8 bits
		{0, 65535, false, 16},              // 16 bits
		{1 << 60, (1 << 60) | 7, false, 3}, // high values, low variance
		{0, 1, true, 2},                    // signed straddle → +1
	}
	for _, tc := range cases {
		if got := valuableBits(tc.min, tc.max, tc.straddle); got != tc.want {
			t.Errorf("valuableBits(%d, %d, %v) = %d, want %d", tc.min, tc.max, tc.straddle, got, tc.want)
		}
	}
}
