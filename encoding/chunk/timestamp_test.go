package chunk

import (
	"errors"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDoDRoundTrip verifies EncodeTimestamps∘DecodeTimestamps == identity for a range
// of timestamp patterns (constant stride, jitter, monotonic, empty, single).
func TestDoDRoundTrip(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		ts   []int64
	}{
		{"empty", nil},
		{"single", []int64{1000}},
		{"two", []int64{1000, 2000}},
		{"constant-stride", makeConstantStride(120, 1_000_000_000, 15_000)},
		{"jittered", makeJittered(120, 1_000_000_000, 15_000, 100)},
		{"burst", []int64{0, 1, 2, 1000, 1001, 1002, 1_000_000, 1_000_001}},
		{"large-jumps", []int64{0, 1 << 10, 1 << 20, 1 << 30, 1 << 40, 1 << 50, 1 << 60}},
		{"negative-dod", []int64{0, 100, 200, 150, 100, 50}}, // dod goes negative
		{"max-int64", []int64{math.MaxInt64 - 5, math.MaxInt64 - 4, math.MaxInt64 - 3}},
		{"min-int64", []int64{math.MinInt64, math.MinInt64 + 1, math.MinInt64 + 2}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			enc := EncodeTimestamps(nil, tc.ts)

			got, _, err := DecodeTimestamps(nil, enc)
			require.NoError(t, err)
			assert.Equal(t, tc.ts, got)
		})
	}
}

// TestDoDCompressionRatio verifies constant-stride timestamps achieve ~1 bit/sample.
func TestDoDCompressionRatio(t *testing.T) {
	t.Parallel()

	ts := makeConstantStride(1000, 1_000_000_000, 15_000)
	enc := EncodeTimestamps(nil, ts)
	// 1000 samples → ~1000 bits (all dod==0) → ~125 bytes + header.
	// Allow some overhead for the header uvarint and first two samples.
	maxBytes := 200 // generous; ~0.16 bytes/sample
	assert.LessOrEqualf(t, len(enc), maxBytes, "compressed size for 1000 constant-stride ts")
}

func makeConstantStride(n int, start, step int64) []int64 {
	ts := make([]int64, n)
	for i := range n {
		ts[i] = start + int64(i)*step
	}

	return ts
}

func makeJittered(n int, start, step, jitter int64) []int64 {
	ts := make([]int64, n)

	ts[0] = start
	for i := 1; i < n; i++ {
		ts[i] = ts[i-1] + step + (int64(i)*7)%jitter - jitter/2
	}

	return ts
}

func TestBitRange(t *testing.T) {
	t.Parallel()

	cases := []struct {
		x    int64
		n    uint8
		want bool
	}{
		{0, 14, true},
		{1, 14, true},
		{8192, 14, true},   // 2^13
		{8193, 14, false},  // > 2^13
		{-8191, 14, true},  // -(2^13-1)
		{-8192, 14, false}, // < -(2^13-1)
		{65536, 17, true},  // 2^16
		{65537, 17, false},
		{524288, 20, true}, // 2^19
		{524289, 20, false},
	}
	for _, tc := range cases {
		assert.Equalf(t, tc.want, bitRange(tc.x, tc.n), "bitRange(%d, %d)", tc.x, tc.n)
	}
}

func TestSignExtend(t *testing.T) {
	t.Parallel()

	cases := []struct {
		u    uint64
		n    uint8
		want int64
	}{
		{0, 14, 0},
		{1, 14, 1},
		{8192, 14, 8192},             // max positive (1<<13): NOT sign-extended (asymmetric range)
		{8193, 14, 8193 - 16384},     // → negative
		{0x3fff, 14, 0x3fff - 16384}, // = -1
	}
	for _, tc := range cases {
		assert.Equalf(t, tc.want, signExtend(tc.u, tc.n), "signExtend(%#x, %d)", tc.u, tc.n)
	}
}

func TestDoDEmptyDecode(t *testing.T) {
	t.Parallel()

	enc := EncodeTimestamps(nil, nil)

	got, n, err := DecodeTimestamps(nil, enc)
	require.NoError(t, err)
	assert.Empty(t, got)
	assert.Equal(t, len(enc), n)
}

func TestDoDTruncated(t *testing.T) {
	t.Parallel()

	enc := EncodeTimestamps(nil, []int64{1000, 2000, 3000})
	// Truncate the encoded bytes to simulate a torn stream.
	_, _, err := DecodeTimestamps(nil, enc[:1])
	require.Error(t, err, "expected error from truncated stream")

	// Any error is acceptable here (IsEOF, errUnexpectedEOF, or a bitstream-specific
	// report); the key is that we don't panic.
	_ = errors.Is(err, errUnexpectedEOF)
}
