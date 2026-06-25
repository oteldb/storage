package chunk

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDecodeCorrupt exercises error paths in each codec.
func TestDecodeCorrupt(t *testing.T) {
	t.Parallel()
	// Truncated timestamp stream.
	enc := EncodeTimestamps(nil, []int64{0, 1000, 2000})
	_, _, err := DecodeTimestamps(nil, enc[:2])
	require.Error(t, err, "expected error from truncated timestamps")
	// Truncated float stream.
	enc = EncodeFloats(nil, []float64{1.0, 2.0, 3.0})
	_, _, err = DecodeFloats(nil, enc[:2])
	require.Error(t, err, "expected error from truncated floats")
	// Truncated T64 stream.
	enc = EncodeIntsT64(nil, []int64{1, 2, 3, 4, 5})
	_, _, err = DecodeIntsT64(nil, enc[:3])
	require.Error(t, err, "expected error from truncated T64")
	// Truncated decimal stream.
	enc = EncodeFloatsDecimal(nil, []float64{1.0, 2.0, 3.0}, 64)
	_, _, err = DecodeFloatsDecimal(nil, enc[:3])
	require.Error(t, err, "expected error from truncated decimal")
}

func TestDecodeFloatsReuseLeadingTrailing(t *testing.T) {
	t.Parallel()
	// Values that exercise the "reuse" branch in decode: consecutive XORs
	// where the second fits within the first's leading/trailing window.
	vals := []float64{
		1.0, 1.0000001, 1.0000002, 1.0000003,
	}
	enc := EncodeFloats(nil, vals)

	got, _, err := DecodeFloats(nil, enc)
	require.NoError(t, err)
	assert.Equal(t, vals, got)
}

func TestDecodeTimestampsIntoExistingSlice(t *testing.T) {
	t.Parallel()
	// Verify decode reuses an existing slice with enough capacity.
	dst := make([]int64, 0, 100)
	ts := []int64{0, 1000, 2000, 3000}
	enc := EncodeTimestamps(nil, ts)

	got, _, err := DecodeTimestamps(dst, enc)
	require.NoError(t, err)
	assert.Equal(t, 100, cap(got), "expected capacity reuse")
}

func TestDecodeFloatsIntoExistingSlice(t *testing.T) {
	t.Parallel()

	dst := make([]float64, 0, 100)
	vals := []float64{1.0, 2.0, 3.0}
	enc := EncodeFloats(nil, vals)

	got, _, err := DecodeFloats(dst, enc)
	require.NoError(t, err)
	assert.Equal(t, 100, cap(got), "expected capacity reuse")
}

func TestEncodeFloatsDecimalPrecision0(t *testing.T) {
	t.Parallel()
	// precisionBits=0 should default to 64 (lossless).
	vals := []float64{0, 10, 20, 30}
	enc := EncodeFloatsDecimal(nil, vals, 0)

	got, _, err := DecodeFloatsDecimal(nil, enc)
	require.NoError(t, err)
	assert.Equal(t, vals, got)
}

func TestDecodeBytesTruncated(t *testing.T) {
	t.Parallel()
	// Truncated dictionary stream.
	enc := EncodeBytes(nil, [][]byte{[]byte("a"), []byte("b"), []byte("c")})
	_, _, err := DecodeBytes(nil, enc[:2])
	require.Error(t, err, "expected error from truncated strings")
}
