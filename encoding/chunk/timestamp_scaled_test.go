package chunk

import (
	"math"
	"testing"

	"github.com/go-faster/sdk/gold"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTsScale(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		ts   []int64
		want int64
	}{
		{"empty", nil, 1e9},
		{"single", []int64{1_700_000_000_123_456_789}, 1e9},
		{"seconds", []int64{1e9, 2e9, 4e9}, 1e9},
		{"milliseconds", []int64{1_000_000, 16_000_000, 31_000_000}, 1e6},
		{"microseconds", []int64{1_000, 16_000, 31_000}, 1e3},
		{"nanoseconds", []int64{1, 16, 31}, 1},
		{"steps-down-monotonically", []int64{0, 1e9, 1e9 + 1e6, 1e9 + 1e6 + 1e3}, 1e3},
		{"ns-at-first-delta", []int64{0, 7, 1e9}, 1},
		{"aligned-absolute-irrelevant", []int64{123, 123 + 1e6, 123 + 2e6}, 1e6},
		{"duplicate-timestamps", []int64{5e6, 5e6, 6e6}, 1e6},
		{"decreasing", []int64{3e6, 2e6, 1e6}, 1e6},
		{"wrapping-delta", []int64{math.MaxInt64, math.MinInt64}, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, tsScale(tc.ts))
		})
	}
}

func TestDoDScaledRoundTrip(t *testing.T) {
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
		{"ms-aligned-jittered", makeMsAligned(makeJittered(120, 1_700_000_000_000, 15_000, 300))},
		{"s-aligned", makeConstantStride(120, 1_700_000_000_000_000_000, 15_000_000_000)},
		{"burst", []int64{0, 1, 2, 1000, 1001, 1002, 1_000_000, 1_000_001}},
		{"large-jumps", []int64{0, 1 << 10, 1 << 20, 1 << 30, 1 << 40, 1 << 50, 1 << 60}},
		{"negative-dod", []int64{0, 100, 200, 150, 100, 50}},
		{"decreasing-ms", []int64{9e6, 5e6, 2e6, 1e6}},
		{"max-int64", []int64{math.MaxInt64 - 5, math.MaxInt64 - 4, math.MaxInt64 - 3}},
		{"min-int64", []int64{math.MinInt64, math.MinInt64 + 1, math.MinInt64 + 2}},
		{"wrapping", []int64{math.MaxInt64, math.MinInt64, math.MaxInt64}},
		{"wrapping-ms", []int64{math.MinInt64 + 500_000, math.MaxInt64 - 499_999, math.MinInt64 + 500_000}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			enc := EncodeTimestampsScaled(nil, tc.ts)

			got, n, err := DecodeTimestampsScaled(nil, enc)
			require.NoError(t, err)
			assert.Equal(t, len(enc), n)

			if len(tc.ts) == 0 {
				assert.Empty(t, got)
			} else {
				assert.Equal(t, tc.ts, got)
			}

			cur, err := NewTsCursor(CodecDoDScaled, enc)
			require.NoError(t, err)
			require.Equal(t, len(tc.ts), cur.Len())

			for i, want := range tc.ts {
				v, err := cur.Next()
				require.NoError(t, err, "row %d", i)
				assert.Equal(t, want, v, "row %d", i)
			}

			_, err = cur.Next()
			assert.True(t, IsEOF(err), "past-end: %v", err)
		})
	}
}

func TestDoDScaledSize(t *testing.T) {
	t.Parallel()

	t.Run("ms-aligned", func(t *testing.T) {
		t.Parallel()

		ts := makeMsAligned(makeJittered(8192, 1_700_000_000_000, 15_000, 300))
		plain, scaled := EncodeTimestamps(nil, ts), EncodeTimestampsScaled(nil, ts)
		assert.Less(t, 2*len(scaled), len(plain), "plain %d, scaled %d", len(plain), len(scaled))
	})
	t.Run("ns-entropic", func(t *testing.T) {
		t.Parallel()

		ts := makeJittered(8192, 1_700_000_000_000_000_000, 15_000_000_000, 300_001)
		plain, scaled := EncodeTimestamps(nil, ts), EncodeTimestampsScaled(nil, ts)
		assert.Len(t, scaled, len(plain)+1, "a unit scale costs exactly its one-byte uvarint")
	})
	t.Run("under-two-rows-is-plain", func(t *testing.T) {
		t.Parallel()

		for _, ts := range [][]int64{nil, {1_700_000_000_000_000_000}} {
			assert.Equal(t, EncodeTimestamps(nil, ts), EncodeTimestampsScaled(nil, ts))
		}
	})
}

func TestDoDScaledRejectsInvalidScale(t *testing.T) {
	t.Parallel()

	w, _ := writeHeader(nil, 3)
	w.WriteVarint(0)
	w.WriteUvarint(7)
	w.WriteUvarint(1)
	writeDoD(w, 0)
	w.PadToByte()
	corrupt := w.Bytes()

	_, _, err := DecodeTimestampsScaled(nil, corrupt)
	require.Error(t, err)

	cur, err := NewTsCursor(CodecDoDScaled, corrupt)
	require.NoError(t, err)

	_, err = cur.Next()
	require.NoError(t, err)

	_, err = cur.Next()
	require.Error(t, err)
}

func TestNewTsCursorRejectsNonTimestampCodec(t *testing.T) {
	t.Parallel()

	cur, err := NewTsCursor(CodecT64, EncodeIntsT64(nil, []int64{1, 2, 3}))
	require.ErrorIs(t, err, errNotTsCodec)
	assert.Nil(t, cur)
}

// TestEncodeTimestampsScaledGolden pins the scaled DoD wire layout. Regenerate with
// `go test ./encoding/chunk -update`.
func TestEncodeTimestampsScaledGolden(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		ts   []int64
	}{
		{"dodscaled_ms", []int64{1_700_000_000_000_000_000, 1_700_000_015_000_000_000, 1_700_000_030_001_000_000, 1_700_000_044_999_000_000}},
		{"dodscaled_ns", []int64{1_700_000_000_000_000_000, 1_700_000_015_000_000_007, 1_700_000_030_000_000_011}},
		{"dodscaled_single", []int64{1_700_000_000_000_000_000}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gold.Bytes(t, EncodeTimestampsScaled(nil, tc.ts), tc.name)
		})
	}
}

func makeMsAligned(ms []int64) []int64 {
	out := make([]int64, len(ms))
	for i, v := range ms {
		out[i] = v * 1e6
	}

	return out
}
