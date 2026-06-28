package chunk

import (
	"errors"
	"math"
	"testing"
	"testing/quick"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTsDecoderRoundTrip verifies the forward cursor decodes the same values as DecodeTimestamps.
func TestTsDecoderRoundTrip(t *testing.T) {
	t.Parallel()

	check := func(ts []int64) bool {
		if len(ts) == 0 {
			return true
		}

		enc := EncodeTimestamps(nil, ts)
		one, _, err := DecodeTimestamps(nil, enc)
		if err != nil {
			t.Logf("one-shot decode: %v", err)
			return false
		}

		d, err := NewTsDecoder(enc)
		if err != nil {
			t.Logf("cursor: %v", err)
			return false
		}

		if d.Len() != len(ts) {
			t.Logf("Len mismatch: %d vs %d", d.Len(), len(ts))
			return false
		}

		for i, want := range one {
			got, err := d.Next()
			if err != nil {
				t.Logf("Next[%d]: %v", i, err)
				return false
			}

			if got != want {
				t.Logf("[%d] cursor %d != one-shot %d", i, got, want)
				return false
			}
		}

		if _, err := d.Next(); !errors.Is(err, errEOF) && !IsEOF(err) {
			t.Logf("past-end did not return EOF: %v", err)
			return false
		}

		return true
	}

	if err := quick.Check(check, &quick.Config{MaxCountScale: 50}); err != nil {
		t.Error(err)
	}

	// Explicit edge cases.
	for _, ts := range [][]int64{{}, {42}, {1, 1, 1}, {100, 200, 200, 201}, {0, 0, 1, 3, 6, 10}} {
		require.True(t, check(ts), "failed for %v", ts)
	}
}

// TestGorillaDecoderRoundTrip verifies the forward cursor decodes the same values as DecodeFloats.
func TestGorillaDecoderRoundTrip(t *testing.T) {
	t.Parallel()

	check := func(vals []float64) bool {
		if len(vals) == 0 {
			return true
		}

		enc := EncodeFloats(nil, vals)
		one, _, err := DecodeFloats(nil, enc)
		if err != nil {
			t.Logf("one-shot decode: %v", err)
			return false
		}

		d, err := NewGorillaDecoder(enc)
		if err != nil {
			t.Logf("cursor: %v", err)
			return false
		}

		for i, want := range one {
			got, err := d.Next()
			if err != nil {
				t.Logf("Next[%d]: %v", i, err)
				return false
			}

			if math.Float64bits(got) != math.Float64bits(want) {
				t.Logf("[%d] cursor %v != one-shot %v", i, got, want)
				return false
			}
		}

		_, err = d.Next()
		return errors.Is(err, errEOF) || IsEOF(err)
	}

	if err := quick.Check(check, &quick.Config{MaxCountScale: 50}); err != nil {
		t.Error(err)
	}

	for _, vals := range [][]float64{{}, {42.0}, {1, 1, 1}, {1.5, 1.5, 9.25}, {0, 0, 1, 2.5, 2.5}} {
		require.True(t, check(vals), "failed for %v", vals)
	}
}

// TestDecimalDecoderRoundTrip verifies the forward cursor decodes the same values as
// DecodeFloatsDecimal, across precision levels.
func TestDecimalDecoderRoundTrip(t *testing.T) {
	t.Parallel()

	check := func(vals []float64, precision uint8) bool {
		if len(vals) == 0 || precision > 52 {
			return true
		}

		enc := EncodeFloatsDecimal(nil, vals, precision)
		one, _, err := DecodeFloatsDecimal(nil, enc)
		if err != nil {
			t.Logf("one-shot decode: %v", err)
			return false
		}

		d, err := NewDecimalDecoder(enc)
		if err != nil {
			t.Logf("cursor: %v", err)
			return false
		}

		for i, want := range one {
			got, err := d.Next()
			if err != nil {
				t.Logf("Next[%d]: %v", i, err)
				return false
			}

			if math.Float64bits(got) != math.Float64bits(want) {
				t.Logf("[%d] cursor %v != one-shot %v", i, got, want)
				return false
			}
		}

		_, err = d.Next()
		return errors.Is(err, errEOF) || IsEOF(err)
	}

	if err := quick.Check(check, &quick.Config{MaxCountScale: 30}); err != nil {
		t.Error(err)
	}

	for _, vals := range [][]float64{{}, {42.0}, {1.1, 1.1, 1.1}, {0, 0.5, 1.25, 1.25, 9.0}} {
		for _, p := range []uint8{0, 16, 52, 64} {
			require.True(t, check(vals, p), "failed for %v precision %d", vals, p)
		}
	}
}

// TestNewFloatDecoderDispatch confirms the factory picks the right cursor for each float codec.
func TestNewFloatDecoderDispatch(t *testing.T) {
	t.Parallel()

	enc := EncodeFloats(nil, []float64{1, 2, 3})
	d, err := NewFloatDecoder(CodecGorilla, enc)
	require.NoError(t, err)
	assert.Equal(t, 3, d.Len())

	v, err := d.Next()
	require.NoError(t, err)
	assert.InDelta(t, 1.0, v, 0)

	_, err = NewFloatDecoder(CodecDoD, enc)
	assert.Error(t, err)
}

var cursorSink int64

// BenchmarkTsDecoderNext measures the forward cursor's per-row decode cost (the streaming merge's
// per-row hot path) against the one-shot decode, sized by the logical column.
func BenchmarkTsDecoderNext(b *testing.B) {
	ts := make([]int64, 0, 4096)
	for i := range 4096 {
		ts = append(ts, int64(i*15))
	}

	enc := EncodeTimestamps(nil, ts)
	b.SetBytes(int64(len(ts)) * 8)
	b.ReportAllocs()

	for range b.N {
		d, err := NewTsDecoder(enc)
		if err != nil {
			b.Fatal(err)
		}

		for {
			v, err := d.Next()
			if err != nil {
				break
			}

			cursorSink ^= v
		}
	}
}
