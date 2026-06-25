package chunk

import (
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestU128RoundTrip(t *testing.T) {
	t.Parallel()

	cases := [][]U128{
		nil,
		{{Hi: 1, Lo: 2}},
		// runs (the metric sort-key shape): one id repeated, then another.
		{{Lo: 5}, {Lo: 5}, {Lo: 5}, {Hi: 1, Lo: 0}, {Hi: 1, Lo: 0}, {Hi: 9, Lo: 9}},
		{{Lo: 0}, {Lo: 1}, {Lo: 2}, {Lo: 3}}, // all distinct (run length 1)
	}
	for _, vals := range cases {
		got, n, err := DecodeU128(nil, EncodeU128(nil, vals))
		require.NoError(t, err)
		assert.Equal(t, vals, orNilU128(got))
		assert.Positive(t, n)
	}
}

func TestU128RunCompression(t *testing.T) {
	t.Parallel()

	// 10000 rows but one id ⇒ one run ⇒ tiny output.
	vals := make([]U128, 10000)
	for i := range vals {
		vals[i] = U128{Hi: 7, Lo: 42}
	}

	enc := EncodeU128(nil, vals)
	assert.Less(t, len(enc), 40, "a single run compresses 10000 rows to a handful of bytes")

	got, _, err := DecodeU128(nil, enc)
	require.NoError(t, err)
	assert.Equal(t, vals, got)
}

func TestU128DecodeTruncated(t *testing.T) {
	t.Parallel()

	enc := EncodeU128(nil, []U128{{Lo: 1}, {Lo: 1}, {Lo: 2}})
	for n := range enc {
		_, _, err := DecodeU128(nil, enc[:n])
		require.Errorf(t, err, "prefix %d should error", n)
	}
}

func FuzzU128RoundTrip(f *testing.F) {
	f.Add(uint64(1), uint64(2), 5)

	f.Fuzz(func(t *testing.T, hiSeed, loSeed uint64, n int) {
		if n < 0 || n > 4096 {
			t.Skip()
		}

		rng := rand.New(rand.NewPCG(hiSeed, loSeed))

		vals := make([]U128, n)
		for i := range vals {
			// Bias toward runs by occasionally repeating.
			if i > 0 && rng.IntN(2) == 0 {
				vals[i] = vals[i-1]
			} else {
				vals[i] = U128{Hi: rng.Uint64() % 4, Lo: rng.Uint64() % 8}
			}
		}

		got, _, err := DecodeU128(nil, EncodeU128(nil, vals))
		require.NoError(t, err)
		assert.Equal(t, orNilU128(vals), orNilU128(got))
	})
}

func orNilU128(s []U128) []U128 {
	if len(s) == 0 {
		return nil
	}

	return s
}
