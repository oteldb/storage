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

// TestEncodeU128RunsMatchesExpanded pins the run-fed encoder against the expanded-column encoder:
// a streaming part writer feeds runs instead of rows, so the two must produce the same bytes.
func TestEncodeU128RunsMatchesExpanded(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		runs []U128Run
	}{
		{"empty", nil},
		{"single", []U128Run{{Value: U128{Hi: 1, Lo: 2}, Count: 1}}},
		{"long run", []U128Run{{Value: U128{Lo: 5}, Count: 1000}}},
		{"many runs", []U128Run{
			{Value: U128{Lo: 5}, Count: 3},
			{Value: U128{Hi: 1}, Count: 2},
			{Value: U128{Hi: 9, Lo: 9}, Count: 1},
		}},
		// Degenerate inputs a streaming writer can hand it: zero-count runs (a series that
		// contributed no surviving sample) and adjacent equal values (a series split across two
		// Append calls) must both collapse exactly as the expanded encoder would.
		{"zero counts", []U128Run{
			{Value: U128{Lo: 1}, Count: 0},
			{Value: U128{Lo: 2}, Count: 4},
			{Value: U128{Lo: 3}, Count: 0},
		}},
		{"adjacent equal", []U128Run{
			{Value: U128{Lo: 7}, Count: 2},
			{Value: U128{Lo: 7}, Count: 3},
			{Value: U128{Lo: 8}, Count: 1},
		}},
		{"zero count between equal", []U128Run{
			{Value: U128{Lo: 7}, Count: 2},
			{Value: U128{Lo: 9}, Count: 0},
			{Value: U128{Lo: 7}, Count: 3},
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, EncodeU128(nil, expandRuns(tc.runs)), EncodeU128Runs(nil, tc.runs))
		})
	}
}

func FuzzEncodeU128Runs(f *testing.F) {
	f.Add(uint64(1), uint64(2), 8)
	f.Add(uint64(7), uint64(11), 64)

	f.Fuzz(func(t *testing.T, hiSeed, loSeed uint64, n int) {
		if n < 0 || n > 512 {
			t.Skip()
		}

		rng := rand.New(rand.NewPCG(hiSeed, loSeed))

		runs := make([]U128Run, n)
		for i := range runs {
			runs[i] = U128Run{
				Value: U128{Hi: rng.Uint64() % 3, Lo: rng.Uint64() % 5},
				Count: rng.IntN(4), // includes 0
			}
		}

		require.Equal(t, EncodeU128(nil, expandRuns(runs)), EncodeU128Runs(nil, runs))
	})
}

func expandRuns(runs []U128Run) []U128 {
	var out []U128
	for _, r := range runs {
		for range r.Count {
			out = append(out, r.Value)
		}
	}

	return out
}
