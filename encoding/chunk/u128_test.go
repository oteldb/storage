package chunk

import (
	"bytes"
	"encoding/binary"
	"math/rand/v2"
	"slices"
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
		got, n, err := DecodeU128(nil, EncodeU128(nil, vals), len(vals))
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

	got, _, err := DecodeU128(nil, enc, len(vals))
	require.NoError(t, err)
	assert.Equal(t, vals, got)
}

func TestU128DecodeTruncated(t *testing.T) {
	t.Parallel()

	enc := EncodeU128(nil, []U128{{Lo: 1}, {Lo: 1}, {Lo: 2}})
	for n := range enc {
		_, _, err := DecodeU128(nil, enc[:n], 3)
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

		got, _, err := DecodeU128(nil, EncodeU128(nil, vals), len(vals))
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
		{"negative counts", []U128Run{
			{Value: U128{Lo: 9}, Count: -7},
			{Value: U128{Lo: 7}, Count: 127},
			{Value: U128{Lo: 9}, Count: -1},
			{Value: U128{Lo: 7}, Count: 1},
			{Value: U128{Lo: 8}, Count: -2},
		}},
		{"varint boundaries", []U128Run{
			{Value: U128{Lo: 1}, Count: 127},
			{Value: U128{Lo: 2}, Count: 128},
			{Value: U128{Lo: 3}, Count: 16383},
			{Value: U128{Lo: 4}, Count: 16384},
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
	f.Add(uint64(0), uint64(0), 0)
	f.Add(uint64(127), uint64(128), 512)

	f.Fuzz(func(t *testing.T, hiSeed, loSeed uint64, n int) {
		if n < 0 || n > 512 {
			t.Skip()
		}

		rng := rand.New(rand.NewPCG(hiSeed, loSeed))

		runs := make([]U128Run, n)
		for i := range runs {
			runs[i] = U128Run{
				Value: U128{Hi: rng.Uint64() % 3, Lo: rng.Uint64() % 5},
				Count: rng.IntN(4) - 1,
			}
			if i > 0 && rng.IntN(2) == 0 {
				runs[i].Value = runs[i-1].Value
			}
			if rng.IntN(16) == 0 {
				runs[i].Count = 127 + rng.IntN(2)
			}
		}
		prefix := bytes.Repeat([]byte{byte(hiSeed)}, int(loSeed%8))
		want := append(slices.Clone(prefix), EncodeU128(nil, expandRuns(runs))...)
		dst := make([]byte, len(prefix), len(want)+int(hiSeed%2))
		copy(dst, prefix)
		got := EncodeU128Runs(dst, runs)
		if !bytes.Equal(want, got) || !bytes.Equal(prefix, dst) {
			t.Fatal("run encoding differs from the expanded column or changed its prefix")
		}
	})
}

func TestEncodeU128RunsAppend(t *testing.T) {
	t.Parallel()
	runs := []U128Run{{Value: U128{Lo: 1}, Count: 127}, {Count: -1},
		{Value: U128{Lo: 1}, Count: 1}, {Value: U128{Hi: 1}, Count: 128}}
	prefix := []byte{0xff, 0, 0x81}
	want := append(slices.Clone(prefix), EncodeU128(nil, expandRuns(runs))...)
	for _, tc := range []struct {
		name string
		cap  int
	}{
		{"prefix only", len(prefix)},
		{"short", len(want) - 1},
		{"exact", len(want)},
		{"spare", len(want) + 64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dst := make([]byte, len(prefix), tc.cap)
			copy(dst, prefix)
			got := EncodeU128Runs(dst, runs)
			require.Equal(t, want, got)
			require.Equal(t, prefix, dst)
			if tc.cap >= len(want) {
				require.Same(t, &dst[0], &got[0])
			}
		})
	}
}

func TestEncodeU128RunsLargeCounts(t *testing.T) {
	t.Parallel()
	maxInt := int(^uint(0) >> 1)
	wrapped := maxInt
	wrapped++
	a, b := U128{Hi: 1, Lo: 2}, U128{Hi: 3, Lo: 4}
	for _, tc := range []struct {
		name string
		runs []U128Run
		rows int
		ids  []U128
		n    []uint64
	}{
		{"maximum", []U128Run{{a, maxInt}}, maxInt, []U128{a}, []uint64{uint64(maxInt)}},
		{"adjacent wrap", []U128Run{{a, maxInt}, {a, 1}}, wrapped, []U128{a}, []uint64{uint64(wrapped)}},
		{"separate wrap", []U128Run{{a, maxInt}, {b, 1}}, wrapped, []U128{a, b}, []uint64{uint64(maxInt), 1}},
		{"full wrap", []U128Run{{a, maxInt}, {a, maxInt}, {a, 2}}, 0, []U128{a}, []uint64{0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			want := binary.AppendUvarint(nil, uint64(tc.rows))
			for i, id := range tc.ids {
				want = binary.BigEndian.AppendUint64(want, id.Hi)
				want = binary.BigEndian.AppendUint64(want, id.Lo)
				want = binary.AppendUvarint(want, tc.n[i])
			}
			require.Equal(t, want, EncodeU128Runs(nil, tc.runs))
		})
	}
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
