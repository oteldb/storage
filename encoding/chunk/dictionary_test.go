package chunk

import (
	"bytes"
	"math/rand/v2"
	"testing"
)

// TestDictRoundTrip verifies EncodeBytes∘DecodeBytes == identity.
func TestDictRoundTrip(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		vals [][]byte
	}{
		{"empty", nil},
		{"single", [][]byte{[]byte("hello")}},
		{"duplicates", [][]byte{[]byte("a"), []byte("b"), []byte("a"), []byte("b"), []byte("a"), []byte("b")}},
		{"low-cardinality", makeLowCardBytes(100, 10)},
		{"high-cardinality", makeLowCardBytes(100, 95)},
		{"flat-fallback", makeLowCardBytes(100, 80)}, // < 256, still dict
		{"unique", [][]byte{[]byte("alpha"), []byte("beta"), []byte("gamma"), []byte("delta"), []byte("epsilon")}},
		{"empty-strings", [][]byte{nil, nil, nil, nil}},
		{"long-strings", [][]byte{[]byte("a-very-long-string-value-here"), []byte("a-very-long-string-value-here"), []byte("different")}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			enc := EncodeBytes(nil, tc.vals)

			got, _, err := DecodeBytes(nil, enc)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}

			if len(got) != len(tc.vals) {
				t.Fatalf("len = %d, want %d", len(got), len(tc.vals))
			}

			for i := range tc.vals {
				if !bytes.Equal(got[i], tc.vals[i]) {
					t.Fatalf("vals[%d] = %q, want %q", i, got[i], tc.vals[i])
				}
			}
		})
	}
}

// TestDictLargeCardinality verifies the 2-byte id path (>256 distinct).
func TestDictLargeCardinality(t *testing.T) {
	t.Parallel()

	vals := make([][]byte, 500)
	for i := range vals {
		vals[i] = []byte("val-" + itoa(i))
	}

	enc := EncodeBytes(nil, vals)

	got, _, err := DecodeBytes(nil, enc)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	if len(got) != 500 {
		t.Fatalf("len = %d, want 500", len(got))
	}

	for i := range vals {
		if !bytes.Equal(got[i], vals[i]) {
			t.Fatalf("vals[%d] = %q, want %q", i, got[i], vals[i])
		}
	}
}

// TestDictFlatFallback verifies the flat fallback (>65536 distinct).
func TestDictFlatFallback(t *testing.T) {
	t.Parallel()

	vals := make([][]byte, 70000)
	for i := range vals {
		vals[i] = []byte("v" + itoa(i))
	}

	enc := EncodeBytes(nil, vals)

	got, _, err := DecodeBytes(nil, enc)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	if len(got) != 70000 {
		t.Fatalf("len = %d, want 70000", len(got))
	}
	// Check a sample (checking all 70k is slow in a test).
	for _, i := range []int{0, 1, 49999, 69999} {
		if !bytes.Equal(got[i], vals[i]) {
			t.Fatalf("vals[%d] = %q, want %q", i, got[i], vals[i])
		}
	}
}

// TestDictCompressionRatio verifies low-cardinality strings achieve ~1 byte/row.
func TestDictCompressionRatio(t *testing.T) {
	t.Parallel()

	vals := makeLowCardBytes(1000, 5) // 5 distinct strings
	enc := EncodeBytes(nil, vals)
	// 1000 rows × 1 byte id + dictionary (5 short strings + lengths) + header.
	// Should be well under 1100 bytes.
	maxBytes := 1200
	if len(enc) > maxBytes {
		t.Fatalf("compressed size = %d bytes for 1000 rows × 5 distinct, want ≤ %d", len(enc), maxBytes)
	}
}

// TestDictEncoderMatchesEncodeBytes verifies DictEncoder.Encode is byte-identical to
// EncodeBytes across inputs and across reuse of a single encoder.
func TestDictEncoderMatchesEncodeBytes(t *testing.T) {
	t.Parallel()

	cases := [][][]byte{
		nil,
		{[]byte("hello")},
		{[]byte("a"), []byte("b"), []byte("a"), []byte("b")},
		makeLowCardBytes(100, 10),
		makeLowCardBytes(500, 400), // >256 distinct: 2-byte id path
		{nil, nil, nil},
	}

	enc := NewDictEncoder()
	defer enc.Release()

	// Run twice over the same encoder to exercise warm-state reuse.
	for pass := range 2 {
		for i, vals := range cases {
			want := EncodeBytes(nil, vals)
			got := enc.Encode(nil, vals)

			if !bytes.Equal(got, want) {
				t.Fatalf("pass %d case %d: DictEncoder output != EncodeBytes\n got=%x\nwant=%x", pass, i, got, want)
			}

			// And the result must still decode back to the input.
			dec, _, err := DecodeBytes(nil, got)
			if err != nil {
				t.Fatalf("pass %d case %d: Decode: %v", pass, i, err)
			}

			if len(dec) != len(vals) {
				t.Fatalf("pass %d case %d: len = %d, want %d", pass, i, len(dec), len(vals))
			}

			for j := range vals {
				if !bytes.Equal(dec[j], vals[j]) {
					t.Fatalf("pass %d case %d: vals[%d] = %q, want %q", pass, i, j, dec[j], vals[j])
				}
			}
		}
	}
}

// TestDictEncoderReset verifies Reset drops references to a prior batch's input.
func TestDictEncoderReset(t *testing.T) {
	t.Parallel()

	enc := NewDictEncoder()
	defer enc.Release()

	_ = enc.Encode(nil, [][]byte{[]byte("a"), []byte("b")})
	enc.Reset()

	for _, e := range enc.entries {
		if e != nil {
			t.Fatalf("Reset did not clear entries: %q", e)
		}
	}

	// Encoder is still usable after Reset.
	got := enc.Encode(nil, [][]byte{[]byte("x"), []byte("x")})
	want := EncodeBytes(nil, [][]byte{[]byte("x"), []byte("x")})

	if !bytes.Equal(got, want) {
		t.Fatalf("after Reset: got=%x want=%x", got, want)
	}
}

// TestDictColumnRoundTrip verifies DecodeBytesDict's split form returns the same values
// as the input (and as DecodeBytes) for the dict, 2-byte-id, flat, and empty paths.
func TestDictColumnRoundTrip(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		vals      [][]byte
		wantWidth int
	}{
		{"empty", nil, 0},
		{"low-card-1byte", makeLowCardBytes(100, 10), 1},
		{"high-card-2byte", makeDistinctBytes(500), 2},
		{"flat-fallback", makeDistinctBytes(70000), 0},
		{"empty-strings", [][]byte{nil, nil, nil}, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			enc := EncodeBytes(nil, tc.vals)

			col, consumed, err := DecodeBytesDict(enc)
			if err != nil {
				t.Fatalf("DecodeBytesDict: %v", err)
			}

			if consumed != len(enc) {
				t.Fatalf("consumed = %d, want %d", consumed, len(enc))
			}

			if col.Len() != len(tc.vals) {
				t.Fatalf("Len = %d, want %d", col.Len(), len(tc.vals))
			}

			if col.IDWidth != tc.wantWidth {
				t.Fatalf("IDWidth = %d, want %d", col.IDWidth, tc.wantWidth)
			}

			for i := range tc.vals {
				if !bytes.Equal(col.At(i), tc.vals[i]) {
					t.Fatalf("At(%d) = %q, want %q", i, col.At(i), tc.vals[i])
				}
			}
		})
	}
}

// TestDictColumnDecodeReuse verifies a single DictColumn can be reused across decodes.
func TestDictColumnDecodeReuse(t *testing.T) {
	t.Parallel()

	a := EncodeBytes(nil, makeLowCardBytes(50, 5))
	b := EncodeBytes(nil, makeDistinctBytes(300))

	var col DictColumn
	for _, enc := range [][]byte{a, b, a} {
		if _, err := col.DecodeBytes(enc); err != nil {
			t.Fatalf("DecodeBytes: %v", err)
		}

		ref, _, err := DecodeBytes(nil, enc)
		if err != nil {
			t.Fatalf("DecodeBytes ref: %v", err)
		}

		if col.Len() != len(ref) {
			t.Fatalf("Len = %d, want %d", col.Len(), len(ref))
		}

		for i := range ref {
			if !bytes.Equal(col.At(i), ref[i]) {
				t.Fatalf("At(%d) = %q, want %q", i, col.At(i), ref[i])
			}
		}
	}
}

// makeDistinctBytes returns n distinct values ("val-0".."val-(n-1)").
func makeDistinctBytes(n int) [][]byte {
	vals := make([][]byte, n)
	for i := range vals {
		vals[i] = []byte("val-" + itoa(i))
	}

	return vals
}

func makeLowCardBytes(n, cardinality int) [][]byte {
	if cardinality > n {
		cardinality = n
	}

	templates := make([][]byte, cardinality)
	for i := range cardinality {
		templates[i] = []byte("label-" + itoa(i))
	}

	vals := make([][]byte, n)
	for i := range n {
		vals[i] = templates[i%cardinality]
	}

	return vals
}

// itoa is a minimal int→string without strconv to keep the test import-light.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}

	neg := n < 0
	if neg {
		n = -n
	}

	var buf [20]byte

	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}

	if neg {
		i--
		buf[i] = '-'
	}

	return string(buf[i:])
}

// Ensure rand is used (for potential future randomized tests).
var _ = rand.Int
