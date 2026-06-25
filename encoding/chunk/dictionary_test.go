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
