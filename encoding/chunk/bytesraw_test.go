package chunk

import (
	"bytes"
	"testing"

	"github.com/go-faster/sdk/gold"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEncodeBytesRawRoundTrip verifies EncodeBytesRaw∘DecodeBytes == identity across the fixed-width
// path, the mixed-width fallback, the all-empty (zero-width) case, and the empty column.
func TestEncodeBytesRawRoundTrip(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		vals  [][]byte
		fixed bool // expected on-disk form
	}{
		{"empty", nil, false},
		{"single", [][]byte{[]byte("abcdefgh")}, true},
		{"fixed-width ids", [][]byte{[]byte("aaaaaaaa"), []byte("bbbbbbbb"), []byte("cccccccc")}, true},
		{"mixed widths", [][]byte{[]byte("a"), []byte("bb"), []byte("ccc")}, false},
		{"all empty (width 0)", [][]byte{{}, {}, {}}, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			enc := EncodeBytesRaw(nil, tc.vals)

			if len(tc.vals) > 0 {
				flag := flagByte(t, enc)
				if tc.fixed {
					assert.Equal(t, flagFixed, flag, "expected fixed-width form")
				} else {
					assert.Equal(t, flagFlat, flag, "expected flat fallback")
				}
			}

			// Gather form.
			got, consumed, err := DecodeBytes(nil, enc)
			require.NoError(t, err)
			assert.Equal(t, len(enc), consumed)
			require.Len(t, got, len(tc.vals))
			for i := range tc.vals {
				assert.Truef(t, bytes.Equal(got[i], tc.vals[i]), "row %d: %q != %q", i, got[i], tc.vals[i])
			}

			// Split (lazy) form must agree.
			col, _, err := DecodeBytesDict(enc)
			require.NoError(t, err)
			require.Equal(t, len(tc.vals), col.Len())
			for i := range tc.vals {
				assert.Truef(t, bytes.Equal(col.At(i), tc.vals[i]), "At(%d): %q != %q", i, col.At(i), tc.vals[i])
			}
		})
	}
}

// TestEncodeBytesRawFixedSmallerThanDict locks in the storage win: for an all-unique fixed-width
// column the raw codec is strictly smaller than the dictionary codec (no dictionary overhead).
func TestEncodeBytesRawFixedSmallerThanDict(t *testing.T) {
	t.Parallel()

	vals := make([][]byte, 1000)
	for i := range vals {
		b := make([]byte, 8)
		b[0], b[1] = byte(i), byte(i>>8)
		vals[i] = b
	}

	raw := EncodeBytesRaw(nil, vals)
	dict := EncodeBytes(nil, vals)
	assert.Less(t, len(raw), len(dict), "raw fixed-width should beat dict for unique ids")
}

// TestEncodeBytesRawGolden pins the raw codec's on-the-wire layout as golden files so an accidental
// format change is caught. Regenerate with `go test ./encoding/chunk -update`.
func TestEncodeBytesRawGolden(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		vals [][]byte
	}{
		// Fixed-width: [uvarint rows][flagFixed][uvarint width][rows×width bytes].
		{"bytesraw_fixed", [][]byte{{0x01, 0x02}, {0x03, 0x04}, {0x05, 0x06}}},
		// Mixed widths fall back to flat: [uvarint rows][flagFlat][per row uvarint len + bytes].
		{"bytesraw_flat", [][]byte{[]byte("a"), []byte("bcd"), []byte("ef")}},
		// All-empty ⇒ fixed-width with width 0.
		{"bytesraw_empty_width", [][]byte{{}, {}}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gold.Bytes(t, EncodeBytesRaw(nil, tc.vals), tc.name)
		})
	}
}

// flagByte returns the format flag byte of an encoded non-empty bytes column: it follows the
// uvarint row count.
func flagByte(t *testing.T, enc []byte) byte {
	t.Helper()

	off := 0
	for off < len(enc) && enc[off] >= 0x80 {
		off++
	}

	require.Less(t, off+1, len(enc), "encoded column too short to hold a flag")

	return enc[off+1]
}
