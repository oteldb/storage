package chunk

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

// uvarint encodes x as a uvarint (the row-count header every column stream starts with).
func uvarint(x uint64) []byte { return binary.AppendUvarint(nil, x) }

// t64Header builds a row-count header plus a 16-byte T64 min/max block. equal selects a constant
// column (min==max ⇒ numBits 0, no per-row payload); otherwise the column is non-constant.
func t64Header(rows uint64, equal bool) []byte {
	b := uvarint(rows)
	b = append(b, make([]byte, 8)...) // min = 0

	maxv := make([]byte, 8)
	if !equal {
		binary.LittleEndian.PutUint64(maxv, math.MaxUint64) // min≠max ⇒ numBits>0
	}

	return append(b, maxv...)
}

// TestDecodeRejectsCorruptRowCount verifies every column decoder rejects a corrupt header with an
// implausible row count (one larger than the stream could encode, and one that overflows int) by
// returning an error rather than panicking on a giant pre-allocation. A panic fails the test.
func TestDecodeRejectsCorruptRowCount(t *testing.T) {
	t.Parallel()

	const huge = uint64(1) << 40 // far beyond any bound; would be ~TBs if allocated

	cases := []struct {
		name string
		src  []byte
		dec  func([]byte) error
	}{
		{"timestamps/huge", uvarint(huge), func(s []byte) error { _, _, e := DecodeTimestamps(nil, s); return e }},
		{"timestamps/overflow", uvarint(math.MaxUint64), func(s []byte) error { _, _, e := DecodeTimestamps(nil, s); return e }},
		{"floats/huge", uvarint(huge), func(s []byte) error { _, _, e := DecodeFloats(nil, s); return e }},
		{"floats/overflow", uvarint(math.MaxUint64), func(s []byte) error { _, _, e := DecodeFloats(nil, s); return e }},
		{"t64/nonconstant-huge", t64Header(huge, false), func(s []byte) error { _, _, e := DecodeIntsT64(nil, s); return e }},
		{"t64/constant-huge", t64Header(huge, true), func(s []byte) error { _, _, e := DecodeIntsT64(nil, s); return e }},
		{"t64/overflow", t64Header(math.MaxUint64, true), func(s []byte) error { _, _, e := DecodeIntsT64(nil, s); return e }},
		{"u128/huge", uvarint(huge), func(s []byte) error { _, _, e := DecodeU128(nil, s); return e }},
		{"u128/overflow", uvarint(math.MaxUint64), func(s []byte) error { _, _, e := DecodeU128(nil, s); return e }},
		{"bytes/huge", uvarint(huge), func(s []byte) error { _, _, e := DecodeBytes(nil, s); return e }},
		{"bytes/overflow", uvarint(math.MaxUint64), func(s []byte) error { _, _, e := DecodeBytes(nil, s); return e }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Error(t, tc.dec(tc.src), "corrupt row count must error, not panic")
		})
	}
}
