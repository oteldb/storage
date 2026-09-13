package chunk

import (
	"encoding/binary"
	"math"
	"runtime"
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
		{"t64/nonconstant-huge", t64Header(huge, false), func(s []byte) error { _, _, e := DecodeIntsT64(nil, s, 16); return e }},
		{"t64/constant-huge", t64Header(huge, true), func(s []byte) error { _, _, e := DecodeIntsT64(nil, s, 16); return e }},
		{"t64/overflow", t64Header(math.MaxUint64, true), func(s []byte) error { _, _, e := DecodeIntsT64(nil, s, 16); return e }},
		{"decimal/huge", uvarint(huge), func(s []byte) error { _, _, e := DecodeFloatsDecimal(nil, s); return e }},
		{"decimal/overflow", uvarint(math.MaxUint64), func(s []byte) error { _, _, e := DecodeFloatsDecimal(nil, s); return e }},
		{"u128/huge", uvarint(huge), func(s []byte) error { _, _, e := DecodeU128(nil, s, 16); return e }},
		{"u128/overflow", uvarint(math.MaxUint64), func(s []byte) error { _, _, e := DecodeU128(nil, s, 16); return e }},
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

// TestDecodeRowsFromCaller covers the decoders whose stream cannot bound its row count: a few bytes
// claiming 1<<31 rows must be rejected against the caller's count before anything is sized by it.
// Accepted, the header alone asked for 16 GiB (constant T64) and 32 GiB (U128).
//
//nolint:paralleltest // the allocation check reads process-wide counters a parallel sibling would skew
func TestDecodeRowsFromCaller(t *testing.T) {
	const claimed = 1 << 31

	u128Run := append(uvarint(claimed), make([]byte, 16)...)
	u128Run = binary.AppendUvarint(u128Run, claimed)

	cases := []struct {
		name string
		dec  func(rows int) error
	}{
		{"t64/constant", func(rows int) error { _, _, e := DecodeIntsT64(nil, t64Header(claimed, true), rows); return e }},
		{"t64/nonconstant", func(rows int) error { _, _, e := DecodeIntsT64(nil, t64Header(claimed, false), rows); return e }},
		{"u128", func(rows int) error { _, _, e := DecodeU128(nil, u128Run, rows); return e }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, rows := range []int{0, 3, claimed - 1, claimed + 1, -1} {
				var before, after runtime.MemStats

				runtime.ReadMemStats(&before)
				err := tc.dec(rows)
				runtime.ReadMemStats(&after)

				require.Error(t, err, "rows %d", rows)
				require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(1<<20), "rows %d: rejected before allocating", rows)
			}
		})
	}
}

func TestDecodeRowsFromCallerAccepts(t *testing.T) {
	t.Parallel()

	got, _, err := DecodeIntsT64(nil, EncodeIntsT64(nil, []int64{7, 7, 7}), 3)
	require.NoError(t, err)
	require.Equal(t, []int64{7, 7, 7}, got)

	ids, _, err := DecodeU128(nil, EncodeU128(nil, []U128{{Lo: 1}, {Lo: 1}}), 2)
	require.NoError(t, err)
	require.Equal(t, []U128{{Lo: 1}, {Lo: 1}}, ids)
}
