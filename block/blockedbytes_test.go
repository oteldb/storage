package block

import (
	"bytes"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/encoding/chunk"
)

// bytesColumn builds a block-framed bytes column from vals and returns its reader.
func bytesColumn(t *testing.T, vals [][]byte, codec chunk.Codec, granule int) *ColumnReader {
	t.Helper()

	desc, obj, err := buildColumn(
		Column{Name: "c", Kind: KindBytes, Codec: codec, Bytes: vals, Block: true},
		noneComp(), granule, defaultCompressBlockBytes,
	)
	require.NoError(t, err)
	// A single-valued column is stored as a constant in the manifest and never framed; every other
	// shape must be block-framed.
	require.True(t, desc.Blocked || desc.Const, "the column must be block-framed or constant")

	return newColumnReader(desc, obj, noneComp(), len(vals))
}

func gather(t *testing.T, c *chunk.DictColumn) [][]byte {
	t.Helper()

	out := make([][]byte, c.Len())
	for i := range c.Len() {
		out[i] = c.At(i)
	}

	return out
}

// TestBlockedBytesRoundTrip is the identity every codec must hold, over the three shapes a granule's
// own encoder picks between: a dictionary with 1-byte ids, one with 2-byte ids, and the flat form a
// near-unique granule degrades to.
func TestBlockedBytesRoundTrip(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		vals [][]byte
	}{
		{"empty", nil},
		{"single", [][]byte{[]byte("x")}},
		{"low cardinality", repeatVals(5000, 4)},
		{"one distinct", repeatVals(5000, 1)},
		{"medium cardinality", repeatVals(5000, 300)},
		{"near unique", uniqueVals(5000)},
		{"mixed granules", append(repeatVals(4096, 3), uniqueVals(4096)...)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			for _, codec := range []chunk.Codec{chunk.CodecDict, chunk.CodecBytesRaw} {
				t.Run(codec.String(), func(t *testing.T) {
					t.Parallel()

					if len(tt.vals) == 0 {
						t.Skip("buildColumn has no rows to frame")
					}

					r := bytesColumn(t, tt.vals, codec, 1024)

					got, err := r.Bytes()
					require.NoError(t, err)
					assert.Equal(t, tt.vals, gather(t, got))
				})
			}
		})
	}
}

// TestBlockedBytesDecodeBlocks checks the seek primitive: decoding a subset of granules returns
// exactly those granules' rows, packed, which is what lets a time-pruned query read a fraction of
// the column.
func TestBlockedBytesDecodeBlocks(t *testing.T) {
	t.Parallel()

	const granule = 1024

	vals := repeatVals(granule*8, 50)
	r := bytesColumn(t, vals, chunk.CodecDict, granule)

	for _, blocks := range [][]int{{0}, {7}, {0, 7}, {1, 2, 3}, {0, 1, 2, 3, 4, 5, 6, 7}} {
		got, err := r.DecodeBlocksBytes(blocks)
		require.NoError(t, err)

		var want [][]byte
		for _, b := range blocks {
			want = append(want, vals[b*granule:(b+1)*granule]...)
		}

		assert.Equal(t, want, gather(t, got), "blocks %v", blocks)
	}
}

// TestBlockedBytesKeepsDictionaryWhenItPays is the point of the whole change: a low-cardinality
// column must keep a dictionary after merging, because the query engine memoizes predicates per
// distinct entry. A merge that flattened would be correct but would put a regex back on the per-row
// path.
func TestBlockedBytesKeepsDictionaryWhenItPays(t *testing.T) {
	t.Parallel()

	r := bytesColumn(t, repeatVals(8192, 6), chunk.CodecDict, 1024)

	got, err := r.Bytes()
	require.NoError(t, err)

	assert.NotZero(t, got.IDWidth, "a 6-value column must stay dictionary-encoded through the merge")
	assert.Len(t, got.Entries, 6, "the merged dictionary must dedup across granules, not concatenate them")
}

// TestBlockedBytesDegradesOnHighCardinality is the other half: past a 2-byte id width there is no
// dictionary to keep, and the merge must degrade to the flat form rather than build one it cannot
// address. This is the kernel-message case.
//
// Note where the degradation happens. A *granule* keeps its dictionary up to 65536 distinct values,
// which 8192 rows can never exceed, so a granule of near-unique values still encodes as a dictionary
// whose entries are each used once (costing 2 bytes/row of ids that compress away, since the ids of
// distinct values are a dense ascending run). It is the *merged* column across many such granules
// that overflows, and that is what must fall back.
func TestBlockedBytesDegradesOnHighCardinality(t *testing.T) {
	t.Parallel()

	vals := uniqueVals(70000) // > 65536 distinct once merged
	r := bytesColumn(t, vals, chunk.CodecDict, 1024)

	got, err := r.Bytes()
	require.NoError(t, err)

	assert.Zero(t, got.IDWidth, "a column past the id width must decode flat")
	assert.Equal(t, vals, gather(t, got))
}

// TestBlockedBytesDegradesMidMerge checks the fallback is correct when it happens *partway*: the
// rows already remapped into the shared dictionary must be materialized, not dropped or duplicated.
// This is the case a naive degrade gets wrong, since it has to unwind state it already built.
func TestBlockedBytesDegradesMidMerge(t *testing.T) {
	t.Parallel()

	// Clean granules first, so the merge builds a real dictionary before the unique ones overflow it.
	vals := append(repeatVals(4096, 8), uniqueVals(70000)...)

	r := bytesColumn(t, vals, chunk.CodecDict, 1024)

	got, err := r.Bytes()
	require.NoError(t, err)

	require.Equal(t, len(vals), got.Len())
	assert.Zero(t, got.IDWidth)
	assert.Equal(t, vals, gather(t, got), "rows merged before the overflow must survive it intact")
}

// TestBlockedBytesMixedCardinalityIsPerGranule pins what per-granule encoding buys over the
// per-column decision it replaces: one messy stretch must not cost the clean granules their
// dictionary. Encoded per column, the whole thing would degrade to flat.
func TestBlockedBytesMixedCardinalityIsPerGranule(t *testing.T) {
	t.Parallel()

	const granule = 1024

	clean := repeatVals(granule, 3)
	messy := uniqueVals(granule)

	mixed := make([][]byte, 0, granule*2)
	mixed = append(mixed, clean...)
	mixed = append(mixed, messy...)

	blocked := bytesColumn(t, mixed, chunk.CodecDict, granule)

	// The clean granule alone still decodes with its dictionary intact.
	got, err := blocked.DecodeBlocksBytes([]int{0})
	require.NoError(t, err)
	assert.NotZero(t, got.IDWidth, "the clean granule keeps its dictionary despite the messy neighbor")
	assert.Equal(t, clean, gather(t, got))

	// Encoded as one unblocked stream, the same values share one dictionary decision.
	desc, obj, err := buildColumn(
		Column{Name: "c", Kind: KindBytes, Codec: chunk.CodecDict, Bytes: mixed},
		noneComp(), granule, defaultCompressBlockBytes,
	)
	require.NoError(t, err)
	require.False(t, desc.Blocked)

	whole, err := newColumnReader(desc, obj, noneComp(), len(mixed)).Bytes()
	require.NoError(t, err)
	assert.Equal(t, mixed, gather(t, whole))
}

// TestBlockedBytesRejectsBadBlocks checks the bounds guard: a caller asking for a granule that does
// not exist must get an error, not a panic or silent truncation.
func TestBlockedBytesRejectsBadBlocks(t *testing.T) {
	t.Parallel()

	r := bytesColumn(t, repeatVals(2048, 4), chunk.CodecDict, 1024)

	for _, blocks := range [][]int{{-1}, {2}, {0, 99}} {
		_, err := r.DecodeBlocksBytes(blocks)
		assert.Error(t, err, "blocks %v", blocks)
	}
}

func repeatVals(rows, distinct int) [][]byte {
	out := make([][]byte, rows)
	for i := range rows {
		out[i] = []byte("value-" + strconv.Itoa(i%distinct))
	}

	return out
}

func uniqueVals(rows int) [][]byte {
	out := make([][]byte, rows)
	for i := range rows {
		out[i] = fmt.Appendf(nil, "unique-%d-%x", i, i*2654435761)
	}

	return out
}

// FuzzBlockedBytesRoundTrip fuzzes encode∘decode == identity over arbitrary values and granule
// sizes, with the table tests above as the seed corpus.
func FuzzBlockedBytesRoundTrip(f *testing.F) {
	f.Add(4, 3, 7)
	f.Add(1, 1, 1)
	f.Add(5000, 300, 1024)
	f.Add(300, 300, 16)

	f.Fuzz(func(t *testing.T, rows, distinct, granule int) {
		if rows <= 0 || rows > 20000 || distinct <= 0 || granule <= 0 || granule > 8192 {
			t.Skip()
		}

		vals := make([][]byte, rows)
		for i := range rows {
			// Mix the two shapes so a granule may be dictionary-encoded or flat.
			if i%3 == 0 {
				vals[i] = fmt.Appendf(nil, "u-%d", i)
			} else {
				vals[i] = fmt.Appendf(nil, "v-%d", i%distinct)
			}
		}

		desc, obj, err := buildColumn(
			Column{Name: "c", Kind: KindBytes, Codec: chunk.CodecDict, Bytes: vals, Block: true},
			noneComp(), granule, defaultCompressBlockBytes,
		)
		if err != nil {
			t.Fatal(err)
		}

		got, err := newColumnReader(desc, obj, noneComp(), rows).Bytes()
		if err != nil {
			t.Fatal(err)
		}

		if got.Len() != rows {
			t.Fatalf("decoded %d rows, want %d", got.Len(), rows)
		}

		for i := range rows {
			if !bytes.Equal(got.At(i), vals[i]) {
				t.Fatalf("row %d: got %q, want %q", i, got.At(i), vals[i])
			}
		}
	})
}
