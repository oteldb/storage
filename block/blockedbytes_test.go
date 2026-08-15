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

// bytesColumn builds a block-framed bytes column, trying the shared dictionary first and falling back
// to per-granule encoding — the framed layout in full.
//
// It does not go through [buildColumn], because buildColumn no longer frames every column it is asked
// to: a near-unique dictionary column is written as a single stream instead, since framing one decodes
// ~2.5× slower and allocates ~2.9× more (see the gate there, and
// [TestBuildColumnSkipsFramingWhenItDoesNotPay] for the policy). The tests in this file are about the
// framed *decode* paths, which every part written before that gate still uses, so they construct the
// framed layout directly rather than asking for it and getting a policy decision.
func bytesColumn(t *testing.T, vals [][]byte, codec chunk.Codec, granule int) *ColumnReader {
	t.Helper()

	c := Column{Name: "c", Kind: KindBytes, Codec: codec, Bytes: vals, Block: true}
	desc := ColumnDesc{
		Name: "c", Kind: KindBytes, Codec: codec,
		Compress: noneComp().Algorithm(), Blocked: true, Framed: true,
	}

	obj, ok, err := trySharedDict(c, codec, noneComp(), granule, defaultCompressBlockBytes)
	require.NoError(t, err)

	if ok {
		desc.SharedDict = true

		return newColumnReader(desc, obj, noneComp(), len(vals))
	}

	obj, err = encodeBlocked(c, codec, 0, noneComp(), granule, defaultCompressBlockBytes)
	require.NoError(t, err)

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

// sharedDesc reports whether a block-framed bytes column chose the shared dictionary.
func sharedDesc(t *testing.T, vals [][]byte, granule int) bool {
	t.Helper()

	desc, _, err := buildColumn(
		Column{Name: "c", Kind: KindBytes, Codec: chunk.CodecDict, Bytes: vals, Block: true},
		noneComp(), granule, defaultCompressBlockBytes,
	)
	require.NoError(t, err)

	return desc.SharedDict
}

// TestSharedDictChosenByRepetition pins the encoder's decision. A column whose values repeat across
// granules must put its dictionary in one place, or every repeat crossing a granule boundary is
// stored again — measured at +74% on a real record-attributes column. A near-unique column must
// decline it, or the dictionary becomes the column with an id array bolted on.
func TestSharedDictChosenByRepetition(t *testing.T) {
	t.Parallel()

	assert.True(t, sharedDesc(t, repeatVals(8192, 20), 1024),
		"repeating values must share one dictionary across granules")
	assert.False(t, sharedDesc(t, uniqueVals(8192), 1024),
		"near-unique values must decline the shared dictionary")
}

// TestSharedDictMixedGranules is the graceful-degradation case: one column holding both shapes, as a
// service that logs clean structured lines and then dumps a stack trace does. The repeating granules
// must use the shared dictionary while the unique ones self-encode, and every row must survive.
func TestSharedDictMixedGranules(t *testing.T) {
	t.Parallel()

	const granule = 1024

	vals := make([][]byte, 0, granule*4)
	vals = append(vals, repeatVals(granule, 5)...)
	vals = append(vals, uniqueVals(granule)...)
	vals = append(vals, repeatVals(granule, 5)...)
	vals = append(vals, uniqueVals(granule)...)

	desc, obj, err := buildColumn(
		Column{Name: "c", Kind: KindBytes, Codec: chunk.CodecDict, Bytes: vals, Block: true},
		noneComp(), granule, defaultCompressBlockBytes,
	)
	require.NoError(t, err)
	require.True(t, desc.SharedDict, "the repeating granules must opt in even with unique neighbors")

	r := newColumnReader(desc, obj, noneComp(), len(vals))

	got, err := r.Bytes()
	require.NoError(t, err)
	assert.Equal(t, vals, gather(t, got))

	// Each granule decodes on its own, whichever mode it chose.
	for _, g := range []int{0, 1, 2, 3} {
		part, err := r.DecodeBlocksBytes([]int{g})
		require.NoError(t, err, "granule %d", g)
		assert.Equal(t, vals[g*granule:(g+1)*granule], gather(t, part), "granule %d", g)
	}
}

// TestSharedDictBlobInput checks the blob+offsets input form encodes identically to the [][]byte
// one — the flush path feeds the blob directly, so a divergence would only show in production.
func TestSharedDictBlobInput(t *testing.T) {
	t.Parallel()

	vals := repeatVals(4096, 12)

	blob := make([]byte, 0, 16*len(vals))
	offsets := make([]int32, 1, len(vals)+1)

	for _, v := range vals {
		blob = append(blob, v...)
		offsets = append(offsets, int32(len(blob)))
	}

	fromSlices, objA, err := buildColumn(
		Column{Name: "c", Kind: KindBytes, Codec: chunk.CodecDict, Bytes: vals, Block: true},
		noneComp(), 1024, defaultCompressBlockBytes,
	)
	require.NoError(t, err)

	fromBlob, objB, err := buildColumn(
		Column{Name: "c", Kind: KindBytes, Codec: chunk.CodecDict, BytesBlob: blob, BytesOffsets: offsets, Block: true},
		noneComp(), 1024, defaultCompressBlockBytes,
	)
	require.NoError(t, err)

	assert.Equal(t, fromSlices.SharedDict, fromBlob.SharedDict)
	assert.Equal(t, objA, objB, "the two input forms must encode byte-identically")
}

// TestBlockedBytesScatterKeepsRowIndices is the property the record fetch path depends on: a pruned
// decode must leave part row indices valid. A fetch resolves its rows through the part's row-range
// index and the marks before any column is read, so a packed result would renumber everything it
// already located.
func TestBlockedBytesScatterKeepsRowIndices(t *testing.T) {
	t.Parallel()

	const granule = 1024

	for _, tt := range []struct {
		name string
		vals [][]byte
	}{
		{"shared dictionary", repeatVals(granule*8, 30)},
		{"self-encoded", uniqueVals(granule * 8)},
		{"mixed", append(repeatVals(granule*4, 6), uniqueVals(granule*4)...)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := bytesColumn(t, tt.vals, chunk.CodecDict, granule)

			for _, blocks := range [][]int{{0}, {3}, {7}, {0, 7}, {2, 3, 4}} {
				got, err := r.DecodeBlocksBytesIntoColumn(blocks)
				require.NoError(t, err, "blocks %v", blocks)
				require.Equal(t, len(tt.vals), got.Len(), "blocks %v: must span the whole column", blocks)

				selected := map[int]bool{}
				for _, b := range blocks {
					selected[b] = true
				}

				// Only the selected rows are specified: an unselected row holds whatever the
				// encoding left there, exactly as the int64 path leaves the destination untouched.
				for i := range tt.vals {
					if selected[i/granule] {
						assert.Equal(t, tt.vals[i], got.At(i),
							"blocks %v: row %d must decode at its own index", blocks, i)
					}
				}
			}
		})
	}
}

// TestBlockedBytesScatterMatchesFullDecode checks the pruned path and the whole-column path agree on
// the rows they share — the decode must not depend on which granules were asked for.
func TestBlockedBytesScatterMatchesFullDecode(t *testing.T) {
	t.Parallel()

	const granule = 512

	vals := append(repeatVals(granule*3, 9), uniqueVals(granule*3)...)
	r := bytesColumn(t, vals, chunk.CodecDict, granule)

	full, err := r.Bytes()
	require.NoError(t, err)

	all := make([]int, (len(vals)+granule-1)/granule)
	for i := range all {
		all[i] = i
	}

	scattered, err := r.DecodeBlocksBytesIntoColumn(all)
	require.NoError(t, err)

	require.Equal(t, full.Len(), scattered.Len())

	for i := range full.Len() {
		assert.Equal(t, full.At(i), scattered.At(i), "row %d", i)
	}
}

// TestBuildColumnSkipsFramingWhenItDoesNotPay pins the write-time policy. Framing is what lets a
// windowed query decode a fraction of a column, and on a low-cardinality column it is free — the
// shared dictionary is written once and each granule carries only ids. On a near-unique column it is
// not: every granule has to carry and rebuild its own dictionary, and a *whole*-column decode (which
// is what a query with no time window does — a trace id, a span attribute) then costs ~2.5x the time
// and ~2.9x the allocations of a single stream.
//
// The shared dictionary declining is exactly that signal, and the writer already computes it, so the
// column is framed only when it will pay. A part therefore mixes framed and unframed columns, which
// the reader has always had to handle for parts written before framing existed.
func TestBuildColumnSkipsFramingWhenItDoesNotPay(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name     string
		vals     [][]byte
		codec    chunk.Codec
		wantFrmd bool
	}{
		{"low cardinality dict frames", repeatVals(50_000, 64), chunk.CodecDict, true},
		{"near unique dict does not", uniqueVals(50_000), chunk.CodecDict, false},
		{"raw always frames", uniqueVals(50_000), chunk.CodecBytesRaw, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			desc, obj, err := buildColumn(
				Column{Name: "c", Kind: KindBytes, Codec: tt.codec, Bytes: tt.vals, Block: true},
				zstdComp(), 8192, defaultCompressBlockBytes,
			)
			require.NoError(t, err)
			assert.Equal(t, tt.wantFrmd, desc.Blocked, "framing decision")
			assert.Equal(t, tt.wantFrmd, desc.Framed, "Framed must track Blocked")

			// Whichever shape it picked, the column must read back identically.
			got, err := newColumnReader(desc, obj, zstdComp(), len(tt.vals)).Bytes()
			require.NoError(t, err)
			assert.Equal(t, tt.vals, gather(t, got))
		})
	}
}
