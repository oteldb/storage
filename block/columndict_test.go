package block

import (
	"bytes"
	"fmt"
	"reflect"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
)

// splitBytesForm builds the (entries, ids) split form of cells, deduplicating by value in
// first-occurrence order.
func splitBytesForm(cells [][]byte) (entries [][]byte, ids []int32) {
	index := make(map[string]int32, len(cells))
	entries = make([][]byte, 0, len(cells))
	ids = make([]int32, 0, len(cells))

	for _, v := range cells {
		id, ok := index[string(v)]
		if !ok {
			id = int32(len(entries))
			index[string(v)] = id
			entries = append(entries, v)
		}

		ids = append(ids, id)
	}

	return entries, ids
}

// reverseSplitForm renumbers a split form so its entry table is in reverse order, proving the
// encoders derive their dictionary from row order rather than from the caller's entry order.
func reverseSplitForm(entries [][]byte, ids []int32) ([][]byte, []int32) {
	rev := slices.Clone(entries)
	slices.Reverse(rev)

	out := make([]int32, len(ids))
	for i, id := range ids {
		out[i] = int32(len(entries)-1) - id
	}

	return rev, out
}

// assertBytesFormsMatch builds cells through all three KindBytes input forms and asserts the
// descriptors and serialized objects are identical, then returns the split form's pair.
func assertBytesFormsMatch(tb testing.TB, cells [][]byte, block bool, blockRows int) (ColumnDesc, []byte) {
	tb.Helper()

	comp := compress.NewCompressor(compress.AlgorithmNone, compress.LevelDefault)

	blob := []byte{}
	offsets := make([]int32, 1, len(cells)+1)

	for _, v := range cells {
		blob = append(blob, v...)
		offsets = append(offsets, int32(len(blob)))
	}

	entries, ids := splitBytesForm(cells)
	revEntries, revIDs := reverseSplitForm(entries, ids)

	build := func(c Column) (ColumnDesc, []byte) {
		tb.Helper()

		c.Name, c.Kind, c.Block = "c", KindBytes, block

		desc, obj, err := buildColumn(c, comp, blockRows, defaultCompressBlockBytes)
		require.NoError(tb, err)

		return desc, obj
	}

	sliceDesc, sliceObj := build(Column{Bytes: cells})
	blobDesc, blobObj := build(Column{BytesBlob: blob, BytesOffsets: offsets})
	dictDesc, dictObj := build(Column{BytesDict: entries, BytesIDs: ids})
	revDesc, revObj := build(Column{BytesDict: revEntries, BytesIDs: revIDs})

	assert.Equal(tb, sliceDesc, blobDesc, "blob descriptor")
	assert.Equal(tb, sliceObj, blobObj, "blob object")
	assert.Equal(tb, sliceDesc, dictDesc, "split descriptor")
	assert.Equal(tb, sliceObj, dictObj, "split object")
	assert.Equal(tb, sliceDesc, revDesc, "reversed-entries descriptor")
	assert.Equal(tb, sliceObj, revObj, "reversed-entries object")

	return dictDesc, dictObj
}

// assertBytesDecodes reads a built object back and compares every cell.
func assertBytesDecodes(tb testing.TB, desc ColumnDesc, obj []byte, cells [][]byte) {
	tb.Helper()

	r := newColumnReader(desc, obj, compress.NewCompressor(compress.AlgorithmNone, compress.LevelDefault), len(cells))

	dc, err := r.Bytes()
	require.NoError(tb, err)
	require.Equal(tb, len(cells), dc.Len())

	for i, want := range cells {
		assert.True(tb, bytes.Equal(want, dc.At(i)), "row %d: %q != %q", i, want, dc.At(i))
	}
}

// bytesFormCases are columns chosen to exercise every branch the shared-dictionary build can take:
// the dictionary accepted for all granules, declined for all, and mixed across them.
func bytesFormCases(blockRows int) map[string][][]byte {
	rep := func(rows, distinct int) [][]byte {
		out := make([][]byte, rows)
		for i := range out {
			out[i] = fmt.Appendf(nil, "value-%d", i%distinct)
		}

		return out
	}

	uniq := func(rows int) [][]byte {
		out := make([][]byte, rows)
		for i := range out {
			out[i] = fmt.Appendf(nil, "unique-%08d", i)
		}

		return out
	}

	// Alternate repetitive and near-unique granules so some take modeShared and others modeSelf.
	mixed := make([][]byte, 0, 6*blockRows)
	for g := range 6 {
		if g%2 == 0 {
			mixed = append(mixed, rep(blockRows, 2)...)
			continue
		}

		mixed = append(mixed, uniq(blockRows)...)
	}

	return map[string][][]byte{
		"empty":          {},
		"single":         {[]byte("only")},
		"constant":       rep(3*blockRows, 1),
		"varied":         {[]byte("a"), []byte("bb"), []byte("a"), []byte("ccc")},
		"shared":         rep(4*blockRows, 3),
		"declined":       uniq(3 * blockRows),
		"mixed":          mixed,
		"emptyCells":     {[]byte(""), []byte("x"), []byte(""), []byte("")},
		"partialGranule": rep(2*blockRows+3, 4),
	}
}

// TestColumnBytesDictFormMatchesSlices pins the split (dictionary) column input to the other two
// input forms: the built object and descriptor must be identical, so a merge feeding the writer a
// dictionary it already holds cannot change what lands on disk.
func TestColumnBytesDictFormMatchesSlices(t *testing.T) {
	t.Parallel()

	const blockRows = 8

	for name, cells := range bytesFormCases(blockRows) {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			for _, block := range []bool{false, true} {
				t.Run(fmt.Sprintf("block=%v", block), func(t *testing.T) {
					t.Parallel()

					desc, obj := assertBytesFormsMatch(t, cells, block, blockRows)
					if len(cells) > 0 {
						assertBytesDecodes(t, desc, obj, cells)
					}
				})
			}
		})
	}
}

// TestColumnBytesDictSharedModes checks the cases above actually reach the shared-dictionary
// branches they are named for, so the byte-identity assertions are not all testing one path.
func TestColumnBytesDictSharedModes(t *testing.T) {
	t.Parallel()

	const blockRows = 8

	cases := bytesFormCases(blockRows)

	for name, wantShared := range map[string]bool{"shared": true, "mixed": true, "declined": false} {
		desc, _ := assertBytesFormsMatch(t, cases[name], true, blockRows)
		assert.Equal(t, wantShared, desc.SharedDict, "%s: SharedDict", name)
	}
}

// TestColumnBytesDictOverflowsSharedEntries drives the union of granule dictionaries past the
// 2-byte id ceiling while every granule stays repetitive enough to want in: the shared dictionary
// closes partway through the column and the remaining granules self-encode.
//
// The ceiling is lowered for the test: reaching the real one takes a ~130k-row column built through
// every input form, which is minutes of work under coverage instrumentation for a property that
// does not depend on where the ceiling sits.
//
//nolint:paralleltest // mutates the package-level ceiling.
func TestColumnBytesDictOverflowsSharedEntries(t *testing.T) {
	defer func(v int) { maxSharedEntries = v }(maxSharedEntries)

	maxSharedEntries = 512

	const blockRows = 16

	var (
		perGran  = blockRows / sharedDictMinRepeat
		granules = maxSharedEntries/perGran + 4
	)

	cells := make([][]byte, 0, granules*blockRows)
	for g := range granules {
		for i := range blockRows {
			cells = append(cells, fmt.Appendf(nil, "v-%d-%d", g, i%perGran))
		}
	}

	desc, obj := assertBytesFormsMatch(t, cells, true, blockRows)
	require.True(t, desc.SharedDict)
	assertBytesDecodes(t, desc, obj, cells)
}

// TestColumnBytesDictRejectsRawCodec pins that the split form is dictionary-only: the merge keeps
// raw columns flat, so there is no raw split-form encoder to fall through to.
func TestColumnBytesDictRejectsRawCodec(t *testing.T) {
	t.Parallel()

	comp := compress.NewCompressor(compress.AlgorithmNone, compress.LevelDefault)
	entries, ids := splitBytesForm([][]byte{[]byte("aaaa"), []byte("bbbb"), []byte("aaaa")})

	_, _, err := buildColumn(
		Column{Name: "c", Kind: KindBytes, Codec: chunk.CodecBytesRaw, BytesDict: entries, BytesIDs: ids},
		comp, defaultGranuleSize, defaultCompressBlockBytes,
	)
	require.ErrorContains(t, err, "BytesDict")
}

// TestColumnBytesDictRawBytes pins the split form's reported footprint to the expanded one: it feeds
// the manifest's RawBytes, which sizes parts for merge tiering.
func TestColumnBytesDictRawBytes(t *testing.T) {
	t.Parallel()

	cells := [][]byte{[]byte("aaaa"), []byte("bb"), []byte("aaaa"), []byte("aaaa")}
	entries, ids := splitBytesForm(cells)

	c := Column{Name: "c", Kind: KindBytes, BytesDict: entries, BytesIDs: ids}

	assert.Equal(t, int64(14), c.rawBytes())
	assert.Equal(t, Column{Name: "c", Kind: KindBytes, Bytes: cells}.rawBytes(), c.rawBytes())
	assert.Equal(t, len(cells), c.rows())
}

func FuzzColumnBytesDictForm(f *testing.F) {
	f.Add([]byte("a\x00bb\x00ccc"), []byte{0, 1, 2, 2, 0}, uint8(4))
	f.Add([]byte("same"), []byte{0, 0, 0, 0, 0, 0, 0, 0, 0}, uint8(3))
	f.Add([]byte("x\x00y"), []byte{0, 1, 0, 1, 0, 1, 0, 1, 1, 1, 0}, uint8(2))
	f.Add([]byte("p\x00q\x00r\x00s\x00t\x00u"), []byte{0, 1, 2, 3, 4, 5, 0, 0, 0, 0, 0, 0}, uint8(6))

	f.Fuzz(func(t *testing.T, seed, idSeed []byte, granule uint8) {
		blockRows := int(granule%16) + 1

		var (
			entries [][]byte
			index   = map[string]bool{}
		)

		for part := range bytes.SplitSeq(seed, []byte{0}) {
			if index[string(part)] {
				continue
			}

			index[string(part)] = true

			entries = append(entries, part)
		}

		if len(entries) == 0 {
			t.Skip("no entries")
		}

		ids := make([]int32, len(idSeed))
		cells := make([][]byte, len(idSeed))

		for i, b := range idSeed {
			ids[i] = int32(int(b) % len(entries))
			cells[i] = entries[ids[i]]
		}

		comp := compress.NewCompressor(compress.AlgorithmNone, compress.LevelDefault)

		for _, block := range []bool{false, true} {
			sliceDesc, sliceObj, err := buildColumn(
				Column{Name: "c", Kind: KindBytes, Bytes: cells, Block: block},
				comp, blockRows, defaultCompressBlockBytes,
			)
			if err != nil {
				t.Fatalf("build slice form: %v", err)
			}

			dictDesc, dictObj, err := buildColumn(
				Column{Name: "c", Kind: KindBytes, BytesDict: entries, BytesIDs: ids, Block: block},
				comp, blockRows, defaultCompressBlockBytes,
			)
			if err != nil {
				t.Fatalf("build split form: %v", err)
			}

			if !reflect.DeepEqual(sliceDesc, dictDesc) {
				t.Fatalf("descriptors differ: %+v vs %+v", sliceDesc, dictDesc)
			}

			if !bytes.Equal(sliceObj, dictObj) {
				t.Fatalf("objects differ (block=%v, blockRows=%d)", block, blockRows)
			}
		}
	})
}

// TestColumnBytesDictRejectsBadIDs pins the guards on the split form's two halves. They reach the
// writer from different places in the caller, so a mismatch must name the column or the row rather
// than raise a bare bounds error from inside the encoder.
func TestColumnBytesDictRejectsBadIDs(t *testing.T) {
	t.Parallel()

	comp := compress.NewCompressor(compress.AlgorithmNone, compress.LevelDefault)

	build := func(c Column, blockRows int) error {
		c.Name, c.Kind = "c", KindBytes

		_, _, err := buildColumn(c, comp, blockRows, defaultCompressBlockBytes)

		return err
	}

	// Empty entry table under a live id stream: caught by name, as an error.
	err := build(Column{BytesDict: nil, BytesIDs: []int32{0, 0}}, defaultGranuleSize)
	require.ErrorContains(t, err, `column "c" has 2 BytesIDs but an empty BytesDict`)

	// An id past the table, through the shared-dictionary path (repetitive, block-framed).
	ids := make([]int32, 64)
	ids[40] = 7

	assert.PanicsWithValue(t,
		"block: dictionary id 7 at row 40 is out of range for 2 entries",
		func() {
			_ = build(Column{
				BytesDict: [][]byte{[]byte("aa"), []byte("bb")}, BytesIDs: ids, Block: true,
			}, 16)
		})
}
