package recordengine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/encoding/chunk"
)

func dictTestSchema() *Schema {
	return NewSchema(Column{Name: "body", Kind: KindBytes, Codec: chunk.CodecDict})
}

func armedSplitCols(t *testing.T, entries ...[]byte) (*recordCols, *splitCol) {
	t.Helper()

	s := dictTestSchema()
	c := newRecordCols(s, 0, fullSel(s))
	c.armSplit([]*mergeDict{{entries: entries}})
	c.prepare(s, 0, fullSel(s))

	sc := c.splitAt(0)
	require.NotNil(t, sc)

	return c, sc
}

// TestSplitColSizeAccountingIsExpanded pins the trap the split form sets: [recordCols.byteSize] and
// [recordCols.rowBytes] drive the merge's output-part seal and its working-set bound, both
// denominated in decoded bytes, so a split column must report the bytes its rows expand to and never
// the id array it actually holds.
func TestSplitColSizeAccountingIsExpanded(t *testing.T) {
	t.Parallel()

	c, sc := armedSplitCols(t, []byte("aaaa"), []byte("bbbbbbbbbb"))

	for _, id := range []int32{0, 1, 1} {
		c.ts = append(c.ts, int64(len(c.ts)))
		sc.append(id)
	}

	const expanded = 4 + 10 + 10

	assert.Equal(t, int64(expanded), sc.bytes)
	assert.Equal(t, int64(8*3+expanded), c.byteSize())
	assert.Equal(t, int64(8+4), c.rowBytes(0))
	assert.Equal(t, int64(8+10), c.rowBytes(1))
}

// TestSplitColAccountingSurvivesReordering checks the running total against the operations a merge
// performs on an accumulated column: the ts permutation, the bulk range append into the output
// buffer, and the row trim.
func TestSplitColAccountingSurvivesReordering(t *testing.T) {
	t.Parallel()

	c, sc := armedSplitCols(t, []byte("a"), []byte("bbb"), []byte(""))
	c.ts = append(c.ts, 1, 2, 3, 4)
	sc.appendIDs([]int32{0, 1, 2, 1})
	assert.Equal(t, int64(1+3+0+3), sc.bytes)

	sc.permute([]int{3, 2, 1, 0})
	assert.Equal(t, []int32{1, 2, 1, 0}, sc.ids)
	assert.Equal(t, int64(1+3+0+3), sc.bytes, "a permutation cannot change the expanded total")

	c.keep(1, 3)
	assert.Equal(t, []int32{2, 1}, sc.ids)
	assert.Equal(t, int64(3), sc.bytes)

	dst, dsc := armedSplitCols(t, sc.dict.entries...)
	dst.splitBytes[0].dict = sc.dict
	dst.appendRange(c, 0, 2)
	assert.Equal(t, []int32{2, 1}, dsc.ids)
	assert.Equal(t, int64(3), dsc.bytes)
}

// TestSplitColEnsureKeepsBacking checks that re-arming a split column for the next stream resets its
// rows and its running total while keeping the id array — the merge re-arms one accumulator per
// stream, tens of thousands of times.
func TestSplitColEnsureKeepsBacking(t *testing.T) {
	t.Parallel()

	_, sc := armedSplitCols(t, []byte("aaaa"))
	sc.appendIDs([]int32{0, 0, 0, 0})

	backing := &sc.ids[:1][0]

	sc.ensure(2)
	assert.Empty(t, sc.ids)
	assert.Zero(t, sc.bytes)
	assert.Same(t, backing, &sc.ids[:1][0], "the id array is reused")
}

// TestBuildMergeDictUnionsAndRemaps checks the union is deduplicated by value and that each source
// part's entry ids map into it — the one thing between a source row and the value it must still hold
// after the merge.
func TestBuildMergeDictUnionsAndRemaps(t *testing.T) {
	t.Parallel()

	col := Column{Name: "body", Kind: KindBytes, Codec: chunk.CodecDict}
	decoded := []*decodedPart{
		{bytes: []mergeByteCol{{dict: &chunk.DictColumn{
			Entries: [][]byte{[]byte("a"), []byte("b")}, IDs: []byte{0, 1}, IDWidth: 1,
		}}}},
		{bytes: []mergeByteCol{{dict: &chunk.DictColumn{
			Entries: [][]byte{[]byte("c"), []byte("a")}, IDs: []byte{1, 0}, IDWidth: 1,
		}}}},
	}

	m := buildMergeDict(col, decoded, 0)
	require.NotNil(t, m)
	assert.Equal(t, [][]byte{[]byte("a"), []byte("b"), []byte("c")}, m.entries)
	assert.Equal(t, [][]int32{{0, 1}, {2, 0}}, m.remap)

	// Every row still resolves to its own value through the union.
	for p, d := range decoded {
		dc := d.bytes[0].dict
		for i := range dc.Len() {
			assert.Equal(t, dc.At(i), m.entries[m.remap[p][d.bytes[0].entryID(i)]])
		}
	}
}

// TestBuildMergeDictFallsBackPerColumn pins the per-column fallback rule: a single flat source, or a
// codec the split form cannot be encoded under, keeps that column on the flat path for the whole
// merge while its neighbors still take the split one.
func TestBuildMergeDictFallsBackPerColumn(t *testing.T) {
	t.Parallel()

	dict := mergeByteCol{dict: &chunk.DictColumn{
		Entries: [][]byte{[]byte("a")}, IDs: []byte{0}, IDWidth: 1,
	}}
	flat := newMergeByteCol(&chunk.DictColumn{Entries: [][]byte{[]byte("a")}})

	schema := NewSchema(
		Column{Name: "dict", Kind: KindBytes, Codec: chunk.CodecDict},
		Column{Name: "raw", Kind: KindBytes, Codec: chunk.CodecBytesRaw},
		Column{Name: "mixed", Kind: KindBytes, Codec: chunk.CodecDict},
	)
	decoded := []*decodedPart{
		{bytes: []mergeByteCol{dict, dict, dict}},
		{bytes: []mergeByteCol{dict, dict, flat}},
	}

	got := buildMergeDicts(schema, decoded)
	require.Len(t, got, 3)
	assert.NotNil(t, got[0])
	assert.Nil(t, got[1], "a raw column cannot be handed over in split form")
	assert.Nil(t, got[2], "one flat source drops the whole column to the flat path")
}
