package recordengine

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/block"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
	"github.com/oteldb/storage/signal"
)

// TestMergeDictUnionsByValueAndOwnsEntries: the union deduplicates by value across sources, keeps
// first-seen order, and copies what it keeps — a streamed granule's entries alias a buffer the next
// granule overwrites.
func TestMergeDictUnionsByValueAndOwnsEntries(t *testing.T) {
	t.Parallel()

	d := newMergeDict()
	defer d.release()

	first := [][]byte{[]byte("a"), []byte("b")}
	assert.Equal(t, []int32{0, 1}, d.remap(nil, first, false))
	assert.Equal(t, []int32{2, 0}, d.remap(nil, [][]byte{[]byte("c"), []byte("a")}, false))

	first[0][0] = 'z'
	assert.Equal(t, [][]byte{[]byte("a"), []byte("b"), []byte("c")}, d.entries, "the union holds its own copies")

	stable := []byte("s")
	assert.Equal(t, []int32{3, 1}, d.remap(nil, [][]byte{stable, []byte("b")}, true))
	assert.Same(t, &stable[0], &d.entries[3][0], "a stable entry is kept as is")

	big := bytes.Repeat([]byte("x"), mergeDictSlabBytes)
	assert.Equal(t, int32(4), d.id(big))
	assert.Equal(t, int32(4), d.id(bytes.Clone(big)))
	assert.Equal(t, big, d.entries[4])

	assert.Equal(t, d.id(nil), d.id([]byte{}), "an empty value is one entry however it is spelled")
}

// newTestCarry arms a carry with every byte column of schema on the split path.
func newTestCarry(t *testing.T, schema *Schema, sources int) (*mergeCarry, *recordCols, *recordCols) {
	t.Helper()

	acc := newRecordCols(schema, 0, fullSel(schema))
	buf := newRecordCols(schema, 0, fullSel(schema))
	m := &mergeCarry{
		dicts: make([]*mergeDict, schema.numBytes()), lazy: make([]int, schema.numBytes()),
		acc: acc, buf: buf, maxEntries: sources * mergeUnionEntriesPerSource,
	}

	for k := range m.dicts {
		m.dicts[k] = newMergeDict()
	}

	t.Cleanup(m.release)

	acc.armSplit(m.dicts)
	buf.armSplit(m.dicts)
	acc.prepare(schema, 0, fullSel(schema))
	buf.prepare(schema, 4, fullSel(schema))

	return m, acc, buf
}

// TestMergeCarryDecidesByCodec: a column takes the split carry exactly when its codec accepts the
// split form — the default and the dictionary codec — and every column is flat with the carry off.
//
//nolint:paralleltest // flips the package-level split seam
func TestMergeCarryDecidesByCodec(t *testing.T) {
	schema := NewSchema(
		Column{Name: "dict", Kind: KindBytes, Codec: chunk.CodecDict},
		Column{Name: "raw", Kind: KindBytes, Codec: chunk.CodecBytesRaw},
		Column{Name: "default", Kind: KindBytes},
	)

	decide := func() []bool {
		acc := newRecordCols(schema, 0, fullSel(schema))
		buf := newRecordCols(schema, 0, fullSel(schema))
		m := newMergeCarry(schema, 2, acc, buf)
		t.Cleanup(m.release)

		assert.Equal(t, 2<<16, m.maxEntries)

		out := make([]bool, len(m.dicts))
		for k := range out {
			out[k] = m.dicts[k] != nil
			assert.Equal(t, out[k], acc.splitAt(k) != nil, "accumulator column %d", k)
			assert.Equal(t, out[k], buf.splitAt(k) != nil, "buffer column %d", k)
		}

		return out
	}

	assert.Equal(t, []bool{true, false, true}, decide())

	defer SetMergeSplitDict(false)()

	assert.Equal(t, []bool{false, false, false}, decide())
}

// TestMergeCarrySettleDropsFinishedIndexes: a union every source resolved into as it opened takes no
// more entries, so its lookup index is handed back before the stream sweep; one a granule-read source
// still feeds keeps it.
func TestMergeCarrySettleDropsFinishedIndexes(t *testing.T) {
	t.Parallel()

	schema := NewSchema(
		Column{Name: "whole", Kind: KindBytes, Codec: chunk.CodecDict},
		Column{Name: "granules", Kind: KindBytes, Codec: chunk.CodecDict},
	)
	m, _, _ := newTestCarry(t, schema, 1)
	m.lazy[1] = 1

	m.dicts[0].id([]byte("a"))
	m.settle(0)
	m.settle(1)

	assert.Nil(t, m.dicts[0].index)
	assert.Equal(t, [][]byte{[]byte("a")}, m.dicts[0].entries)
	assert.Panics(t, func() { m.dicts[0].id([]byte("b")) }, "a settled union takes no entry")

	m.dicts[1].id([]byte("b"))
	assert.NotNil(t, m.dicts[1].index)
}

// TestMergeCarryFlattenExpandsHeldIDs: falling a column back mid-merge expands the ids both
// accumulators hold into cells, and the rows appended after it land as cells behind them.
func TestMergeCarryFlattenExpandsHeldIDs(t *testing.T) {
	t.Parallel()

	schema := dictTestSchema()
	m, acc, buf := newTestCarry(t, schema, 1)

	a, b := m.dicts[0].id([]byte("aa")), m.dicts[0].id([]byte("b"))
	buf.ts = append(buf.ts, 1, 2)
	buf.splitAt(0).append(a)
	buf.splitAt(0).append(b)
	acc.ts = append(acc.ts, 3)
	acc.splitAt(0).append(b)

	m.flatten(0)
	m.flatten(0)

	assert.Nil(t, m.dicts[0])
	assert.Nil(t, acc.splitAt(0))
	assert.Nil(t, buf.splitAt(0))
	assert.Equal(t, int64(2*8+3), buf.byteSize(), "the expanded size is unchanged by the move")

	buf.appendRange(acc, 0, 1)

	got := make([]string, 0, buf.len())
	for i := range buf.len() {
		got = append(got, string(buf.bytes[0].at(i)))
	}

	assert.Equal(t, []string{"aa", "b", "b"}, got)
}

// TestMergeCarryFlattensPastBound: a union may grow to its bound and no further.
func TestMergeCarryFlattensPastBound(t *testing.T) {
	t.Parallel()

	m, _, _ := newTestCarry(t, dictTestSchema(), 1)
	m.maxEntries = 2

	m.dicts[0].id([]byte("x"))
	m.dicts[0].id([]byte("y"))
	m.grew(0)
	require.NotNil(t, m.dicts[0])

	m.dicts[0].id([]byte("z"))
	m.grew(0)
	assert.Nil(t, m.dicts[0])
}

func TestForwardReadable(t *testing.T) {
	t.Parallel()

	rng := func(id uint64, start, end int) streamRange {
		return streamRange{id: signal.SeriesID{Lo: id}, rowRange: rowRange{start: start, end: end}}
	}

	for _, tt := range []struct {
		name   string
		ranges []streamRange
		want   bool
	}{
		{"empty", nil, true},
		{"contiguous", []streamRange{rng(1, 0, 3), rng(2, 3, 5), rng(3, 5, 9)}, true},
		{"gap", []streamRange{rng(1, 0, 3), rng(2, 4, 5)}, true},
		{"backwards", []streamRange{rng(1, 3, 5), rng(2, 0, 3)}, false},
		{"repeated stream", []streamRange{rng(1, 0, 1), rng(1, 2, 3), rng(2, 1, 2)}, false},
		{"inverted", []streamRange{rng(1, 2, 1)}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, forwardReadable(tt.ranges))
		})
	}
}

func cellsOf(c *recordCols, k int) []string {
	out := make([]string, 0, c.len())

	cells := c.cellsAt(k)
	for i := range c.len() {
		out = append(out, string(cells.at(i)))
	}

	return out
}

// TestByteCursorFlatGranuleFlattens: a granule with no dictionary — the >65536-distinct fallback —
// drops its column off the split carry before a row of it moves, as a flat source always has.
func TestByteCursorFlatGranuleFlattens(t *testing.T) {
	t.Parallel()

	m, acc, _ := newTestCarry(t, dictTestSchema(), 1)
	c := byteCursor{col: mergeByteCol{dict: &chunk.DictColumn{
		Entries: [][]byte{[]byte("x"), []byte("yy"), []byte("zzz")},
	}}, hi: 3}

	require.NoError(t, c.appendRange(acc, m, 0, 3, []bool{true, false, true}))
	acc.ts = append(acc.ts, 1, 3)

	assert.Nil(t, m.dicts[0])
	assert.Equal(t, []string{"x", "zzz"}, cellsOf(acc, 0))
}

// TestByteCursorRemapsSharedDictionaryOnce: granules on the column's shared dictionary resolve it
// into the union once, and a constant column is one entry for every row.
func TestByteCursorRemapsSharedDictionaryOnce(t *testing.T) {
	t.Parallel()

	m, acc, _ := newTestCarry(t, dictTestSchema(), 1)
	m.dicts[0].id([]byte("pre"))

	shared := [][]byte{[]byte("s0"), []byte("s1")}
	c := byteCursor{shared: shared, col: mergeByteCol{dict: &chunk.DictColumn{
		Entries: shared, IDs: []byte{1, 0, 1}, IDWidth: 1,
	}}, hi: 3}

	require.NoError(t, c.appendRange(acc, m, 0, 3, nil))
	assert.Equal(t, []int32{1, 2}, c.sharedRemap)

	c.remapped = false
	require.NoError(t, c.appendRange(acc, m, 1, 2, nil))

	k := byteCursor{constant: true, value: []byte("k"), hi: 5}
	require.NoError(t, k.appendRange(acc, m, 2, 5, []bool{true, false, true}))

	acc.ts = make([]int64, acc.splitAt(0).rows())
	assert.Equal(t, []string{"s1", "s0", "s1", "s0", "k", "k"}, cellsOf(acc, 0))
	assert.Len(t, m.dicts[0].entries, 4)

	err := (&byteCursor{hi: 3, col: c.col}).load(3)
	require.ErrorIs(t, err, block.ErrCorrupt, "a row past a whole-read column")
}

func TestIntCursorConstant(t *testing.T) {
	t.Parallel()

	c := intCursor{constant: true, value: 7, hi: 4}

	got, err := c.appendRange(nil, 1, 4, []bool{true, false, true})
	require.NoError(t, err)
	assert.Equal(t, []int64{7, 7}, got)

	_, err = c.appendRange(nil, 3, 5, nil)
	require.ErrorIs(t, err, block.ErrCorrupt)
}

// TestMergeShapeScalesToCap: the output buffer is sized from the manifests — rows, and per flat
// column its objects' bytes — and scaled to one output part when the sources outgrow the seal.
func TestMergeShapeScalesToCap(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	schema := NewSchema(
		Column{Name: "sev", Kind: KindInt64, Codec: chunk.CodecT64},
		Column{Name: "body", Kind: KindBytes, Codec: chunk.CodecDict},
		Column{Name: "id", Kind: KindBytes, Codec: chunk.CodecBytesRaw},
	)
	e := New(Config{Schema: schema, Backend: backend.Memory(), Prefix: "t/shape"})

	const rows = 4000

	series := signal.Series{Resource: signal.Resource{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte("svc"))},
	)}}

	b := &Batch{
		Stream: series.Hash(), Identity: func() signal.Series { return series },
		Ints: [][]int64{nil}, Bytes: [][][]byte{nil, nil},
	}

	for i := range rows {
		b.Ts = append(b.Ts, int64(i))
		b.Ints[0] = append(b.Ints[0], int64(i%3))
		b.Bytes[0] = append(b.Bytes[0], []byte("body"))
		b.Bytes[1] = append(b.Bytes[1], fmt.Appendf(nil, "%016x", i))
	}

	_, err := e.AppendBatch(b, AppendLimits{})
	require.NoError(t, err)
	require.NoError(t, e.Flush(ctx))
	require.Len(t, e.parts, 1)

	desc, ok := e.parts[0].reader.ColumnDescByName("id")
	require.True(t, ok)
	require.Positive(t, desc.Bytes)

	body, ok := e.parts[0].reader.ColumnDescByName("body")
	require.True(t, ok)

	gotRows, blob := mergeShape(schema, e.parts, 0)
	assert.Equal(t, rows, gotRows)
	require.True(t, body.Const)
	assert.Equal(t, []int{rows * len("body"), int(desc.Bytes)}, blob, "a constant expands to its value per row")

	total := e.parts[0].sizeBytes()
	gotRows, blob = mergeShape(schema, e.parts, total/4)
	assert.InDelta(t, rows/4, gotRows, 1)
	assert.InDelta(t, rows, blob[0], 1)
	assert.InDelta(t, int(desc.Bytes)/4, blob[1], 1)
}

// writeTestPart writes f as a part under e and opens it.
func writeTestPart(t *testing.T, e *Engine, be backend.Backend, f *flushColumns) *part {
	t.Helper()

	ctx := context.Background()
	prefix := e.newPartPrefix()
	require.NoError(t, writePart(ctx, be, e.cfg.Schema, prefix, f, e.identitiesForColumn(f.stream),
		compress.AlgorithmNone, 0, e.blooms()))

	p, err := openPart(ctx, be, e.cfg.Schema, prefix, e.cfg.Obs.Corruption)
	require.NoError(t, err)

	return p
}

// reorderRows rewrites f's rows into the order given, every column alike.
func reorderRows(f *flushColumns, order []int) {
	stream := make([]chunk.U128, 0, len(order))
	for _, i := range order {
		stream = append(stream, f.stream[i])
	}

	f.stream = stream
	f.cols.ts = permute(f.cols.ts, order)

	for k := range f.cols.ints {
		f.cols.ints[k] = permute(f.cols.ints[k], order)
	}

	for k := range f.cols.bytes {
		var bc byteCol
		for _, i := range order {
			bc.appendCell(f.cols.bytes[k].at(i))
		}

		f.cols.bytes[k] = bc
	}
}

// mergedRows is every row of the parts as "ts/body" strings, sorted.
func mergedRows(t *testing.T, parts []*part) []string {
	t.Helper()

	var out []string

	for _, p := range parts {
		c, err := p.readCols(context.Background(), fullSel(p.schema), nil, nil)
		require.NoError(t, err)

		for i := range c.len() {
			out = append(out, fmt.Sprintf("%d/%s", c.ts[i], c.bytes[0].at(i)))
		}
	}

	slices.Sort(out)

	return out
}

// TestWholeSourceResolve: a part decoded whole resolves each dictionary into the union as it opens,
// and one decoded flat drops the column to the flat carry.
func TestWholeSourceResolve(t *testing.T) {
	t.Parallel()

	schema := NewSchema(
		Column{Name: "dict", Kind: KindBytes, Codec: chunk.CodecDict},
		Column{Name: "flat", Kind: KindBytes, Codec: chunk.CodecDict},
	)
	m, _, _ := newTestCarry(t, schema, 1)

	s := &wholeSource{d: &decodedPart{
		ts: []int64{1, 2},
		bytes: []mergeByteCol{
			newMergeByteCol(&chunk.DictColumn{Entries: [][]byte{[]byte("a")}, IDs: []byte{0, 0}, IDWidth: 1}),
			newMergeByteCol(&chunk.DictColumn{Entries: [][]byte{[]byte("x"), []byte("y")}}),
		},
		remap: make([][]int32, 2),
	}}

	s.resolve(0, m)
	s.resolve(1, m)
	s.resolve(1, m)

	assert.Equal(t, []int32{0}, s.d.remap[0])
	assert.Nil(t, m.dicts[1], "a flat source drops the column to the flat carry")

	empty := &wholeSource{d: &decodedPart{bytes: s.d.bytes, remap: make([][]int32, 2)}}
	empty.resolve(0, m)
	assert.Nil(t, empty.d.remap[0], "a part with no rows resolves nothing")
}

// TestCursorOpenRejectsMismatchedColumns: a cursor opened on a column the part lacks, or of the other
// kind, fails rather than decoding it as something it is not.
func TestCursorOpenRejectsMismatchedColumns(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	e := New(Config{Schema: headTestSchema, Backend: be, Prefix: "t/open"})
	p := writeTestPart(t, e, be, streamColumns(t, e, map[string][]int64{"a": {1, 2}}))

	var ic intCursor
	require.Error(t, ic.open(ctx, p.reader, "missing", 0))
	require.Error(t, ic.open(ctx, p.reader, "body", 0))

	m, _, _ := newTestCarry(t, headTestSchema, 1)

	var bc byteCursor
	require.Error(t, bc.open(ctx, p.reader, "missing", 0, m))
	require.Error(t, bc.open(ctx, p.reader, "sev", 0, m))
}
