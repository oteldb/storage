package recordengine

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/signal"
)

// byteColBytes is the resident footprint of one decoded byte column: the blob plus its offset index
// (readCols form) or the dictionary entries + slice headers + packed ids (readForMerge dict form).
func mergeByteColBytes(m mergeByteCol) int {
	if m.dict != nil {
		n := len(m.dict.IDs) + 24*len(m.dict.Entries) // 24B per []byte header
		for _, e := range m.dict.Entries {
			n += len(e)
		}

		return n
	}

	return len(m.flat.data) + 4*len(m.flat.offsets)
}

func recordColsByteBytes(c *recordCols) int {
	n := 0
	for k := range c.bytes {
		n += len(c.bytes[k].data) + 4*len(c.bytes[k].offsets)
	}

	return n
}

// TestNewMergeByteColForms covers both decode forms of a merge byte column: a real dictionary
// (IDWidth > 0) is kept compressed, while the flat fallback (IDWidth 0 — a column the writer found no
// dedup for) is expanded into a packed byteCol. Both must return the same cells through at().
func TestNewMergeByteColForms(t *testing.T) {
	t.Parallel()

	// Dict form: two unique entries referenced by 1-byte ids — kept as-is.
	dict := newMergeByteCol(&chunk.DictColumn{
		Entries: [][]byte{[]byte("a"), []byte("bb")},
		IDs:     []byte{0, 1, 0, 1},
		IDWidth: 1,
	})
	require.NotNil(t, dict.dict, "a real dictionary must stay compressed")
	for i, want := range []string{"a", "bb", "a", "bb"} {
		assert.Equal(t, want, string(dict.at(i)))
	}

	// Flat fallback: Entries holds one value per row (IDWidth 0) — must expand to a packed byteCol,
	// which is smaller than one []byte header per row.
	flat := newMergeByteCol(&chunk.DictColumn{
		Entries: [][]byte{[]byte("x"), []byte("yy"), []byte("zzz")},
		IDWidth: 0,
	})
	require.Nil(t, flat.dict, "the flat fallback must expand, not keep the dict")
	for i, want := range []string{"x", "yy", "zzz"} {
		assert.Equal(t, want, string(flat.at(i)))
	}
}

// TestMergeByteColExpandedBytes pins the sizing measure the merge output buffer is pre-allocated
// from: the blob the column's cells expand to, across both decode forms and both dictionary id
// widths. An undercount puts the buffer back on doubling growth, an overcount wastes a part's worth
// of memory, and neither shows up as a test failure anywhere else.
func TestMergeByteColExpandedBytes(t *testing.T) {
	t.Parallel()

	wide := &chunk.DictColumn{IDs: make([]byte, 0, 6), IDWidth: 2}
	for i := range 300 {
		wide.Entries = append(wide.Entries, fmt.Appendf(nil, "e%03d", i)) // 4 bytes each
	}

	for _, id := range []int{0, 299, 150} {
		wide.IDs = append(wide.IDs, byte(id>>8), byte(id))
	}

	for _, tt := range []struct {
		name string
		col  mergeByteCol
		want int64
	}{
		{
			name: "dict_1b_ids",
			col: newMergeByteCol(&chunk.DictColumn{
				Entries: [][]byte{[]byte("a"), []byte("bbb")},
				IDs:     []byte{0, 1, 1, 0},
				IDWidth: 1,
			}),
			want: 8, // a + bbb + bbb + a
		},
		{name: "dict_2b_ids", col: newMergeByteCol(wide), want: 12},
		{
			name: "flat_fallback",
			col: newMergeByteCol(&chunk.DictColumn{
				Entries: [][]byte{[]byte("x"), []byte("yy"), []byte("zzz")},
			}),
			want: 6,
		},
		{name: "empty", col: newMergeByteCol(&chunk.DictColumn{}), want: 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, tt.col.expandedBytes())

			// Cross-check against the cells themselves: the measure must equal what a walk would copy.
			rows := tt.col.flat.rows()
			if tt.col.dict != nil {
				rows = tt.col.dict.Len()
			}

			var sum int64
			for i := range rows {
				sum += int64(len(tt.col.at(i)))
			}

			assert.Equal(t, sum, tt.col.expandedBytes())
		})
	}
}

// TestDecodedShapeScalesToCap covers the two regimes of the output-buffer sizing: sources that fit
// one part are sized whole, and sources that outgrow the seal threshold are scaled down to what a
// single output part holds — otherwise a merge emitting a dozen parts would reserve all of them.
func TestDecodedShapeScalesToCap(t *testing.T) {
	t.Parallel()

	src := []*decodedPart{{
		ts:   make([]int64, 100),
		ints: [][]int64{make([]int64, 100)},
		bytes: []mergeByteCol{newMergeByteCol(&chunk.DictColumn{
			Entries: [][]byte{make([]byte, 10)},
			IDs:     make([]byte, 100), // every row the single 10-byte entry
			IDWidth: 1,
		})},
	}}

	// 100 rows × (8 ts + 8 int + 16 stream id) + 100 × 10 blob bytes.
	const total = 100*32 + 1000

	rows, blob := decodedShape(src, 0)
	assert.Equal(t, 100, rows)
	assert.Equal(t, []int{1000}, blob)

	rows, blob = decodedShape(src, total)
	assert.Equal(t, 100, rows)
	assert.Equal(t, []int{1000}, blob)

	rows, blob = decodedShape(src, total/4)
	assert.Equal(t, 25, rows)
	assert.Equal(t, []int{250}, blob)

	rows, blob = decodedShape(nil, 0)
	assert.Zero(t, rows)
	assert.Nil(t, blob)
}

// TestMergeDecodeDictCompact verifies the merge's dict-compressed decode ([part.readForMerge]) holds a
// far smaller resident set than the fetch-path whole-blob decode ([part.readCols]) when byte values
// repeat — the common log case (templated bodies, low-cardinality attributes). This is the constant the
// DictColumn reduction saves on top of the size-tiered selection that bounds how many parts a merge
// holds.
func TestMergeDecodeDictCompact(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	schema := NewSchema(
		Column{Name: "sev", Kind: KindInt64, Codec: chunk.CodecT64},
		Column{Name: "body", Kind: KindBytes, Codec: chunk.CodecDict, Bloom: BloomFullText},
	)

	e := New(Config{Schema: schema, Backend: backend.Memory(), Prefix: "t/recs"})

	// One stream of many records drawn from a small set of distinct bodies — heavy repetition, exactly
	// what dictionary encoding is for.
	const (
		rows      = 20_000
		templates = 16
	)

	series := signal.Series{Resource: signal.Resource{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte("svc"))},
	)}}

	ts := make([]int64, rows)
	sev := make([]int64, rows)
	bodies := make([][]byte, rows)
	for i := range rows {
		ts[i] = int64(i)
		sev[i] = int64(i % 9)
		bodies[i] = fmt.Appendf(nil, "GET /api/v1/resource/%d status=200 handler=template done", i%templates)
	}

	b := &Batch{
		Stream:   series.Hash(),
		Identity: func() signal.Series { return series },
		Ts:       ts,
		Ints:     [][]int64{sev},
		Bytes:    [][][]byte{bodies},
	}

	_, err := e.AppendBatch(b, AppendLimits{})
	require.NoError(t, err)
	require.NoError(t, e.Flush(ctx))
	require.Len(t, e.parts, 1)

	p := e.parts[0]

	full, err := p.readCols(ctx, fullSel(schema), nil, nil)
	require.NoError(t, err)
	expanded := recordColsByteBytes(full)

	d, err := p.readForMerge(ctx)
	require.NoError(t, err)
	require.Len(t, d.bytes, 1)
	require.NotNil(t, d.bytes[0].dict, "a repetitive column must decode to the compact dict form, not flat")
	dictBytes := mergeByteColBytes(d.bytes[0])

	t.Logf("byte column resident: expanded=%d B, dict=%d B (%.1fx smaller)",
		expanded, dictBytes, float64(expanded)/float64(dictBytes))

	// With 16 distinct bodies across 20k rows the dictionary is ~16 entries + 20k×1B ids, vs a 20k-cell
	// blob — a large reduction. Require at least 4x to guard the property without being brittle.
	assert.Less(t, dictBytes*4, expanded, "dict decode must hold far less than the expanded blob")
}
