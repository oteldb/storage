package recordengine

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/block"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/signal"
)

func TestSidecarDictionaryMatchesSelectedRows(t *testing.T) {
	t.Parallel()
	for _, tc := range bloomCorpus(t) {
		if tc.mode == BloomNone {
			continue
		}
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for _, width := range []int{0, 1, 2} {
				entries := make([][]byte, 1, 1+tc.values.rows())
				entries[0] = []byte("unused-entry-must-not-reach-bloom")
				ids := make([]byte, 0, tc.values.rows()*max(width, 1))
				for row := range tc.values.rows() {
					entries = append(entries, tc.values.at(row))
					switch width {
					case 1:
						ids = append(ids, byte(row+1))
					case 2:
						ids = append(ids, byte((row+1)>>8), byte(row+1))
					}
				}
				// Use only a bounded prefix for the one-byte id form.
				if width == 1 && len(entries) > 256 {
					continue
				}
				dc := chunk.DictColumn{Entries: entries, IDs: ids, IDWidth: width}
				if width == 0 {
					dc.Entries = entries[1:]
				}
				s := &columnSink{bloom: &bloomAccum{mode: tc.mode}, seen: make(map[uint64]int32)}
				var cache sidecarCache
				n := dc.Len()
				lo, hi := n/4, n-n/4
				mid := (lo + hi) / 2
				cache.add(s, &dc, lo, mid)
				cache.add(s, &dc, mid, hi)
				var selected byteCol
				for row := lo; row < hi; row++ {
					selected.appendCell(dc.At(row))
				}
				require.Equal(t, buildColumnBloom(tc.mode, &selected), s.bloom.encode(), "width%d", width)
				require.Equal(t, selected.byteSize(), s.bloom.bytes, "width%d", width)
				if width != 0 && len(cache.tokens) != 0 {
					require.Zero(t, cache.tokens[0], "unused dictionary entries are not inspected")
				}
			}
		})
	}
}

func TestSidecarZeroTokenAndReset(t *testing.T) {
	t.Parallel()
	s := &columnSink{bloom: &bloomAccum{mode: BloomFullText}, seen: make(map[uint64]int32)}
	dc := chunk.DictColumn{Entries: [][]byte{nil}, IDs: []byte{0, 0, 0}, IDWidth: 1}
	var cache sidecarCache
	cache.add(s, &dc, 0, 3)
	require.Equal(t, uint32(1), cache.tokens[0], "zero tokens are memoized")
	cache.reset()
	dc.Entries = [][]byte{[]byte("new table has four tokens")}
	cache.add(s, &dc, 0, 3)
	var expected byteCol
	for range 3 {
		expected.appendCell(dc.Entries[0])
	}
	require.Equal(t, buildColumnBloom(BloomFullText, &expected), s.bloom.encode())
	require.EqualValues(t, cap(cache.tokens)*4, cache.residentBytes())
}

func TestWriterSidecarGenerationAndOutputIsolation(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	e := New(Config{Schema: streamedSchema, Backend: backend.Memory(), Prefix: "sidecar-generation"})
	w, err := newRecordPartStreamWriter(ctx, e, []*part{{}}, nil)
	require.NoError(t, err)
	defer w.abort()
	kv := func(key, value string) []byte {
		return signal.NewAttributes(signal.KeyValue{Key: []byte(key), Value: signal.StringValue([]byte(value))}).AppendHashInput(nil)
	}
	a, b, unused := kv("first", "1"), kv("second", "2"), kv("unused", "3")
	var expected byteCol
	for _, stable := range []bool{true, false} {
		for _, v := range [][]byte{a, b} {
			dc := chunk.DictColumn{Entries: [][]byte{v, unused}, IDs: []byte{0, 0, 0, 0, 0, 0, 1}, IDWidth: 1}
			g := block.OwnedGranule(&dc, block.NewDictGen())
			require.NoError(t, w.appendBytes(0, 1, g, dc.Entries, stable, 0, 6))
			for range 6 {
				expected.appendCell(v)
			}
		}
	}
	require.Equal(t, buildColumnBloom(BloomAttrs, &expected), w.sinks[1].bloom.encode())
	require.Equal(t, [][]byte{[]byte("first"), []byte("second")}, w.sinks[1].keys.sorted())
	require.Positive(t, w.binds[1].stableSide.residentBytes())
	require.Positive(t, w.binds[1].framedSide.residentBytes())
	other, err := newRecordPartStreamWriter(ctx, e, []*part{{}}, nil)
	require.NoError(t, err)
	defer other.abort()
	dc := chunk.DictColumn{Entries: [][]byte{b}, IDs: []byte{0}, IDWidth: 1}
	g := block.OwnedGranule(&dc, block.NewDictGen())
	require.NoError(t, other.appendBytes(0, 1, g, dc.Entries, true, 0, 1))
	require.Equal(t, [][]byte{[]byte("second")}, other.sinks[1].keys.sorted(), "each output sees only its emitted values")
}

func TestSidecarCacheWaitsForReuse(t *testing.T) {
	t.Parallel()
	dc := chunk.DictColumn{Entries: [][]byte{[]byte("first"), []byte("second")}, IDs: []byte{0, 1, 0, 1, 0, 1}, IDWidth: 1}
	s := &columnSink{bloom: &bloomAccum{mode: BloomFullText}, seen: make(map[uint64]int32)}
	var cache sidecarCache
	cache.add(s, &dc, 0, 4)
	require.Zero(t, cache.residentBytes(), "two visits cannot repay memoization")
	cache.add(s, &dc, 4, 6)
	require.EqualValues(t, 8, cache.residentBytes())
	var expected byteCol
	for row := range dc.Len() {
		expected.appendCell(dc.At(row))
	}
	require.Equal(t, buildColumnBloom(BloomFullText, &expected), s.bloom.encode())
	cache.reset()
	cache.add(s, &dc, 0, 2)
	require.Empty(t, cache.tokens, "the next dictionary starts a fresh reuse count")
}

func TestSidecarEqualityAlsoCollectsKeys(t *testing.T) {
	t.Parallel()
	v := signal.NewAttributes(signal.KeyValue{Key: []byte("host"), Value: signal.StringValue([]byte("node"))}).AppendHashInput(nil)
	dc := chunk.DictColumn{Entries: [][]byte{v}, IDs: []byte{0, 0, 0}, IDWidth: 1}
	s := &columnSink{bloom: &bloomAccum{mode: BloomEquality}, keys: make(recordKeySet), seen: make(map[uint64]int32)}
	var cache sidecarCache
	cache.add(s, &dc, 0, 1)
	cache.add(s, &dc, 1, 3)
	require.Equal(t, [][]byte{[]byte("host")}, s.keys.sorted())
	var expected byteCol
	for range 3 {
		expected.appendCell(v)
	}
	require.Equal(t, buildColumnBloom(BloomEquality, &expected), s.bloom.encode())
}

func FuzzSidecarDictionaryMatchesRows(f *testing.F) {
	f.Add([]byte("hello world"), []byte("second value"), byte(31), byte(1))
	f.Add([]byte{}, []byte{0xff}, byte(200), byte(2))
	f.Fuzz(func(t *testing.T, a, b []byte, repeat, width byte) {
		if len(a) > 4096 || len(b) > 4096 {
			t.Skip()
		}
		dc := chunk.DictColumn{Entries: [][]byte{a, b, []byte("unreferenced")}, IDWidth: int(width % 3)}
		rows := int(repeat) + 1
		var expected byteCol
		flat := make([][]byte, 0, rows)
		for row := range rows {
			id := byte(row % 2)
			flat = append(flat, dc.Entries[id])
			switch dc.IDWidth {
			case 1:
				dc.IDs = append(dc.IDs, id)
			case 2:
				dc.IDs = append(dc.IDs, 0, id)
			}
		}
		if dc.IDWidth == 0 {
			dc.Entries = flat
		}
		lo, hi := rows/5, rows-rows/5
		for row := lo; row < hi; row++ {
			expected.appendCell(dc.At(row))
		}
		for _, mode := range []BloomMode{BloomFullText, BloomAttrs, BloomEquality} {
			s := &columnSink{bloom: &bloomAccum{mode: mode}, seen: make(map[uint64]int32)}
			var cache sidecarCache
			cache.add(s, &dc, lo, (lo+hi)/2)
			cache.add(s, &dc, (lo+hi)/2, hi)
			if !bytes.Equal(buildColumnBloom(mode, &expected), s.bloom.encode()) {
				t.Fatalf("mode%d width%d mismatch", mode, dc.IDWidth)
			}
		}
	})
}
