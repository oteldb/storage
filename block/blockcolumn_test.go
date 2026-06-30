package block

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
)

// blockableCases enumerates the per-row sequential codecs that support block framing, each with a
// non-constant column (a constant column collapses and is never blocked).
type blockCase struct {
	name  string
	col   func(n int) Column // a column of n rows, with Block set
	int64 bool               // read back via Int64 (else Float64)
}

func blockCases() []blockCase {
	return []blockCase{
		{"dod", func(n int) Column {
			v := make([]int64, n)
			for i := range v {
				v[i] = int64(1000 + i*15) // monotonic timestamps
			}
			return Column{Name: "ts", Kind: KindInt64, Codec: chunk.CodecDoD, Int64: v, Block: true}
		}, true},
		{"t64", func(n int) Column {
			v := make([]int64, n)
			for i := range v {
				v[i] = int64(i*i - i) // varied, non-constant
			}
			return Column{Name: "c", Kind: KindInt64, Codec: chunk.CodecT64, Int64: v, Block: true}
		}, true},
		{"gorilla", func(n int) Column {
			v := make([]float64, n)
			for i := range v {
				v[i] = float64(i)*1.5 + 0.25
			}
			return Column{Name: "v", Kind: KindFloat64, Codec: chunk.CodecGorilla, Float64: v, Block: true}
		}, false},
		{"decimal", func(n int) Column {
			v := make([]float64, n)
			for i := range v {
				v[i] = float64(i) // integer-valued ⇒ decimal round-trips losslessly
			}
			return Column{Name: "v", Kind: KindFloat64, Codec: chunk.CodecDecimal, Float64: v, Block: true}
		}, false},
		{"autocodec", func(n int) Column {
			v := make([]float64, n)
			for i := range v {
				v[i] = float64(i) * 3.0
			}
			return Column{Name: "v", Kind: KindFloat64, AutoCodec: true, Float64: v, Block: true}
		}, false},
	}
}

// TestBlockedRoundTrip checks that a block-framed column decodes back to the same values as its
// unblocked encoding, across codecs, compressors, and row counts straddling the block size — and
// that it is flagged Blocked in the descriptor.
func TestBlockedRoundTrip(t *testing.T) {
	t.Parallel()

	const blockRows = 4
	rowCounts := []int{0, 1, 3, 4, 5, 8, 9, 16, 17}

	for _, tc := range blockCases() {
		for _, comp := range []struct {
			name string
			c    func() *compress.Compressor
		}{{"none", noneComp}, {"zstd", zstdComp}} {
			for _, n := range rowCounts {
				t.Run(tc.name+"/"+comp.name+"/n="+itoaT(n), func(t *testing.T) {
					t.Parallel()

					c := tc.col(n)

					blockedDesc, blockedObj, err := buildColumn(c, comp.c(), blockRows)
					require.NoError(t, err)
					// A single-value column constant-collapses (no data object), so it is never
					// blocked; every other non-empty column is.
					if !blockedDesc.Const {
						assert.True(t, blockedDesc.Blocked, "non-constant column must be blocked")
					}

					// Unblocked reference (same column, Block off).
					ref := c
					ref.Block = false
					refDesc, refObj, err := buildColumn(ref, comp.c(), blockRows)
					require.NoError(t, err)
					assert.False(t, refDesc.Blocked)

					if tc.int64 {
						got, err := newColumnReader(blockedDesc, blockedObj, comp.c(), n).Int64(nil)
						require.NoError(t, err)
						want, err := newColumnReader(refDesc, refObj, comp.c(), n).Int64(nil)
						require.NoError(t, err)
						assert.Equal(t, want, got)
					} else {
						got, err := newColumnReader(blockedDesc, blockedObj, comp.c(), n).Float64(nil)
						require.NoError(t, err)
						want, err := newColumnReader(refDesc, refObj, comp.c(), n).Float64(nil)
						require.NoError(t, err)
						assert.Equal(t, want, got)
					}
				})
			}
		}
	}
}

// TestBlockedRange checks the seek primitive: RangeInt64/RangeFloat64 over a blocked column returns
// exactly the same rows as slicing a full decode, for every window — and that the unblocked
// decode-and-slice fallback agrees too.
func TestBlockedRange(t *testing.T) {
	t.Parallel()

	const blockRows, n = 4, 18

	for _, tc := range blockCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := tc.col(n)
			desc, obj, err := buildColumn(c, noneComp(), blockRows)
			require.NoError(t, err)

			ref := c
			ref.Block = false
			refDesc, refObj, err := buildColumn(ref, noneComp(), blockRows)
			require.NoError(t, err)

			for lo := range n {
				for hi := lo + 1; hi <= n; hi++ {
					if tc.int64 {
						full, err := newColumnReader(desc, obj, noneComp(), n).Int64(nil)
						require.NoError(t, err)
						gotBlocked, err := newColumnReader(desc, obj, noneComp(), n).RangeInt64(nil, lo, hi)
						require.NoError(t, err)
						gotUnblocked, err := newColumnReader(refDesc, refObj, noneComp(), n).RangeInt64(nil, lo, hi)
						require.NoError(t, err)
						assert.Equal(t, full[lo:hi], gotBlocked, "blocked range [%d,%d)", lo, hi)
						assert.Equal(t, full[lo:hi], gotUnblocked, "unblocked range [%d,%d)", lo, hi)
					} else {
						full, err := newColumnReader(desc, obj, noneComp(), n).Float64(nil)
						require.NoError(t, err)
						gotBlocked, err := newColumnReader(desc, obj, noneComp(), n).RangeFloat64(nil, lo, hi)
						require.NoError(t, err)
						gotUnblocked, err := newColumnReader(refDesc, refObj, noneComp(), n).RangeFloat64(nil, lo, hi)
						require.NoError(t, err)
						assert.Equal(t, full[lo:hi], gotBlocked, "blocked range [%d,%d)", lo, hi)
						assert.Equal(t, full[lo:hi], gotUnblocked, "unblocked range [%d,%d)", lo, hi)
					}
				}
			}
		})
	}
}

// TestBlockedManifestFlag pins that the Blocked descriptor flag survives manifest encode/decode.
func TestBlockedManifestFlag(t *testing.T) {
	t.Parallel()

	m := Manifest{
		Version: manifestVersion, RowCount: 8, GranuleSize: 4,
		Columns: []ColumnDesc{
			{Name: "ts", Kind: KindInt64, Codec: chunk.CodecDoD, Blocked: true, MinInt64: 1, MaxInt64: 9},
			{Name: "plain", Kind: KindInt64, Codec: chunk.CodecT64},
		},
	}

	got, err := DecodeManifest(m.Encode(nil))
	require.NoError(t, err)
	assert.True(t, got.Columns[0].Blocked, "blocked column flag must round-trip")
	assert.False(t, got.Columns[1].Blocked, "unblocked column stays unblocked")
}

// TestBlockedPartRoundTrip writes a part with a blocked timestamp/value column through the full
// PartWriter/PartReader path (blocks sized to the granule size) and reads the columns back.
func TestBlockedPartRoundTrip(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	const n = 20

	ts := make([]int64, n)
	val := make([]float64, n)
	for i := range n {
		ts[i] = int64(100 + i*10)
		val[i] = float64(i) * 2.5
	}

	w := NewPartWriter(WithGranuleSize(4), WithSortKey("ts"))
	require.NoError(t, w.AddColumn(Column{Name: "ts", Kind: KindInt64, Codec: chunk.CodecDoD, Int64: ts, Block: true}))
	require.NoError(t, w.AddColumn(Column{Name: "v", Kind: KindFloat64, AutoCodec: true, Float64: val, Block: true}))

	b := backend.Memory()
	require.NoError(t, WritePart(ctx, b, "p", w))

	r, err := OpenPart(ctx, b, "p")
	require.NoError(t, err)

	tsCol, err := r.Column(ctx, "ts")
	require.NoError(t, err)
	assert.True(t, tsCol.desc.Blocked)
	gotTs, err := tsCol.Int64(nil)
	require.NoError(t, err)
	assert.Equal(t, ts, gotTs)

	vCol, err := r.Column(ctx, "v")
	require.NoError(t, err)
	gotVal, err := vCol.Float64(nil)
	require.NoError(t, err)
	assert.Equal(t, val, gotVal)

	// A mid-part window decodes via the block-seek path.
	rng, err := tsCol.RangeInt64(nil, 5, 13)
	require.NoError(t, err)
	assert.Equal(t, ts[5:13], rng)
}

// TestBlockedCursor drives the streaming cursors over a blocked column and checks they yield the
// same rows as a full decode, transparently crossing block boundaries — the merge path's contract.
func TestBlockedCursor(t *testing.T) {
	t.Parallel()

	const blockRows, n = 4, 18 // straddles block boundaries (4.5 blocks)

	ts := make([]int64, n)
	vals := make([]float64, n)
	for i := range n {
		ts[i] = int64(1000 + i*15)
		vals[i] = float64(i)*2.5 + 1
	}

	tsDesc, tsObj, err := buildColumn(Column{Name: "ts", Kind: KindInt64, Codec: chunk.CodecDoD, Int64: ts, Block: true}, noneComp(), blockRows)
	require.NoError(t, err)
	require.True(t, tsDesc.Blocked)

	cur, err := newColumnReader(tsDesc, tsObj, noneComp(), n).TsCursor()
	require.NoError(t, err)
	assert.Equal(t, n, cur.Len())

	for i := range n {
		assert.Equal(t, i, cur.Pos())
		got, err := cur.Next()
		require.NoError(t, err)
		assert.Equal(t, ts[i], got, "row %d", i)
	}

	_, err = cur.Next()
	require.Error(t, err, "cursor past the end errors")

	vDesc, vObj, err := buildColumn(Column{Name: "v", Kind: KindFloat64, Codec: chunk.CodecGorilla, Float64: vals, Block: true}, noneComp(), blockRows)
	require.NoError(t, err)

	fcur, err := newColumnReader(vDesc, vObj, noneComp(), n).FloatCursor()
	require.NoError(t, err)

	gotVals := make([]float64, 0, n)

	for range n {
		got, err := fcur.Next()
		require.NoError(t, err)

		gotVals = append(gotVals, got)
	}

	assert.Equal(t, vals, gotVals)
}

// FuzzBlockedRoundTrip fuzzes the blocked int64 round-trip: arbitrary values and a block size in
// 1..64 must encode and decode to the same slice.
func FuzzBlockedRoundTrip(f *testing.F) {
	f.Add([]byte{1, 2, 3, 4, 5, 6, 7, 8}, uint8(3))
	f.Add([]byte{}, uint8(1))

	f.Fuzz(func(t *testing.T, data []byte, blockSel uint8) {
		blockRows := int(blockSel%64) + 1

		vals := make([]int64, len(data))
		for i, b := range data {
			vals[i] = int64(int8(b)) * 7 // signed, varied
		}

		// Force a non-constant column so it is not const-collapsed (≥2 distinct values).
		if len(vals) >= 2 {
			vals[len(vals)-1] = vals[0] + 12345
		}

		c := Column{Name: "c", Kind: KindInt64, Codec: chunk.CodecT64, Int64: vals, Block: true}

		desc, obj, err := buildColumn(c, noneComp(), blockRows)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}

		if !desc.Blocked && len(vals) >= 2 {
			t.Fatalf("non-constant column should be blocked")
		}

		got, err := newColumnReader(desc, obj, noneComp(), len(vals)).Int64(nil)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}

		if len(got) != len(vals) {
			t.Fatalf("len: got %d want %d", len(got), len(vals))
		}

		for i := range vals {
			if got[i] != vals[i] {
				t.Fatalf("row %d: got %d want %d", i, got[i], vals[i])
			}
		}
	})
}

// FuzzBlockedDecodeNoPanic feeds arbitrary bytes to the blocked decode path: a corrupt object must
// error, never panic, and never read out of bounds.
func FuzzBlockedDecodeNoPanic(f *testing.F) {
	f.Add([]byte{0})
	f.Add([]byte{2, 4, 1, 1, 0xff})

	f.Fuzz(func(_ *testing.T, object []byte) {
		desc := ColumnDesc{Name: "c", Kind: KindInt64, Codec: chunk.CodecDoD, Blocked: true}
		// Must not panic regardless of the (arbitrary) declared row count. Exercise every decode
		// path that parses the directory: whole-column, range, block-set, and the streaming cursor.
		_, _ = newColumnReader(desc, object, noneComp(), 32).Int64(nil)
		_, _ = newColumnReader(desc, object, noneComp(), 32).RangeInt64(nil, 0, 8)
		_, _ = newColumnReader(desc, object, noneComp(), 32).DecodeBlocksInt64(nil, []int{0, 1, 5})

		if cur, err := newColumnReader(desc, object, noneComp(), 32).TsCursor(); err == nil {
			for range 40 {
				if _, err := cur.Next(); err != nil {
					break
				}
			}
		}

		if dir, err := parseBlockDir(object); err == nil {
			_ = dir.nBlocks()
		}
	})
}

// itoaT is a tiny non-allocating-ish int formatter for subtest names (avoids importing strconv into
// the test's hot table).
func itoaT(n int) string {
	if n == 0 {
		return "0"
	}

	var buf [20]byte

	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}

	return string(buf[i:])
}
