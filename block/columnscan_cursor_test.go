package block

import (
	"context"
	"math"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
)

type rowCursorOf[T any] interface{ Next() (T, error) }

// drain reads exactly n rows and then checks the cursor is exhausted rather than running on.
func drain[T any](t *testing.T, c rowCursorOf[T], n int) []T {
	t.Helper()

	out := make([]T, 0, n)

	for i := range n {
		v, err := c.Next()
		require.NoErrorf(t, err, "row %d", i)

		out = append(out, v)
	}

	_, err := c.Next()
	require.Error(t, err, "the cursor ran past the column's rows")

	return out
}

func floatBits(vs []float64) []uint64 {
	out := make([]uint64, len(vs))
	for i, v := range vs {
		out[i] = math.Float64bits(v)
	}

	return out
}

// TestColumnScanCursorsMatchWholeColumn is what the metrics merge reads through: a forward cursor
// over a windowed scan must yield exactly the whole-object decode, under both directory layouts and
// at windows below a frame, around one, and past the column.
func TestColumnScanCursorsMatchWholeColumn(t *testing.T) {
	t.Parallel()

	for _, streamed := range []bool{false, true} {
		for _, tc := range metricCases() {
			if len(tc.rows.ts) == 0 {
				continue
			}

			t.Run(layoutName(streamed)+"/"+tc.name, func(t *testing.T) {
				t.Parallel()

				ctx := context.Background()

				b := writeRangedPart(t, tc.rows, streamed,
					WithSortKey("ts"), WithGranuleSize(tc.gsize),
					WithCompression(compress.AlgorithmZSTD), WithCompressBlockBytes(64))

				r, err := OpenPart(ctx, b, "p")
				require.NoError(t, err)

				for _, window := range []int64{0, 1, 256, 1 << 20} {
					if desc, _ := r.ColumnDescByName("ts"); !desc.Const {
						col, err := r.Column(ctx, "ts")
						require.NoError(t, err)

						want, err := col.Int64(nil)
						require.NoError(t, err)

						scan, err := r.ColumnScan(ctx, "ts", window)
						require.NoError(t, err)

						cur, err := scan.TsCursor()
						require.NoError(t, err)

						assert.Equalf(t, want, drain(t, cur, r.RowCount()), "ts, window %d", window)
					}

					if desc, _ := r.ColumnDescByName("value"); !desc.Const {
						col, err := r.Column(ctx, "value")
						require.NoError(t, err)

						want, err := col.Float64(nil)
						require.NoError(t, err)

						scan, err := r.ColumnScan(ctx, "value", window)
						require.NoError(t, err)

						cur, err := scan.FloatCursor()
						require.NoError(t, err)

						assert.Equalf(t, floatBits(want), floatBits(drain(t, cur, r.RowCount())),
							"value, window %d", window)
					}
				}
			})
		}
	}
}

func TestDecoderCursorRejectsWrongColumn(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	rows := numericRows(2048)

	b := backend.Memory()
	w := NewPartWriter(WithGranuleSize(256), WithCompressBlockBytes(64))
	require.NoError(t, w.AddColumn(Column{Name: "ts", Kind: KindInt64, Codec: chunk.CodecDoD, Int64: rows.ts, Block: true}))
	require.NoError(t, w.AddColumn(Column{Name: "t64", Kind: KindInt64, Codec: chunk.CodecT64, Int64: rows.ts, Block: true}))
	require.NoError(t, w.AddColumn(Column{Name: "value", Kind: KindFloat64, Float64: rows.value, Block: true}))
	require.NoError(t, WritePart(ctx, b, "p", w))

	r, err := OpenPart(ctx, b, "p")
	require.NoError(t, err)

	open := func(name string) *Decoder {
		t.Helper()

		d, err := r.ColumnScan(ctx, name, 1<<20)
		require.NoError(t, err)

		return d
	}

	_, err = open("ts").FloatCursor()
	require.Error(t, err, "an int64 column is not a float cursor")

	_, err = open("value").TsCursor()
	require.Error(t, err, "a float64 column is not a timestamp cursor")

	_, err = open("t64").TsCursor()
	require.Error(t, err, "only delta-of-delta timestamps stream row by row")
}

// wholeReadBackend offers neither ranged nor view reads and counts every whole-object read by key.
type wholeReadBackend struct {
	backend.Backend

	mu    sync.Mutex
	reads map[string]int
}

func (b *wholeReadBackend) Read(ctx context.Context, key string) ([]byte, error) {
	b.mu.Lock()
	b.reads[key]++
	b.mu.Unlock()

	return b.Backend.Read(ctx, key)
}

// TestColumnScanReadsWholeObjectOnceWithoutRanges: over a backend that can only read whole objects,
// every ranged read — the directory probe, each window — is itself a whole-object read, so a
// windowed walk would re-read the column once per window. The scan reads it once instead.
func TestColumnScanReadsWholeObjectOnceWithoutRanges(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	rows := numericRows(8192)

	ranged := writeRangedPart(t, rows, false,
		WithSortKey("ts"), WithGranuleSize(256),
		WithCompression(compress.AlgorithmZSTD), WithCompressBlockBytes(64))

	const window = 256

	walk := func(r *PartReader) []int64 {
		t.Helper()

		scan, err := r.ColumnScan(ctx, "ts", window)
		require.NoError(t, err)

		cur, err := scan.TsCursor()
		require.NoError(t, err)

		return drain(t, cur, r.RowCount())
	}

	rr, err := OpenPart(ctx, ranged, "p")
	require.NoError(t, err)

	ranged.reset()
	want := walk(rr)
	require.Greater(t, ranged.reads.Load(), int64(4), "the corpus must span several windows")

	whole := &wholeReadBackend{Backend: ranged.Backend, reads: map[string]int{}}

	wr, err := OpenPart(ctx, whole, "p")
	require.NoError(t, err)

	assert.Equal(t, want, walk(wr))

	i, ok := wr.byName["ts"]
	require.True(t, ok)
	assert.Equal(t, 1, whole.reads[columnKey("p", i)], "the column object must be read exactly once")
}

// TestColumnScanReadsLegacyLayout: the pre-framing layout has no directory a range can find, so a
// scan over it reads the object whole instead of refusing a part that still has to merge.
func TestColumnScanReadsLegacyLayout(t *testing.T) {
	t.Parallel()

	const blockRows, n = 16, 1000

	ctx := context.Background()
	rows := numericRows(n)
	c := Column{Name: "ts", Kind: KindInt64, Codec: chunk.CodecDoD, Int64: rows.ts, Block: true}

	desc, _, err := buildColumn(c, zstdComp(), blockRows, defaultCompressBlockBytes)
	require.NoError(t, err)
	require.True(t, desc.Framed)

	desc.Framed = false
	obj := encodeLegacyBlocked(t, c, desc.Codec, zstdComp(), blockRows)
	desc.Bytes = int64(len(obj))

	b := backend.Memory()
	m := Manifest{Version: manifestVersion, RowCount: n, GranuleSize: blockRows, Columns: []ColumnDesc{desc}}
	require.NoError(t, b.Write(ctx, columnKey("p", 0), obj))
	require.NoError(t, b.Write(ctx, manifestKey("p"), m.Encode(nil)))

	r, err := OpenPart(ctx, b, "p")
	require.NoError(t, err)

	_, err = r.ColumnBlocks(ctx, "ts")
	require.Error(t, err, "the ranged path cannot locate a legacy directory")

	scan, err := r.ColumnScan(ctx, "ts", 64)
	require.NoError(t, err)

	cur, err := scan.TsCursor()
	require.NoError(t, err)

	assert.Equal(t, rows.ts, drain(t, cur, n))
}

// TestBlockDecoderDecodesSharedDictionary: a whole-object decoder must carry the column's kind and
// shared dictionary, or its shared granules' ids resolve against nothing.
func TestBlockDecoderDecodesSharedDictionary(t *testing.T) {
	t.Parallel()

	const granules, rows = 12, 256

	for _, tc := range sharedDictCases(granules) {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			vals := scanCorpus(granules, rows, tc.self)
			r, _ := writeBytesPart(t, vals, rows, WithCompressBlockBytes(64))

			if desc, _ := r.ColumnDescByName("attrs"); !desc.Blocked {
				t.Skip("no granule joined, so the writer emitted one unframed stream")
			}

			col, err := r.Column(ctx, "attrs")
			require.NoError(t, err)

			d, err := col.BlockDecoder()
			require.NoError(t, err)

			got, err := d.DecodeBytes(nil)
			require.NoError(t, err)
			require.Equal(t, len(vals), got.Len())

			for i, want := range vals {
				require.Equalf(t, want, got.At(i), "row %d", i)
			}
		})
	}
}
