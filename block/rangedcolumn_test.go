package block

import (
	"context"
	"math/rand/v2"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
)

// countingBackend records the bytes each read pulls, so a test can assert on what a ranged open
// actually costs rather than only on what it returns.
type countingBackend struct {
	backend.Backend

	bytes atomic.Int64
	reads atomic.Int64
}

func newCountingBackend(inner backend.Backend) *countingBackend {
	return &countingBackend{Backend: inner}
}

func (b *countingBackend) Read(ctx context.Context, key string) ([]byte, error) {
	v, err := b.Backend.Read(ctx, key)
	b.note(len(v))

	return v, err
}

func (b *countingBackend) ReadView(ctx context.Context, key string) ([]byte, error) {
	v, err := backend.ReadView(ctx, b.Backend, key)
	b.note(len(v))

	return v, err
}

func (b *countingBackend) ReadAt(ctx context.Context, key string, off, n int64) ([]byte, error) {
	v, err := backend.ReadAt(ctx, b.Backend, key, off, n)
	b.note(len(v))

	return v, err
}

// Size must be forwarded: the interface embedding above does not promote it, and without it a
// ranged open falls back to reading the whole object to learn its length — silently undoing the
// thing being tested.
func (b *countingBackend) Size(ctx context.Context, key string) (int64, error) {
	return backend.SizeOf(ctx, b.Backend, key)
}

func (b *countingBackend) note(n int) {
	b.bytes.Add(int64(n))
	b.reads.Add(1)
}

func (b *countingBackend) reset() {
	b.bytes.Store(0)
	b.reads.Store(0)
}

// writeRangedPart writes rows as a part and returns the backend it landed on. streamed selects the
// footer directory layout ([NewStreamWriterTo]) over the directory-first one.
func writeRangedPart(t *testing.T, rows metricRows, streamed bool, opts ...PartOption) *countingBackend {
	t.Helper()

	ctx := context.Background()
	b := newCountingBackend(newStreamingMemory())

	if streamed {
		rows.writeStreamTo(t, ctx, b, "p", false, opts...)

		return b
	}

	require.NoError(t, rows.writeStream(t, false, opts...).write(ctx, b, "p"))

	return b
}

// TestColumnBlocksMatchesWholeColumn is the correctness bar for reading a column by range: every
// block must decode to exactly what the whole-object reader produces, under both directory layouts.
func TestColumnBlocksMatchesWholeColumn(t *testing.T) {
	t.Parallel()

	for _, streamed := range []bool{false, true} {
		t.Run(layoutName(streamed), func(t *testing.T) {
			t.Parallel()

			for _, tc := range metricCases() {
				if len(tc.rows.ts) == 0 {
					continue // no columns, no part
				}

				t.Run(tc.name, func(t *testing.T) {
					t.Parallel()

					ctx := context.Background()
					opts := []PartOption{
						WithSortKey("ts"), WithGranuleSize(tc.gsize),
						WithCompression(compress.AlgorithmZSTD), WithCompressBlockBytes(64),
					}

					b := writeRangedPart(t, tc.rows, streamed, opts...)

					r, err := OpenPart(ctx, b, "p")
					require.NoError(t, err)

					// A corpus of one row collapses even the timestamp column to a constant, and a
					// collapsed column has no object to range over.
					if desc, ok := r.ColumnDescByName("ts"); !ok || desc.Const {
						t.Skip("the timestamp column is constant-collapsed in this corpus")
					}

					ranged, err := r.ColumnBlocks(ctx, "ts")
					require.NoError(t, err)

					col, err := r.Column(ctx, "ts")
					require.NoError(t, err)

					whole, err := col.BlockDecoder()
					require.NoError(t, err)

					require.Equal(t, whole.NumBlocks(), ranged.NumBlocks())
					require.Equal(t, whole.BlockRows(), ranged.BlockRows())

					for blk := range whole.NumBlocks() {
						want, err := whole.DecodeInt64(blk)
						require.NoError(t, err)

						got, err := ranged.DecodeInt64(blk)
						require.NoError(t, err, "block %d", blk)
						assert.Equal(t, want, got, "block %d", blk)
					}
				})
			}
		})
	}
}

func layoutName(streamed bool) string {
	if streamed {
		return "footer directory"
	}

	return "leading directory"
}

// TestColumnBlocksReadsOnlyWhatItDecodes is the point of the ranged open: the bytes it pulls must
// track the blocks decoded, not the column's size.
func TestColumnBlocksReadsOnlyWhatItDecodes(t *testing.T) {
	t.Parallel()

	for _, streamed := range []bool{false, true} {
		t.Run(layoutName(streamed), func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			rows := gen(200, 500, func(r *rand.Rand, _, _ int) float64 { return r.Float64() }, 21)
			opts := []PartOption{WithSortKey("ts"), WithGranuleSize(1024)}

			b := writeRangedPart(t, rows, streamed, opts...)

			// The value column, not the timestamp one: a regular timestamp step delta-encodes to
			// almost nothing, leaving no frames to skip.
			size, err := backend.SizeOf(ctx, b, columnKey("p", 2))
			require.NoError(t, err)
			require.Greater(t, size, int64(256<<10), "the column must span many frames to have anything to skip")

			r, err := OpenPart(ctx, b, "p")
			require.NoError(t, err)

			b.reset()

			d, err := r.ColumnBlocks(ctx, "value")
			require.NoError(t, err)

			opened := b.bytes.Load()

			_, err = d.DecodeFloat64(0)
			require.NoError(t, err)

			total := b.bytes.Load()
			t.Logf("open read %d bytes, one block %d more, of a %d byte column", opened, total-opened, size)

			assert.Less(t, total, size/2, "decoding one block must not read the column")
			assert.Positive(t, total-opened, "decoding a block must read the frame holding it")
		})
	}
}

// TestColumnBlocksRejectsUnrangeable pins the columns this path does not serve, so a caller gets an
// error rather than a decoder over nothing.
func TestColumnBlocksRejectsUnrangeable(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	rows := gen(4, 20, func(*rand.Rand, int, int) float64 { return 7 }, 5)
	b := writeRangedPart(t, rows, false, WithSortKey("ts"), WithGranuleSize(4))

	r, err := OpenPart(ctx, b, "p")
	require.NoError(t, err)

	_, err = r.ColumnBlocks(ctx, "value")
	require.Error(t, err, "an all-7 column is constant-collapsed and has no blocks")

	_, err = r.ColumnBlocks(ctx, "series")
	require.Error(t, err, "the id column is not block-framed")

	_, err = r.ColumnBlocks(ctx, "nope")
	require.Error(t, err)
}

// TestColumnBlocksSurvivesAHighCompressionRatio covers the bound the directory read is sized by: a
// granule's recorded length is measured in the *decompressed* frame, so on very compressible data it
// can dwarf the object holding it. A bound derived from the object size would come up short and the
// directory would parse as corrupt.
func TestColumnBlocksSurvivesAHighCompressionRatio(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	// A single repeated timestamp step compresses to almost nothing, so the object is orders of
	// magnitude smaller than the granule lengths its directory records.
	var rows metricRows

	for i := range 200_000 {
		rows.series = append(rows.series, chunk.U128{Lo: 1})
		rows.ts = append(rows.ts, int64(i)*15_000)
		rows.value = append(rows.value, float64(i%7))
	}

	b := writeRangedPart(t, rows, false,
		WithSortKey("ts"), WithGranuleSize(1024), WithCompression(compress.AlgorithmZSTD))

	size, err := backend.SizeOf(ctx, b, columnKey("p", 1))
	require.NoError(t, err)

	r, err := OpenPart(ctx, b, "p")
	require.NoError(t, err)

	ranged, err := r.ColumnBlocks(ctx, "ts")
	require.NoError(t, err)

	col, err := r.Column(ctx, "ts")
	require.NoError(t, err)

	whole, err := col.BlockDecoder()
	require.NoError(t, err)

	t.Logf("%d rows in a %d byte column, %d granules", len(rows.ts), size, ranged.NumBlocks())

	for _, blk := range []int{0, 1, ranged.NumBlocks() / 2, ranged.NumBlocks() - 1} {
		want, err := whole.DecodeInt64(blk)
		require.NoError(t, err)

		got, err := ranged.DecodeInt64(blk)
		require.NoError(t, err, "block %d", blk)
		assert.Equal(t, want, got, "block %d", blk)
	}
}

// TestColumnBlocksWithoutRecordedSize covers a part written before the manifest carried each
// column's byte size: the open must fall back to asking the backend and still work.
func TestColumnBlocksWithoutRecordedSize(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	rows := gen(20, 100, func(r *rand.Rand, _, _ int) float64 { return r.Float64() }, 31)
	b := writeRangedPart(t, rows, false, WithSortKey("ts"), WithGranuleSize(64))

	// Rewrite the manifest with the sizes stripped, as an older writer would have left it.
	raw, err := b.Read(ctx, manifestKey("p"))
	require.NoError(t, err)

	m, err := DecodeManifest(raw)
	require.NoError(t, err)

	for i := range m.Columns {
		m.Columns[i].Bytes = 0
	}

	require.NoError(t, b.Write(ctx, manifestKey("p"), m.Encode(nil)))

	r, err := OpenPart(ctx, b, "p")
	require.NoError(t, err)

	desc, ok := r.ColumnDescByName("ts")
	require.True(t, ok)
	require.Zero(t, desc.Bytes)

	ranged, err := r.ColumnBlocks(ctx, "ts")
	require.NoError(t, err)

	got, err := ranged.DecodeInt64(0)
	require.NoError(t, err)
	assert.Equal(t, rows.ts[:len(got)], got)
}
