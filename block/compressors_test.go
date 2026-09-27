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

// TestWithCompressors: a writer compresses through a handed-in compressor whose algorithm and level
// match, keeps one of its own otherwise, and writes the same part either way.
func TestWithCompressors(t *testing.T) {
	t.Parallel()

	given := compress.NewCompressor(compress.AlgorithmZSTD, compress.LevelBest)
	other := compress.NewCompressor(compress.AlgorithmZSTD, compress.LevelFast)

	opts := []PartOption{WithCompression(compress.AlgorithmZSTD), WithCompressionLevel(compress.LevelBest)}

	w := NewStreamWriter(append(opts, WithCompressors(other, given))...)
	assert.Same(t, given, w.compressorFor(compress.AlgorithmZSTD))
	assert.Same(t, given, w.compressorFor(compress.AlgorithmZSTD), "the choice is kept")

	lz4 := w.compressorFor(compress.AlgorithmLZ4)
	assert.Equal(t, compress.AlgorithmLZ4, lz4.Algorithm(), "no handed-in compressor matches")
	assert.Same(t, lz4, w.compressorFor(compress.AlgorithmLZ4))

	own := NewPartWriter(opts...)
	assert.NotSame(t, given, own.compressorFor(compress.AlgorithmZSTD))

	ctx := context.Background()
	vals := make([]int64, 3*defaultGranuleSize)

	for i := range vals {
		vals[i] = int64(i % 97)
	}

	write := func(extra ...PartOption) map[string][]byte {
		t.Helper()

		b := backend.Memory()
		pw := NewPartWriter(append(opts, extra...)...)
		require.NoError(t, pw.AddColumn(Column{Name: "v", Kind: KindInt64, Codec: chunk.CodecT64, Int64: vals, Block: true}))
		require.NoError(t, WritePart(ctx, b, "p", pw))

		keys, err := b.List(ctx, "")
		require.NoError(t, err)

		out := make(map[string][]byte, len(keys))
		for _, k := range keys {
			out[k], err = b.Read(ctx, k)
			require.NoError(t, err)
		}

		return out
	}

	assert.Equal(t, write(), write(WithCompressors(given)))
}

// TestWithReadCompressors: every reader handed the same set decompresses through it.
func TestWithReadCompressors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := backend.Memory()

	w := NewPartWriter(WithCompression(compress.AlgorithmZSTD))
	require.NoError(t, w.AddColumn(Column{Name: "v", Kind: KindInt64, Int64: []int64{1, 2, 3}}))
	require.NoError(t, WritePart(ctx, b, "p", w))

	shared := NewReadCompressors()

	r1, err := OpenPart(ctx, b, "p", shared)
	require.NoError(t, err)
	r2, err := OpenPart(ctx, b, "p", shared)
	require.NoError(t, err)
	own, err := OpenPart(ctx, b, "p")
	require.NoError(t, err)

	zstd := r1.compressorFor(compress.AlgorithmZSTD)
	assert.Same(t, zstd, r2.compressorFor(compress.AlgorithmZSTD))
	assert.Same(t, r1.compressorFor(compress.AlgorithmLZ4), r2.compressorFor(compress.AlgorithmLZ4))
	assert.NotSame(t, zstd, own.compressorFor(compress.AlgorithmZSTD))

	col, err := r2.Column(ctx, "v")
	require.NoError(t, err)

	got, err := col.Int64(nil)
	require.NoError(t, err)
	assert.Equal(t, []int64{1, 2, 3}, got)
}
