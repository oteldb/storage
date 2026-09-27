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
