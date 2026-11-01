package block

import (
	"context"
	"math/rand/v2"
	"sync"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/compress"
)

type contextRangeBackend struct{ backend.Backend }

func (b contextRangeBackend) ReadAt(ctx context.Context, key string, off, n int64) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return backend.ReadAt(ctx, b.Backend, key, off, n)
}

func TestDecoderCloneIsolatesContextAndScratch(t *testing.T) {
	t.Parallel()
	rows := gen(4, 40, func(r *rand.Rand, _, _ int) float64 { return r.Float64() }, 17)
	for _, streamed := range []bool{false, true} {
		t.Run(layoutName(streamed), func(t *testing.T) {
			t.Parallel()
			be := writeRangedPart(t, rows, streamed, WithSortKey("ts"), WithGranuleSize(16),
				WithCompressBlockBytes(64), WithCompression(compress.AlgorithmZSTD))
			r, err := OpenPart(t.Context(), contextRangeBackend{Backend: be}, "p", NewReadCompressors())
			require.NoError(t, err)
			ctx, cancel := context.WithCancel(t.Context())
			original, err := r.ColumnBlocks(ctx, "ts")
			require.NoError(t, err)
			cancel()
			_, err = original.DecodeInt64(0)
			require.ErrorIs(t, err, context.Canceled)
			clone := original.Clone(t.Context())
			got, err := clone.DecodeInt64(0)
			require.NoError(t, err)
			require.Equal(t, rows.ts[:len(got)], got)
			require.Empty(t, original.streams.buf)
			require.Positive(t, original.MetadataBytes())

			var wg sync.WaitGroup
			failures := make(chan error, 4)
			for range 4 {
				wg.Go(func() {
					d := original.Clone(t.Context())
					for block := range d.NumBlocks() {
						values, err := d.DecodeInt64(block)
						if err != nil {
							failures <- err
							return
						}
						lo := block * d.BlockRows()
						for i, value := range values {
							if value != rows.ts[lo+i] {
								failures <- errors.New("clone decoded a different timestamp")
								return
							}
						}
					}
				})
			}
			wg.Wait()
			close(failures)
			for err := range failures {
				require.NoError(t, err)
			}
		})
	}
}
