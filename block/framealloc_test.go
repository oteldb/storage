package block

import (
	"bytes"
	"context"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/compress"
)

func TestStreamedFramesMatchBuffered(t *testing.T) {
	t.Parallel()

	for _, alg := range []compress.Algorithm{compress.AlgorithmNone, compress.AlgorithmLZ4, compress.AlgorithmZSTD} {
		t.Run(alg.String(), func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			be := backend.Memory()
			comp := compress.NewFrameCompressor(alg, compress.LevelDefault, 64<<10)
			a := newBlockAccum(comp, 128)
			require.NoError(t, a.attach(func() (backend.ObjectWriter, error) {
				return backend.CreateObject(ctx, be, "column")
			}))
			rnd := rand.New(rand.NewPCG(103, 107))
			expected := make([]byte, 0, 100<<10)
			for i, size := range []int{128, 64 << 10, 1024, 32 << 10} {
				input := bytes.Repeat([]byte{byte(i)}, size)
				if i%2 != 0 {
					for j := range input {
						input[j] = byte(rnd.Uint64())
					}
				}
				expected = append(expected, comp.Compress(nil, input)...)
				require.NoError(t, a.addGranule(func(dst []byte) ([]byte, error) { return append(dst, input...), nil }))
				require.GreaterOrEqual(t, a.residentBytes(), int64(cap(a.pending)+cap(a.packed)))
			}

			_, _, err := a.finish(context.Background(), 1, nil)
			require.NoError(t, err)
			obj, err := be.Read(ctx, "column")
			require.NoError(t, err)
			require.Greater(t, len(obj), len(expected))
			require.Equal(t, expected, obj[:len(expected)], "reusing a streamed buffer preserves every frame")
			require.Nil(t, a.packed, "finished writers release the scratch")
		})
	}
}
