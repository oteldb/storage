package recordengine

import (
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend/backendtest"
	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/encoding/compress"
	"github.com/oteldb/storage/signal"
)

func TestCompressionFallbackLeavesColdNeighbor(t *testing.T) {
	t.Parallel()

	for _, tc := range []backendtest.Case{backendtest.Memory(), backendtest.Dir("file", file.New)} {
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()

			cfg := Config{Schema: headTestSchema, Backend: tc.Open(t), Prefix: "records",
				MergeMemoryBytes: -1, MergeCompression: compress.AlgorithmZSTD, MergeCompressionLevel: compress.LevelBest}
			e := New(cfg)
			rnd := rand.New(rand.NewPCG(107, 109))
			series := signal.Series{}
			appendRows := func(start, rows int) {
				t.Helper()
				b := &Batch{Stream: series.Hash(), Identity: func() signal.Series { return series },
					Ts: make([]int64, rows), Ints: [][]int64{make([]int64, rows)}, Bytes: [][][]byte{make([][]byte, rows)}}
				for i := range rows {
					b.Ts[i], b.Ints[0][i] = int64(start+i+1), int64(start+i)
					b.Bytes[0][i] = make([]byte, 256)
					for j := range b.Bytes[0][i] {
						b.Bytes[0][i][j] = byte(rnd.Uint64())
					}
				}
				_, err := e.AppendBatch(b, AppendLimits{})
				require.NoError(t, err)
				require.NoError(t, e.Flush(t.Context()))
			}

			appendRows(0, 1024)
			require.NoError(t, e.Merge(t.Context(), 0))
			require.Len(t, e.parts, 1)
			cold := e.parts[0].prefix
			require.False(t, e.compressionForces(e.parts[0]))
			appendRows(1024, 4)
			want := mergedRows(t, e.parts)

			require.NoError(t, e.Merge(t.Context(), 0))
			assert.Contains(t, e.PartPrefixes(), cold, "upgrading a tiny fresh part must not rewrite its cold neighbor")
			assert.Equal(t, want, mergedRows(t, e.parts))

			reopened := New(cfg)
			require.NoError(t, reopened.LoadParts(t.Context()))
			assert.Equal(t, want, mergedRows(t, reopened.parts))
			require.NoError(t, reopened.MergeWith(t.Context(), MergeOptions{Force: true}))
			assert.Equal(t, 1, reopened.PartCount(), "explicit force can still collapse an uneven pair")
			assert.Equal(t, want, mergedRows(t, reopened.parts))
		})
	}
}
