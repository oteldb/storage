package engine

import (
	"context"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/signal"
)

// TestBuildPartIndexProperties checks the compact starts-offsets index against a brute-force scan
// of the sorted series column: every present id resolves to exactly its contiguous run, absent ids
// miss, the runs partition [0, rows), and rows() equals the column length.
func TestBuildPartIndexProperties(t *testing.T) {
	t.Parallel()

	rng := rand.New(rand.NewPCG(1, 2))

	for _, distinct := range []int{0, 1, 2, 7, 100} {
		// A sorted series column: ascending ids, each repeated 1..8 times.
		col := make([]chunk.U128, 0, distinct*8)

		runs := map[signal.SeriesID]rowRange{}

		for k := range distinct {
			id := chunk.U128{Hi: uint64(k / 3), Lo: uint64(k * 17)}
			n := 1 + rng.IntN(8)
			start := len(col)

			for range n {
				col = append(col, id)
			}

			runs[u128ToID(id)] = rowRange{start: start, end: len(col)}
		}

		idx := buildPartIndex(col)

		require.Equal(t, len(col), idx.rows(), "rows() equals the column length")
		require.Len(t, idx.ids, distinct)

		ctx := context.Background()

		for id, want := range runs {
			got, ok, err := idx.lookup(ctx, id)
			require.NoError(t, err)
			require.True(t, ok)
			assert.Equal(t, want, got)

			hasIt, err := idx.has(ctx, id)
			require.NoError(t, err)
			assert.True(t, hasIt)
		}

		_, ok, err := idx.lookup(ctx, signal.SeriesID{Hi: ^uint64(0), Lo: ^uint64(0)})
		require.NoError(t, err)
		assert.False(t, ok, "absent id misses")

		hasIt, err := idx.has(ctx, signal.SeriesID{Hi: ^uint64(0), Lo: ^uint64(0)})
		require.NoError(t, err)
		assert.False(t, hasIt)

		// The runs partition [0, rows): consecutive lookups over the sorted ids tile the row space.
		next := 0
		for _, id := range idx.ids {
			r, ok, err := idx.lookup(ctx, id)
			require.NoError(t, err)
			require.True(t, ok)
			assert.Equal(t, next, r.start, "runs are contiguous")
			assert.Greater(t, r.end, r.start, "runs are non-empty")
			next = r.end
		}

		assert.Equal(t, idx.rows(), next)
	}
}
