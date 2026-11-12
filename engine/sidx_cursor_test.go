package engine

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/internal/obs"
	"github.com/oteldb/storage/signal"
)

func TestSIDXPositionCursor(t *testing.T) {
	t.Parallel()
	const n = indexPageEntries*4 + 3
	for _, cache := range []int64{0, 4096, 2 << 20} {
		t.Run(strconv.FormatInt(cache, 10), func(t *testing.T) {
			t.Parallel()
			be := backend.Memory()
			col := make([]chunk.U128, n)
			for i := range col {
				col[i] = chunk.U128{Lo: uint64((i + 1) * 2)}
			}
			require.NoError(t, be.Write(t.Context(), sidxKey("p"), encodeSeriesIndex(col)))
			count := backendtest.NewByteCounter(be)
			index, ok := openPagedIndex(t.Context(), backend.Cached(count, cache), "p", n, obs.NewNop().Corruption)
			require.True(t, ok)
			count.Reset()
			var c sidxPositionCursor
			idx := partIndex{paged: index}
			for i := range n {
				pos, found, err := c.position(t.Context(), idx, signal.SeriesID{Lo: uint64((i + 1) * 2)})
				require.NoError(t, err)
				require.True(t, found)
				require.Equal(t, i, pos)
				_, found, err = c.position(t.Context(), idx, signal.SeriesID{Lo: uint64((i+1)*2 + 1)})
				require.NoError(t, err)
				require.False(t, found)
			}
			require.LessOrEqual(t, count.Reads(), int64(5), "at most one read per page despite tiny/absent cache")
			require.LessOrEqual(t, len(c.data), (indexPageEntries+1)*sidxEntryW)
			pos, found, err := c.position(t.Context(), idx, signal.SeriesID{Lo: 2})
			require.NoError(t, err)
			require.True(t, found)
			require.Zero(t, pos)
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			_, _, err = c.position(ctx, idx, signal.SeriesID{Lo: uint64(n * 2)})
			require.ErrorIs(t, err, context.Canceled)
			// A different owner cannot reuse the prior owner's page.
			copyIndex, ok := openPagedIndex(t.Context(), index.be, "p", n, obs.NewNop().Corruption)
			require.True(t, ok)
			_, found, err = c.position(t.Context(), partIndex{paged: copyIndex}, signal.SeriesID{Lo: 2})
			require.NoError(t, err)
			require.True(t, found)
		})
	}
}

func TestSIDXPositionCursorDenseFallback(t *testing.T) {
	t.Parallel()
	idx := partIndex{ids: []signal.SeriesID{{Lo: 2}, {Lo: 4}}}
	var c sidxPositionCursor
	for _, id := range []uint64{1, 2, 3, 4, 5} {
		got, found, err := c.position(t.Context(), idx, signal.SeriesID{Lo: id})
		want, exists, werr := idx.position(t.Context(), signal.SeriesID{Lo: id})
		require.Equal(t, werr, err)
		require.Equal(t, exists, found)
		require.Equal(t, want, got)
	}
	require.Nil(t, c.data)
}
