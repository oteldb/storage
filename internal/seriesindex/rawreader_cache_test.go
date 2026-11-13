package seriesindex

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
)

func TestRawReaderCachedIDsStreamedDescriptors(t *testing.T) {
	t.Parallel()
	src := indexSeries()
	data, _ := buildIndex(t, src)
	const limit = 1 << 20
	be := backend.Cached(backend.Memory(), limit)
	require.NoError(t, be.Write(t.Context(), "index", data))
	index, err := Open(t.Context(), be, "index")
	require.NoError(t, err)
	r := index.RawReader(t.Context())
	r.StreamDescriptors()
	for _, i := range []int{3, 0, 1, 2, 1, 3, 0} {
		raw, _, found, err := r.Lookup(src[i].Hash())
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, src[i].AppendHashInput(nil), raw)
		require.NotEmpty(t, r.ids.decoded, "ID lookup keeps checked offsets")
		require.Empty(t, r.records.cursor.decoded, "descriptor payloads stay in private scratch")
	}
	require.LessOrEqual(t, be.(interface{ Stats() backend.CacheStats }).Stats().Bytes, int64(limit))
}
