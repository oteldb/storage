package seriesindex

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

func TestMetadataReader(t *testing.T) {
	t.Parallel()
	src := indexSeries()
	data, _ := buildIndex(t, src)
	r := openTestIndex(t, data)
	reader := r.MetadataReader(t.Context())
	for _, i := range []int{0, 1, 1, 3, 2, 0} {
		meta, found, err := reader.Lookup(src[i].Hash())
		require.NoError(t, err)
		require.True(t, found)
		require.EqualValues(t, i+10, meta.Covered)
		require.Equal(t, Stats{Count: 3, Sum: 6, Min: 1, Max: 3}, meta.Stats)
	}
	_, found, err := reader.Lookup(signal.SeriesID{Hi: ^uint64(0), Lo: ^uint64(0)})
	require.NoError(t, err)
	require.False(t, found)
	meta, found, err := reader.Lookup(src[0].Hash())
	require.NoError(t, err)
	require.True(t, found)
	require.EqualValues(t, 10, meta.Covered)
}

func FuzzMetadataReader(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3})
	f.Add([]byte{3, 2, 1, 0})
	f.Add([]byte{0, 0, 255, 3})
	src := indexSeries()
	data, _ := buildIndex(f, src)
	f.Fuzz(func(t *testing.T, probes []byte) {
		if len(probes) > 256 {
			t.Skip()
		}
		r := openTestIndex(t, data)
		reader := r.MetadataReader(t.Context())
		for _, probe := range probes {
			i := int(probe) % len(src)
			meta, found, err := reader.Lookup(src[i].Hash())
			require.NoError(t, err)
			require.True(t, found)
			require.EqualValues(t, i+10, meta.Covered)
		}
	})
}
