package bucketindex_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend/bucketindex"
)

func TestMetricCatalogVersion(t *testing.T) {
	t.Parallel()
	ix := bucketindex.Index{CompactMetricMetadata: true, SeriesRoots: []bucketindex.SeriesRoot{seriesRoot("series/a", 1)}}
	v11 := ix.AppendBinary(nil)
	require.Equal(t, byte(11), v11[2])
	ix.MetricFeatures = bucketindex.MetricFeaturePackedWAL
	v12 := ix.AppendBinary(nil)
	require.Equal(t, byte(12), v12[2])
	out, err := bucketindex.Decode(v12)
	require.NoError(t, err)
	require.Equal(t, &ix, out)
	require.Equal(t, v12, out.AppendBinary(nil))

	readerV11 := bytes.Clone(v12)
	readerV11[2] = 11
	_, err = bucketindex.Decode(readerV11)
	require.ErrorIs(t, err, bucketindex.ErrCorrupt)

	for _, last := range []byte{0, byte(bucketindex.MetricFeaturePackedWAL), 0x7f, 0xff} {
		bad := bytes.Clone(v12)
		bad[len(bad)-1] = last
		_, err := bucketindex.Decode(bad)
		require.ErrorIs(t, err, bucketindex.ErrCorrupt)
	}
}
