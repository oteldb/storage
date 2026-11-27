package engine

import (
	"testing"

	"github.com/stretchr/testify/require"

	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/internal/watermark"
	"github.com/oteldb/storage/signal"
)

func TestSpilledSeriesSidecarsPreserveStatsAndCoverage(t *testing.T) {
	t.Parallel()
	files, err := backendfile.New(t.TempDir())
	require.NoError(t, err)
	e := separateCatalogEngine(files)
	t.Cleanup(func() { require.NoError(t, e.CloseWAL()) })
	p, err := newPartStreamWriterAt(t.Context(), e, "spool", compressProfile{}, 0, false, true, nil)
	require.NoError(t, err)
	defer p.abort()
	for i := range seriesMetadataMemoryLimit + 2 {
		id := chunk.U128{Hi: 1, Lo: uint64(i)}
		require.NoError(t, p.appendSeries(id, []int64{1}, []float64{2}, nil, 3))
		require.NoError(t, p.appendSeries(id, []int64{2}, []float64{3}, nil, int64(i+4)))
	}
	require.NotNil(t, p.meta)
	require.Len(t, p.covered, 1)
	require.Len(t, p.stats, 1)
	require.NoError(t, p.writeSeriesSidecars(t.Context()))
	coverageRaw, err := files.Read(t.Context(), watermark.Key(p.prefix))
	require.NoError(t, err)
	coverage, err := watermark.Decode(coverageRaw)
	require.NoError(t, err)
	require.Len(t, coverage, seriesMetadataMemoryLimit+2)
	statsRaw, err := files.Read(t.Context(), statsKey(p.prefix))
	require.NoError(t, err)
	stats, err := decodeSeriesStats(statsRaw)
	require.NoError(t, err)
	for i, ent := range coverage {
		id := signal.SeriesID{Hi: 1, Lo: uint64(i)}
		require.Equal(t, id, ent.ID)
		require.Equal(t, int64(i+4), ent.Max)
		require.Equal(t, SeriesAgg{Count: 2, Rows: 2, Sum: 5, Min: 2, Max: 3}, stats[id])
	}
	p.dropSeriesMetadata(t.Context())
	keys, err := files.List(t.Context(), p.prefix+"/")
	require.NoError(t, err)
	require.NotContains(t, keys, p.prefix+"/.series-meta")
}
