package engine

import (
	"testing"

	"github.com/stretchr/testify/require"

	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/block"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/internal/seriesindex"
	"github.com/oteldb/storage/internal/sidecar"
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

func TestOrdinalSeriesSidecarsSkipSpool(t *testing.T) {
	t.Parallel()
	files, err := backendfile.New(t.TempDir())
	require.NoError(t, err)
	e, _ := newFaultWALEngine(t, 0)
	e.cfg.Backend, e.cfg.SeparateSeries = files, true
	e.cfg.DiskSeries, e.cfg.MetricSeriesChunks = true, true
	e.cfg.AsyncPressureFlush, e.cfg.PressureGenerations, e.cfg.HeadRotationBytes = true, 2, 256<<20
	p, err := newPartStreamWriterAt(t.Context(), e, "ordinal", compressProfile{}, 0, false, true, nil)
	require.NoError(t, err)
	defer p.abort()
	const count = seriesMetadataMemoryLimit + 2
	for i := range count {
		id := chunk.U128{Hi: 1, Lo: uint64(i)}
		require.NoError(t, p.appendSeries(id, []int64{1}, []float64{2}, nil, 3))
		require.NoError(t, p.appendSeries(id, []int64{2}, []float64{3}, nil, int64(i+4)))
	}
	require.NotNil(t, p.ordinal)
	require.Nil(t, p.meta)
	require.Len(t, p.covered, 1)
	require.Len(t, p.stats, 1)
	require.NoError(t, block.WriteStreamPartWithScratch(t.Context(), files, p.prefix, p.w, nil))
	require.NoError(t, p.writeSeriesSidecars(t.Context()))
	keys, err := files.List(t.Context(), p.prefix+"/")
	require.NoError(t, err)
	require.NotContains(t, keys, p.prefix+"/.series-meta")
	part, err := openPart(t.Context(), files, p.prefix, e.cfg.Obs.Corruption)
	require.NoError(t, err)
	t.Cleanup(part.closeReaders)
	r, err := part.openPartWatermarks(t.Context())
	require.NoError(t, err)
	require.True(t, r.Ordinal())
	statsRaw, err := files.Read(t.Context(), statsKey(p.prefix))
	require.NoError(t, err)
	stats, err := part.decodePartStats(t.Context(), statsRaw)
	require.NoError(t, err)
	require.Len(t, stats, count)
	require.Len(t, statsRaw, 4+count*25+sidecar.OrdinalFooterBytes)
	cursor := r.Cursor(t.Context())
	for _, i := range []int{0, count / 2, count - 1} {
		id := signal.SeriesID{Hi: 1, Lo: uint64(i)}
		ent, err := cursor.AtSeries(i, id)
		require.NoError(t, err)
		require.Equal(t, watermark.Entry{ID: id, Max: int64(i + 4)}, ent)
		require.Equal(t, SeriesAgg{Count: 2, Rows: 2, Sum: 5, Min: 2, Max: 3}, stats[id])
	}
	ids := []signal.SeriesID{{Hi: 1, Lo: 1}, {Hi: 1, Lo: count - 1}}
	var visited []watermark.Entry
	require.NoError(t, part.visitSharedDiskMetadata(t.Context(), ids, &watermark.Cursor{},
		func(id signal.SeriesID, meta seriesindex.Metadata) {
			visited = append(visited, watermark.Entry{ID: id, Max: meta.Covered})
		}))
	require.Equal(t, []watermark.Entry{{ID: ids[0], Max: 5}, {ID: ids[1], Max: count + 3}}, visited)

	part.index.paged.crc[0] ^= 1
	_, err = part.openPartWatermarks(t.Context())
	require.ErrorIs(t, err, watermark.ErrCorrupt)
	_, err = part.decodePartStats(t.Context(), statsRaw)
	require.ErrorIs(t, err, errStatsCorrupt)
}
