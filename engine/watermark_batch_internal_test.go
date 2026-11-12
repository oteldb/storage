package engine

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
	"github.com/oteldb/storage/internal/seriesindex"
	"github.com/oteldb/storage/internal/watermark"
	"github.com/oteldb/storage/signal"
)

func watermarkBatchFixture(t *testing.T) (*Engine, *backendtest.ByteCounter, []signal.SeriesID) {
	t.Helper()
	be := backendtest.NewByteCounter(backend.Memory())
	e := New(Config{Backend: be, Prefix: "batch-watermark", DiskSeries: true, SeparateSeries: true, MetricSeriesChunks: true})
	t.Cleanup(func() { require.NoError(t, e.Close(t.Context())) })
	ids := make([]signal.SeriesID, 512)
	for i := range ids {
		s := benchSeries(i)
		ids[i] = s.Hash()
		ok, err := e.Append(s, 100, 1)
		require.NoError(t, err)
		require.True(t, ok)
	}
	require.NoError(t, e.Flush(t.Context()))
	require.Len(t, e.parts, 1)
	slices.SortFunc(ids, signal.SeriesID.Compare)
	require.NoError(t, e.parts[0].index.warm(t.Context()))
	_, err := e.parts[0].sharedWatermarkReader(t.Context())
	require.NoError(t, err)
	be.Reset()
	return e, be, ids
}

func TestSharedWatermarkBatchReadsOneWindow(t *testing.T) {
	t.Parallel()
	e, be, ids := watermarkBatchFixture(t)
	pending := make(map[signal.SeriesID]diskHeadEntry)
	require.NoError(t, lookupDiskSeries(e.parts, ids, pending))
	require.Len(t, pending, len(ids))
	for _, id := range ids {
		require.Equal(t, diskHeadEntry{exists: true, newest: 100}, pending[id])
	}
	require.EqualValues(t, 1, be.Reads())
	require.EqualValues(t, len(ids)*24, be.Bytes(), "only the fixed-width entries are read")
}

func TestSharedWatermarkBatchIgnoresAbsentIDs(t *testing.T) {
	t.Parallel()
	e, be, ids := watermarkBatchFixture(t)
	part := e.parts[0]
	part.diskWatermark = nil
	part.idBloom.Store(nil)
	require.NoError(t, be.Delete(t.Context(), watermark.Key(part.prefix)))
	absent := benchSeries(1000).Hash()
	for i := 1001; absent.Compare(ids[0]) < 0 || absent.Compare(ids[len(ids)-1]) > 0; i++ {
		absent = benchSeries(i).Hash()
	}
	pending := make(map[signal.SeriesID]diskHeadEntry)
	require.NoError(t, lookupDiskSeries(e.parts, []signal.SeriesID{absent}, pending))
	require.Empty(t, pending)
	require.Zero(t, be.Reads(), "a false positive need not open irrelevant coverage")
}

func TestSharedWatermarkBatchChecksOrdinalIdentity(t *testing.T) {
	t.Parallel()
	e, be, ids := watermarkBatchFixture(t)
	part := e.parts[0]
	entries := make([]watermark.Entry, len(ids))
	for i, id := range ids {
		entries[i] = watermark.Entry{ID: id, Max: 1000}
	}
	require.NoError(t, be.Write(t.Context(), watermark.Key(part.prefix), watermark.Encode(nil, entries)))
	part.diskWatermark = nil
	var reader seriesindex.MetadataReader
	var cursor watermark.Cursor
	newest := int64(0)
	require.NoError(t, part.visitDiskMetadataWindow(t.Context(), ids, &reader, &cursor, func(_ signal.SeriesID, meta seriesindex.Metadata) { newest = max(newest, meta.Covered) }))
	require.EqualValues(t, 1000, newest, "coverage can exceed stored sample time")
	entries[0].ID = signal.SeriesID{}
	require.NoError(t, be.Write(t.Context(), watermark.Key(part.prefix), watermark.Encode(nil, entries)))
	part.diskWatermark = nil
	err := part.visitDiskMetadataWindow(t.Context(), ids, &reader, &cursor, func(signal.SeriesID, seriesindex.Metadata) {})
	require.ErrorIs(t, err, seriesindex.ErrCorrupt)
}

func TestSharedWatermarkBatchAcrossParts(t *testing.T) {
	t.Parallel()
	e, be, ids := watermarkBatchFixture(t)
	newer := make(map[signal.SeriesID]bool)
	for i := 0; i < 512; i += 4 {
		series := benchSeries(i)
		ok, err := e.Append(series, 200, 2)
		require.NoError(t, err)
		require.True(t, ok)
		newer[series.Hash()] = true
	}
	require.NoError(t, e.Flush(t.Context()))
	require.Len(t, e.parts, 2)
	for _, part := range e.parts {
		require.NoError(t, part.index.warm(t.Context()))
		_, err := part.sharedWatermarkReader(t.Context())
		require.NoError(t, err)
	}
	be.Reset()
	pending := make(map[signal.SeriesID]diskHeadEntry)
	require.NoError(t, lookupDiskSeries(e.parts, ids, pending))
	require.Len(t, pending, 512)
	for _, id := range ids {
		want := int64(100)
		if newer[id] {
			want = 200
		}
		require.Equal(t, diskHeadEntry{exists: true, newest: want}, pending[id])
	}
	require.EqualValues(t, 2, be.Reads())
	require.EqualValues(t, (512+128)*24, be.Bytes())
}
