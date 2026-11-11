package engine

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
	"github.com/oteldb/storage/index/series"
	"github.com/oteldb/storage/internal/watermark"
	"github.com/oteldb/storage/signal"
)

func TestSeparateWriterOmitsPartDescriptors(t *testing.T) {
	t.Parallel()
	be := backend.Memory()
	e := New(Config{Backend: be, Prefix: "separate-writer", DiskSeries: true, SeparateSeries: true,
		MetricSeriesChunks: true, AggregateStats: true})
	w, err := newPartStreamWriter(t.Context(), e, compressProfile{}, 0, false, true, nil)
	require.NoError(t, err)
	defer w.abort()
	registry := series.New()
	s := benchSeries(1)
	id := registry.Add(s)
	ref, ok := registry.Ref(id)
	require.True(t, ok)
	require.NoError(t, w.addDiskSeries(registry.View(), ref, id))
	require.NoError(t, w.addMergedDiskIdentity(id, s, nil))
	require.Nil(t, w.seriesBuilder)
	require.NoError(t, w.appendSeries(idToU128(id), []int64{10, 20}, []float64{2, 3}, nil, 37))
	out, err := w.finishWithIdentities(t.Context(), identitySnapshot{view: registry.View(), refs: []uint32{ref}})
	require.NoError(t, err)
	defer out.closeReaders()
	require.Nil(t, w.seriesBuilder)
	require.False(t, w.streamIdentities)
	require.Zero(t, w.identityWriterBytes)
	for _, key := range []string{identityKey(out.prefix), seriesIndexKey(out.prefix)} {
		_, err := be.Read(t.Context(), key)
		require.ErrorIs(t, err, backend.ErrNotExist)
	}
	for _, key := range []string{sidxKey(out.prefix), statsKey(out.prefix)} {
		_, err := be.Read(t.Context(), key)
		require.NoError(t, err)
	}
	data, err := be.Read(t.Context(), watermark.Key(out.prefix))
	require.NoError(t, err)
	bounds, err := watermark.Decode(data)
	require.NoError(t, err)
	require.Equal(t, []watermark.Entry{{ID: id, Max: 37}}, bounds)
	require.NotNil(t, out.idBloom.Load())
}

func TestSeparateMergeIdentityUsesNumericCoverage(t *testing.T) {
	t.Parallel()
	be := backend.Memory()
	e := New(Config{Backend: be, Prefix: "source-coverage", DiskSeries: true, SeparateSeries: true, MetricSeriesChunks: true})
	s := benchSeries(1)
	for i := range 2 {
		accepted, err := e.Append(s, int64(i+1), float64(i))
		require.NoError(t, err)
		require.True(t, accepted)
		require.NoError(t, e.Flush(t.Context()))
	}
	for _, p := range e.parts {
		_, err := be.Read(t.Context(), identityKey(p.prefix))
		require.ErrorIs(t, err, backend.ErrNotExist)
		_, err = be.Read(t.Context(), seriesIndexKey(p.prefix))
		require.ErrorIs(t, err, backend.ErrNotExist)
		require.NoError(t, be.Write(t.Context(), watermark.Key(p.prefix), watermark.Encode(nil, []watermark.Entry{{ID: s.Hash(), Max: 100}})))
		p.seriesMax = nil
	}
	var keys seriesMerge
	require.NoError(t, keys.init(t.Context(), e.parts))
	var item mergeIdentityItem
	var batch mergeIdentityBatch
	next, err := item.next(t.Context(), e, &keys, &batch, e.parts, nil)
	require.NoError(t, err)
	require.True(t, next)
	require.Equal(t, s.Hash(), item.id)
	require.EqualValues(t, 100, item.covered)
	require.Equal(t, signal.Series{}, item.identity)
	next, err = item.next(t.Context(), e, &keys, &batch, e.parts, nil)
	require.NoError(t, err)
	require.False(t, next)
}

func TestSeparateNumericCompactionSkipsDescriptors(t *testing.T) {
	t.Parallel()
	be := backendtest.NewSizedByteCounter(backend.Memory())
	e := New(Config{Backend: be, Prefix: "numeric-compaction", DiskSeries: true, SeparateSeries: true, MetricSeriesChunks: true})
	s := benchSeries(1)
	for i := range 2 {
		accepted, err := e.Append(s, int64(i+1), float64(i+10))
		require.NoError(t, err)
		require.True(t, accepted)
		require.NoError(t, e.Flush(t.Context()))
	}
	be.Reset()
	parts, err := e.compactStream(t.Context(), e.parts, 64<<20, rollupPlan{}, MergeOptions{})
	require.NoError(t, err)
	require.NotEmpty(t, parts)
	t.Cleanup(func() {
		for _, p := range parts {
			p.closeReaders()
		}
	})
	require.NotContains(t, be.Report(), "/series-index")
	require.NotContains(t, be.Report(), "/identity")
	var rows int
	for _, p := range parts {
		rows += p.rows()
		for _, key := range []string{identityKey(p.prefix), seriesIndexKey(p.prefix)} {
			_, err := be.Read(t.Context(), key)
			require.ErrorIs(t, err, backend.ErrNotExist)
		}
		covered, found, err := p.seriesWatermark(t.Context(), s.Hash())
		require.NoError(t, err)
		require.True(t, found)
		require.EqualValues(t, 2, covered)
	}
	require.Equal(t, 2, rows)
}
