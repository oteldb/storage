package engine

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"
)

type marksCountingFile struct {
	*file.File

	marksReads atomic.Int64
}

func (b *marksCountingFile) Read(ctx context.Context, key string) ([]byte, error) {
	if strings.HasSuffix(key, "/marks") {
		b.marksReads.Add(1)
	}
	return b.File.Read(ctx, key)
}

func (b *marksCountingFile) ReadView(ctx context.Context, key string) ([]byte, error) {
	return b.Read(ctx, key)
}

func TestRawFlushRetainsMarksFileWAL(t *testing.T) {
	t.Parallel()
	files, err := file.New(t.TempDir())
	require.NoError(t, err)
	counted := &marksCountingFile{File: files}
	dir := t.TempDir()
	w, err := wal.Create(dir, 0)
	require.NoError(t, err)
	cfg := Config{Backend: counted, WAL: w, Prefix: "metrics", DecodeCacheBytes: 1 << 20}
	e := New(cfg)
	t.Cleanup(func() { require.NoError(t, e.CloseWAL()) })
	expected := make(map[signal.SeriesID][]float64, 32)
	for series := range 32 {
		expected[orderSeries(series).Hash()] = []float64{float64(series*128 + 20), float64(series*128 + 21), float64(series*128 + 22), float64(series*128 + 23)}
		for point := range 128 {
			_, err := e.Append(orderSeries(series), int64(100+point), float64(series*128+point))
			require.NoError(t, err)
		}
	}
	require.NoError(t, e.Flush(t.Context()))
	require.NotEmpty(t, e.parts)
	counted.marksReads.Store(0)
	for _, p := range e.parts {
		granules := e.partGranuleTimes(t.Context(), p)
		require.Len(t, granules, (p.rows()+DefaultMetricBlockRows-1)/DefaultMetricBlockRows)
	}
	require.Zero(t, counted.marksReads.Load(), "raw-flush bounds must come from the writer")
	read := func(e *Engine) {
		it, err := e.Fetch(t.Context(), fetch.Request{Start: 120, End: 123})
		require.NoError(t, err)
		rows, err := fetch.Drain(t.Context(), it)
		require.NoError(t, err)
		require.Len(t, rows, 32)
		for _, row := range rows {
			require.Equal(t, []int64{120, 121, 122, 123}, row.Timestamps)
			require.Equal(t, expected[row.ID], row.Values)
		}
	}
	read(e)
	require.Zero(t, counted.marksReads.Load())
	for series := range 32 {
		_, err := e.Append(orderSeries(series), 300, 999)
		require.NoError(t, err)
	}
	require.NoError(t, e.Flush(t.Context()))
	require.NoError(t, e.Merge(t.Context(), 0))
	require.Equal(t, 1, e.PartCount())
	counted.marksReads.Store(0)
	read(e)
	require.Zero(t, counted.marksReads.Load(), "merged outputs also reuse the writer bounds")
	require.NoError(t, e.CloseWAL())
	resumed, err := wal.Create(dir, 0)
	require.NoError(t, err)
	cfg.WAL = resumed
	restored := New(cfg)
	t.Cleanup(func() { require.NoError(t, restored.CloseWAL()) })
	require.NoError(t, restored.LoadParts(t.Context()))
	require.NoError(t, restored.Replay(t.Context(), dir))
	counted.marksReads.Store(0)
	read(restored)
	require.Positive(t, counted.marksReads.Load(), "reopened parts retain the lazy disk-backed path")
}
