package storage

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/query/fetch"
)

func TestWALNoneRecoversOnlyFlushedMetrics(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	root := t.TempDir()
	dataDir, walDir := filepath.Join(root, "parts"), filepath.Join(root, "wal")
	be, err := file.New(dataDir)
	require.NoError(t, err)
	open := func() *Storage {
		s, err := Open(ctx, Options{
			Backend: be, WALDir: walDir, WALSync: WALNone, FlushInterval: -1,
		})
		require.NoError(t, err)
		return s
	}

	s := open()
	_, err = s.WriteMetrics(ctx, gaugeBatch("api", "http.requests", []int64{100}, []float64{1}))
	require.NoError(t, err)
	require.NoError(t, s.Close(ctx))
	s = open()
	_, err = s.WriteMetrics(ctx, gaugeBatch("api", "http.requests", []int64{200}, []float64{2}))
	require.NoError(t, err)
	require.NoDirExists(t, walDir)
	crash(t, s)

	s = open()
	check := func(s *Storage, times []int64, values []float64) {
		it, err := s.Fetcher("default").Fetch(ctx, fetch.Request{
			Start: 0, End: 1 << 60, Matchers: []fetch.Matcher{nameMatcher("http.requests")},
		})
		require.NoError(t, err)
		batches, err := fetch.Drain(ctx, it)
		require.NoError(t, err)
		require.Len(t, batches, 1)
		require.Equal(t, times, batches[0].Timestamps)
		require.Equal(t, values, batches[0].Values)
	}
	check(s, []int64{100}, []float64{1})
	require.NoError(t, s.Close(ctx))
	require.NoDirExists(t, walDir)

	// Re-enabling WAL starts the next segment beyond the flushed catalog epoch.
	s = reopenDurable(t, dataDir, walDir)
	_, err = s.WriteMetrics(ctx, gaugeBatch("api", "http.requests", []int64{300}, []float64{3}))
	require.NoError(t, err)
	crash(t, s)
	s = reopenDurable(t, dataDir, walDir)
	t.Cleanup(func() { require.NoError(t, s.Close(context.WithoutCancel(ctx))) })
	check(s, []int64{100, 300}, []float64{1, 3})
}

func TestWALNoneRequiresDrainedJournal(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	dataDir, walDir := t.TempDir(), t.TempDir()
	s := reopenDurable(t, dataDir, walDir)
	_, err := s.WriteMetrics(ctx, gaugeBatch("api", "http.requests", []int64{100}, []float64{1}))
	require.NoError(t, err)
	crash(t, s)

	be, err := file.New(dataDir)
	require.NoError(t, err)
	opts := Options{Backend: be, WALDir: walDir, WALSync: WALNone, FlushInterval: -1}
	_, err = Open(ctx, opts)
	require.ErrorContains(t, err, "WALNone requires a drained WAL directory")

	s = reopenDurable(t, dataDir, walDir)
	require.NoError(t, s.Close(ctx))
	s, err = Open(ctx, opts)
	require.NoError(t, err)
	require.NoError(t, s.Close(ctx))
}
