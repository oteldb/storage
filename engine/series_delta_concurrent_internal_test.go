package engine

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	backendfile "github.com/oteldb/storage/backend/file"
)

func TestConcurrentSeriesDeltasReuseDescriptors(t *testing.T) {
	t.Parallel()
	t.Run("buffered", func(t *testing.T) {
		t.Parallel()
		testConcurrentSeriesDeltas(t, backend.Memory())
	})
	t.Run("spill", func(t *testing.T) {
		t.Parallel()
		files, err := backendfile.New(t.TempDir())
		require.NoError(t, err)
		testConcurrentSeriesDeltas(t, files)
	})
}

func testConcurrentSeriesDeltas(t *testing.T, be backend.Backend) {
	t.Helper()
	e, _ := newFaultWALEngine(t, 10)
	t.Cleanup(func() { require.NoError(t, e.CloseWAL()) })
	e.cfg.Backend, e.cfg.Prefix = be, "shared-descriptor-race"
	e.cfg.FlushMemoryBytes = 1
	entries := identityCacheInput(seriesSpillFanIn + 1)

	captured := make(chan struct{}, 2)
	start := make(chan struct{})
	done := make(chan error, 2)
	for range 2 {
		go func() {
			wait := sync.OnceFunc(func() {
				captured <- struct{}{}
				<-start
			})
			done <- e.ensureSeriesIdentities(t.Context(), entries, func(int64) error {
				wait()
				return nil
			})
		}()
	}
	<-captured
	<-captured
	close(start)
	for range 2 {
		require.NoError(t, <-done)
	}
	require.Len(t, e.seriesTable.roots, 1)
	known, _, err := e.seriesTable.knownIdentities(t.Context(), entries)
	require.NoError(t, err)
	require.Len(t, known, len(entries.refs))
	require.NotContains(t, known, false)
	keys, err := be.List(t.Context(), e.cfg.Prefix+"/")
	require.NoError(t, err)
	for _, key := range keys {
		require.NotContains(t, key, "/.postings/")
	}
}
