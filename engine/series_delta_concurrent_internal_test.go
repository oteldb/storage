package engine

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
)

func TestConcurrentSeriesDeltasReuseDescriptors(t *testing.T) {
	t.Parallel()
	e, _ := newFaultWALEngine(t, 10)
	t.Cleanup(func() { require.NoError(t, e.CloseWAL()) })
	e.cfg.Backend, e.cfg.Prefix = backend.Memory(), "shared-descriptor-race"
	e.head.registerSeries(benchSeries(0))
	entries := identitySnapshot{view: e.head.series.View(), refs: []uint32{0}}

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
	known, err := e.seriesTable.knownIdentities(t.Context(), entries)
	require.NoError(t, err)
	require.Equal(t, []bool{true}, known)
}
