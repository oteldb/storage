package engine

import (
	"sync"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/query/fetch"
)

func TestConfirmedGenerationRetiresBeforeCacheRefresh(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		e, _ := newReturningIDEngine(t, backend.Memory())
		e.cfg.OOOWindow = 10
		last := benchSeries(257)
		for i := range 258 {
			accepted, err := e.Append(benchSeries(i), 100, 1)
			require.NoError(t, err)
			require.True(t, accepted)
		}

		refreshing, proceed := make(chan struct{}), make(chan struct{})
		resume := sync.OnceFunc(func() { close(proceed) })
		defer resume()
		var once sync.Once
		e.parallel.hooks = &parallelHeadHooks{afterFrozenCacheBatch: func() {
			once.Do(func() { close(refreshing); <-proceed })
		}}
		done := make(chan error, 1)
		go func() { done <- e.Flush(t.Context()) }()
		<-refreshing
		synctest.Wait()

		require.Empty(t, e.pressureGenerations, "the cache warmer must free the frozen slot")
		require.Zero(t, e.HeadSampleCount(), "sample ownership has moved to immutable parts")
		require.NotNil(t, e.head.retiring)
		require.Positive(t, e.head.retiring.bytes)
		_, cached := e.head.disk.cache.get(last.Hash())
		require.False(t, cached, "the last identity is still waiting for cache refresh")
		count, err := e.Count(t.Context(), fetch.Request{Start: 100, End: 101})
		require.NoError(t, err)
		require.Equal(t, 258, count)

		accepted, err := e.Append(last, 89, 2)
		require.NoError(t, err)
		require.False(t, accepted, "an unwarmed series must still reject an out-of-order sample")
		accepted, err = e.Append(last, 200, 2)
		require.NoError(t, err)
		require.True(t, accepted)
		e.mu.Lock()
		err = e.freezePressureGenerationLocked(t.Context())
		e.mu.Unlock()
		require.NoError(t, err)
		require.Len(t, e.pressureGenerations, 1, "the next head can rotate before refresh ends")

		resume()
		require.NoError(t, <-done)
		require.Nil(t, e.head.retiring)
		require.NoError(t, e.Close(t.Context()))
	})
}
