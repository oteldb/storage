package storage

import (
	"context"
	"math"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/faultbackend"
	"github.com/oteldb/storage/signal/metric"
)

func TestPressureWorkerFlushesDuringBackgroundMerge(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		be := faultbackend.Wrap(backend.Memory())
		s, err := Open(t.Context(), Options{
			Backend: be, FlushInterval: int64(time.Second), FlushThresholdBytes: 16, MergeMemoryBytes: 256 << 20,
		})
		require.NoError(t, err)
		defer func() { require.NoError(t, s.Close(context.Background())) }()
		eng, err := s.engineFor("default")
		require.NoError(t, err)
		seed := gaugeBatch("api", "m", []int64{1}, []float64{1})
		metric.Project(seed, func(b *metric.Batch) {
			for round := range 2 {
				for row := range 16 {
					_, err := eng.Append(b.Series(0), int64(round*16+row+1), float64(row))
					require.NoError(t, err)
				}
				require.NoError(t, eng.Flush(t.Context()))
			}
		})
		gate := faultbackend.NewGate()
		release := sync.OnceFunc(gate.Release)
		defer release()
		be.Add(gate.Rule(faultbackend.Read, func(op faultbackend.Op) bool { return strings.Contains(op.Key, "/c/") }))
		time.Sleep(time.Second) // Virtual time advances the real maintenance loop's ticker.
		gate.Await(t)
		be.Reset()
		_, err = s.WriteMetrics(t.Context(), gaugeBatch("api", "m", []int64{100}, []float64{100}))
		require.NoError(t, err)
		synctest.Wait()
		assert.Equal(t, 3, eng.PartCount(), "pressure wake is serviced while background maintenance is blocked")
		assert.Positive(t, s.maintStats.pressureFlushes.Load())
		release()
	})
}

func TestOversizedFlushRunsAlone(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		s, err := InMemory(WithMergeMemory(128 << 20))
		require.NoError(t, err)
		defer func() { require.NoError(t, s.Close(context.Background())) }()
		releaseMerge, ok, err := s.admitMerge(t.Context(), 64<<20, false)
		require.NoError(t, err)
		require.True(t, ok)
		flushed := make(chan func(), 1)
		go func() {
			release, err := s.flushAdmission()(t.Context(), math.MaxInt64)
			if err != nil {
				flushed <- nil
				return
			}
			flushed <- release
		}()
		synctest.Wait()
		assert.Equal(t, 1, s.buildPool.Waiting())
		_, ok, err = s.admitMerge(t.Context(), 1, false)
		require.NoError(t, err)
		assert.False(t, ok, "a queued flush prevents new merges consuming its reservation")
		releaseMerge()
		releaseFlush := <-flushed
		require.NotNil(t, releaseFlush)
		_, ok, err = s.admitMerge(t.Context(), 1, false)
		require.NoError(t, err)
		assert.False(t, ok, "an oversized flush owns the whole build pool")
		releaseFlush()
	})
}

func TestCloseCancelsQueuedPressureAdmission(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		s, err := Open(t.Context(), Options{
			Backend: backend.Memory(), FlushInterval: int64(time.Hour), FlushThresholdBytes: 16,
			MergeMemoryBytes: 128 << 20,
		})
		require.NoError(t, err)
		held, err := s.buildPool.Acquire(t.Context(), s.buildPool.Total())
		require.NoError(t, err)
		defer held()
		_, err = s.WriteMetrics(t.Context(), gaugeBatch("api", "m", []int64{1}, []float64{1}))
		require.NoError(t, err)
		synctest.Wait()
		assert.Equal(t, 1, s.buildPool.Waiting())

		// The final synchronous flush cannot acquire the externally held budget either.
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		require.ErrorIs(t, s.Close(ctx), context.Canceled)
		assert.Zero(t, s.buildPool.Waiting(), "Close drains its pressure worker and canceled reservation")
	})
}
