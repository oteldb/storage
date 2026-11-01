package storage

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/faultbackend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/metric"
)

func pausedPressureFlush(t *testing.T, extraIdentities int) (*Storage, *engine.Engine, func()) {
	t.Helper()
	be := faultbackend.Wrap(backend.Memory())
	s, err := Open(t.Context(), Options{
		Backend: be, FlushInterval: int64(time.Second), FlushThresholdBytes: 32, MergeMemoryBytes: 256 << 20,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close(context.Background())) })
	eng, err := s.engineFor("default")
	require.NoError(t, err)
	seed := gaugeBatch("api", "coalesced", []int64{1}, []float64{1})
	metric.Project(seed, func(batch *metric.Batch) {
		for generation := range 2 {
			for row := range 16 {
				_, err := eng.Append(batch.Series(0), int64(generation*16+row+1), float64(row))
				require.NoError(t, err)
			}
			for identity := range extraIdentities {
				series := batch.Series(0)
				series.Attributes = signal.NewAttributes(signal.KeyValue{Key: []byte("host"),
					Value: signal.StringValue([]byte(strconv.Itoa(identity)))})
				_, err := eng.Append(series, int64(generation+1), float64(identity))
				require.NoError(t, err)
			}
			require.NoError(t, eng.Flush(t.Context()))
		}
	})
	gate := faultbackend.NewGate()
	release := sync.OnceFunc(gate.Release)
	t.Cleanup(release)
	be.Add(gate.Rule(faultbackend.Write, func(op faultbackend.Op) bool { return strings.Contains(op.Key, "/c/") }))
	_, err = s.WriteMetrics(t.Context(), gaugeBatch("api", "coalesced", []int64{100, 101}, []float64{100, 101}))
	require.NoError(t, err)
	gate.Await(t)
	be.Reset()
	metric.Project(seed, func(batch *metric.Batch) {
		_, err := eng.Append(batch.Series(0), 102, 102)
		require.NoError(t, err)
	})
	return s, eng, release
}

func TestBackgroundMaintenanceMergesDuringPressureFlush(t *testing.T) {
	t.Parallel()
	for _, identities := range []int{0, 4096} {
		t.Run(strconv.Itoa(identities), func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				s, eng, release := pausedPressureFlush(t, identities)
				time.Sleep(time.Second) // Advance the real maintenance ticker in virtual time.
				synctest.Wait()
				assert.EqualValues(t, 1, s.maintStats.cycles.Load(), "background maintenance must finish before the paused pressure flush")
				assert.Equal(t, 1, eng.PartCount(), "the two immutable parts merge while the pressure flush remains paused")
				assert.EqualValues(t, 1, eng.Stats().HeadSamples, "coalescing does not detach the newly active head")
				release()
				synctest.Wait()
				rows, err := fetch.Drain(t.Context(), must(s.Fetcher("default").Fetch(t.Context(), fetch.Request{Start: 0, End: 1000})))
				require.NoError(t, err)
				require.Len(t, rows, identities+1)
				var samples int
				for _, batch := range rows {
					samples += len(batch.Timestamps)
				}
				assert.Equal(t, 35+identities*2, samples)
			})
		})
	}
}

func TestManualMaintenanceWaitsForPressureFlush(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		s, eng, release := pausedPressureFlush(t, 0)
		done := make(chan error, 1)
		started := make(chan struct{})
		go func() {
			close(started)
			done <- s.Admin().MaintainNow(t.Context())
		}()
		<-started
		release()
		require.NoError(t, <-done)
		assert.Zero(t, eng.Stats().HeadSamples, "explicit maintenance flushes the active head too")
	})
}

func TestBackgroundMaintenanceFlushesLowRateHead(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		s, err := Open(t.Context(), Options{
			Backend: backend.Memory(), FlushInterval: int64(30 * time.Second), FlushThresholdBytes: 1 << 20,
		})
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, s.Close(context.Background())) })
		_, err = s.WriteMetrics(t.Context(), gaugeBatch("api", "low_rate", []int64{1}, []float64{1}))
		require.NoError(t, err)
		eng, err := s.engineFor("default")
		require.NoError(t, err)
		time.Sleep(29 * time.Second)
		assert.Zero(t, eng.PartCount())
		time.Sleep(time.Second)
		synctest.Wait()
		assert.Equal(t, 1, eng.PartCount(), "the unchanged 30s cadence flushes below-threshold heads")
	})
}
