package engine

import (
	"context"
	"os"
	"strings"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/internal/vfs/faultfs"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

type pressureGateBackend struct {
	backend.Backend
	entered, resume chan struct{}
	once            sync.Once
}

func (b *pressureGateBackend) ReadAt(ctx context.Context, key string, offset, n int64) ([]byte, error) {
	return backend.ReadAt(ctx, b.Backend, key, offset, n)
}

func (b *pressureGateBackend) NativeSize(ctx context.Context, key string) (int64, bool, error) {
	return backend.NativeSizeOf(ctx, b.Backend, key)
}

func pressureTestEngine(t *testing.T) (*Engine, *faultfs.FS, *pressureGateBackend, func()) {
	t.Helper()
	e, fs := newFaultWALEngine(t, 10)
	be := &pressureGateBackend{Backend: backend.Memory(), entered: make(chan struct{}), resume: make(chan struct{})}
	resume := sync.OnceFunc(func() { close(be.resume) })
	e.cfg.Backend, e.cfg.Prefix = be, "pressure-contract"
	e.cfg.DiskSeries, e.cfg.AsyncPressureFlush = true, true
	e.cfg.ResidentHeadBytes = 1 << 30
	e.head.disk = newDiskHeadState()
	for i := range 64 {
		_, err := e.Append(benchSeries(i), 100, float64(i))
		require.NoError(t, err)
	}
	e.cfg.ResidentHeadBytes = 2 * (e.head.residentIdentityBytes() + e.head.inFlightBytes())
	require.NoError(t, e.flushResidentHead())
	<-be.entered
	return e, fs, be, resume
}

func TestPressurePipelineBackpressuresAtAggregateBudget(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		e, _, _, resume := pressureTestEngine(t)
		defer resume()
		series := make([]signal.Series, 128)
		ids := make([]signal.SeriesID, len(series))
		ts, values := make([]int64, len(series)), make([]float64, len(series))
		for i := range series {
			series[i], ts[i] = benchSeries(i+1000), 200
			ids[i] = series[i].Hash()
		}
		result, err := e.AppendBatch(ids, ts, values, nil, func(i int) signal.Series { return series[i] }, AppendLimits{})
		require.NoError(t, err)
		require.Equal(t, len(ids), result.Accepted)
		done := make(chan parallelAppendResult, 1)
		fresh := stagedTestSeries("full-budget")
		go func() { done <- appendColdTest(e, fresh, 300, AppendLimits{}, func() signal.Series { return fresh }) }()
		synctest.Wait()
		select {
		case <-done:
			t.Error("a full aggregate head budget must apply backpressure")
		default:
		}
		resume()
		synctest.Wait()
		r := <-done
		require.NoError(t, r.err)
		require.Equal(t, 1, r.result.Accepted)
		require.NoError(t, e.Flush(t.Context()))
		it, err := e.Fetch(t.Context(), fetch.Request{Start: 0, End: 400})
		require.NoError(t, err)
		all, err := fetch.Drain(t.Context(), it)
		require.NoError(t, err)
		require.Len(t, all, 64+len(ids)+1)
		require.NoError(t, e.Close(t.Context()))
	})
}

func TestCloseWALCancelsPressureBuildWithoutCheckpoint(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		e, fs, _, resume := pressureTestEngine(t)
		defer resume()
		done := make(chan error, 1)
		go func() { done <- e.CloseWAL() }()
		synctest.Wait()
		require.NoError(t, <-done)
		require.Equal(t, 64, e.HeadSampleCount(), "aborted generation must remain replayable")
		recovered := replayedHead(t, fs)
		require.Equal(t, 64, len(recovered.samples))
		s := stagedTestSeries("after-close")
		r := appendColdTest(e, s, 300, AppendLimits{}, func() signal.Series { t.Error("closed engine materialized identity"); return s })
		require.ErrorIs(t, r.err, os.ErrClosed)
	})
}

func (b *pressureGateBackend) Write(ctx context.Context, key string, data []byte) error {
	var err error
	if strings.HasSuffix(key, "/identity") {
		b.once.Do(func() {
			close(b.entered)
			select {
			case <-b.resume:
			case <-ctx.Done():
				err = ctx.Err()
			}
		})
	}
	if err != nil {
		return err
	}
	return b.Backend.Write(ctx, key, data)
}

func TestActiveIngestionProgressesDuringFrozenBuild(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		e, _ := newFaultWALEngine(t, 10)
		be := &pressureGateBackend{Backend: backend.Memory(), entered: make(chan struct{}), resume: make(chan struct{})}
		resume := sync.OnceFunc(func() { close(be.resume) })
		defer resume()
		e.cfg.Backend, e.cfg.Prefix = be, "pipeline"
		e.cfg.DiskSeries, e.cfg.AsyncPressureFlush = true, true
		e.cfg.ResidentHeadBytes = 1 << 30
		e.head.disk = newDiskHeadState()
		for i := range 64 {
			_, err := e.Append(benchSeries(i), 100, float64(i))
			require.NoError(t, err)
		}
		e.mu.RLock()
		e.cfg.ResidentHeadBytes = 2 * (e.head.residentIdentityBytes() + e.head.inFlightBytes())
		e.mu.RUnlock()
		require.NoError(t, e.flushResidentHead())
		synctest.Wait()
		started := false
		select {
		case <-be.entered:
			started = true
		default:
		}
		if started {
			done := make(chan parallelAppendResult, 1)
			fresh := stagedTestSeries("active-while-frozen")
			go func() { done <- appendColdTest(e, fresh, 200, AppendLimits{}, func() signal.Series { return fresh }) }()
			synctest.Wait()
			select {
			case r := <-done:
				require.NoError(t, r.err)
				require.Equal(t, 1, r.result.Accepted)
			default:
				resume()
				synctest.Wait()
				require.NoError(t, (<-done).err)
				t.Error("active admission waited behind an unrelated frozen generation despite free resident space")
			}
		}
		resume()
		synctest.Wait()
		require.NoError(t, e.Close(t.Context()))
		require.True(t, started, "pressure must rotate in the background before the full resident budget is consumed")
	})
}
