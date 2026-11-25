package engine

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/internal/memlimit"
	"github.com/oteldb/storage/internal/obs/obstest"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

func TestAutomaticPressureSecondGeneration(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		const headLimit = 512 << 10
		e, _ := newFaultWALEngine(t, 10)
		observability, _ := obstest.New(t)
		firstRotation := &automaticRotationGate{Tracer: observability.Tracer, entered: make(chan struct{}), resume: make(chan struct{})}
		observability.Tracer = firstRotation
		releaseRotation := sync.OnceFunc(func() { close(firstRotation.resume) })
		e.cfg.Obs = observability
		firstBuild, resumeBuild := make(chan struct{}), make(chan struct{})
		resume := sync.OnceFunc(func() { close(resumeBuild) })
		var blockOldest sync.Once
		e.pressure.beforeBuild = func(epoch uint64) {
			e.mu.RLock()
			oldest := e.pressureGenerations[0].epoch == epoch
			e.mu.RUnlock()
			if oldest {
				blockOldest.Do(func() {
					close(firstBuild)
					<-resumeBuild
				})
			}
		}
		e.cfg.Backend, e.cfg.Prefix = backend.Memory(), "automatic-generations"
		e.cfg.DiskSeries, e.cfg.AsyncPressureFlush, e.cfg.PressureGenerations = true, true, 2
		e.cfg.ResidentHeadBytes = headLimit
		pool := memlimit.NewPool(64 << 20)
		e.cfg.FlushMemoryBytes = 16 << 20
		e.cfg.FlushAdmission, e.cfg.TryFlushAdmission = pool.Acquire, pool.TryAcquire
		e.head.disk = newDiskHeadState()
		var maxGrowth, accepted atomic.Int64
		var snapshot fetch.Iterator
		var snapshotCount int
		done := make(chan error, 1)
		go func() {
			const batch = 16
			for base := 0; base < 8192; base += batch {
				ids := make([]signal.SeriesID, batch)
				series := make([]signal.Series, batch)
				ts, values := make([]int64, batch), make([]float64, batch)
				for i := range ids {
					series[i] = benchSeries(base + i)
					ids[i] = series[i].Hash()
					ts[i] = 100
					values[i] = 1
				}
				before := automaticInputBytes(e)
				result, err := e.AppendBatch(ids, ts, values, nil, func(i int) signal.Series { return series[i] }, AppendLimits{})
				if err != nil {
					done <- err
					return
				}
				if result.Accepted != batch {
					t.Errorf("accepted=%d want=%d", result.Accepted, batch)
				}
				accepted.Add(int64(result.Accepted))
				growth := automaticInputBytes(e) - before
				if growth > maxGrowth.Load() {
					maxGrowth.Store(growth)
				}
			}
			done <- nil
		}()
		defer func() {
			releaseRotation()
			resume()
			require.NoError(t, <-done)
			require.NoError(t, e.Close(t.Context()))
			require.Zero(t, e.pressure.generationBuilds.Load())
			release, ok := pool.TryAcquire(pool.Total())
			require.True(t, ok, "both admitted builders return all workspace credits")
			release()
			require.LessOrEqual(t, e.PartCount(), 64, "the fixed fixture must not produce a part per small input batch")
			if snapshot != nil {
				batches, err := fetch.Drain(t.Context(), snapshot)
				require.NoError(t, err)
				require.Len(t, batches, snapshotCount, "pinned windows survive generation retirement and arena reuse")
				for _, b := range batches {
					require.Equal(t, []int64{100}, b.Timestamps)
					require.Equal(t, []float64{1}, b.Values)
				}
			}
		}()
		<-firstRotation.entered
		synctest.Wait()
		beforeRotation := automaticInputBytes(e)
		t.Logf("before rotation input=%d target=%d largest growth=%d", beforeRotation, headLimit, maxGrowth.Load())
		require.LessOrEqual(t, beforeRotation, int64(headLimit/2)+maxGrowth.Load(), "scheduling delay must not consume the next generation's allowance")
		releaseRotation()
		<-firstBuild
		synctest.Wait()
		e.mu.RLock()
		frozen := len(e.pressureGenerations)
		ready := frozen == 2 && e.pressureGenerations[1].ready
		e.mu.RUnlock()
		require.Equal(t, 2, frozen)
		require.True(t, ready, "the later generation may finish multiple parts before A publishes")
		require.Zero(t, e.PartCount(), "a later output cannot publish across the blocked generation")
		snapshotCount = int(accepted.Load())
		var err error
		snapshot, err = e.Fetch(t.Context(), fetch.Request{Start: 0, End: 200})
		require.NoError(t, err)
		used := automaticInputBytes(e)
		require.LessOrEqual(t, used, int64(headLimit)+maxGrowth.Load(), "only the already-admitted batch may overshoot")
		t.Logf("input=%d target=%d largest admission growth=%d", used, headLimit, maxGrowth.Load())
	})
}

func automaticInputBytes(e *Engine) int64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	n := e.head.residentIdentityBytes() + e.head.inFlightBytes() + e.parallel.pending.Load()
	for _, g := range e.pressureGenerations {
		n -= g.head.buildRetained
	}
	return n
}

func TestSecondPressureRotationNeeded(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                string
		active, used, limit int64
		want                bool
	}{
		{name: "room remains", active: 256, used: 895, limit: 1024},
		{name: "reserve eighth reached", active: 256, used: 896, limit: 1024, want: true},
		{name: "tiny active tail", active: 255, used: 1024, limit: 1024},
		{name: "full with useful active", active: 300, used: 1024, limit: 1024, want: true},
		{name: "overflow safe", active: 1 << 61, used: 1<<63 - 1, limit: 1<<63 - 1, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, secondPressureRotationNeeded(tc.active, tc.used, tc.limit))
		})
	}
}

type automaticRotationGate struct {
	trace.Tracer

	entered, resume chan struct{}
	used            atomic.Bool
}

//nolint:spancheck // The caller owns and ends the returned span.
func (g *automaticRotationGate) Start(ctx context.Context, name string, options ...trace.SpanStartOption) (context.Context, trace.Span) {
	if name == "engine.flush" && g.used.CompareAndSwap(false, true) {
		close(g.entered)
		select {
		case <-g.resume:
		case <-ctx.Done():
		}
	}
	ctx, span := g.Tracer.Start(ctx, name, options...)
	return ctx, span
}

func TestCloseWALWakesInitialRotationAdmission(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		e, fs := newFaultWALEngine(t, 10)
		observability, _ := obstest.New(t)
		gate := &automaticRotationGate{Tracer: observability.Tracer, entered: make(chan struct{}), resume: make(chan struct{})}
		observability.Tracer = gate
		e.cfg.Obs = observability
		defer close(gate.resume)
		e.cfg.Backend, e.cfg.Prefix = backend.Memory(), "close-initial-rotation"
		e.cfg.DiskSeries, e.cfg.AsyncPressureFlush, e.cfg.PressureGenerations = true, true, 2
		e.cfg.ResidentHeadBytes = 512 << 10
		e.head.disk = newDiskHeadState()
		done := make(chan error, 1)
		go func() {
			for i := range 8192 {
				if _, err := e.Append(benchSeries(i), 100, 1); err != nil {
					done <- err
					return
				}
			}
			done <- nil
		}()
		<-gate.entered
		synctest.Wait()
		accepted := e.HeadSampleCount()
		require.Positive(t, accepted)
		require.NoError(t, e.CloseWAL())
		require.ErrorIs(t, <-done, os.ErrClosed)
		require.Zero(t, e.pressure.generationBuilds.Load())
		require.Zero(t, e.flushedEpoch)
		require.Len(t, replayedHead(t, fs).samples, accepted)
	})
}
