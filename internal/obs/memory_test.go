package obs_test

import (
	"bytes"
	"context"
	"runtime/pprof"
	runtimetrace "runtime/trace"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/oteldb/storage/internal/memlimit"
	"github.com/oteldb/storage/internal/obs"
)

type memoryFixture struct {
	o      *obs.Obs
	reader *sdkmetric.ManualReader
	spans  *tracetest.SpanRecorder
	logs   *observer.ObservedLogs
}

func newMemoryFixture(t *testing.T) memoryFixture {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	core, logs := observer.New(zap.WarnLevel)
	o, err := obs.New(obs.Config{Logger: zap.New(core), MeterProvider: mp, TracerProvider: tp})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, tp.Shutdown(context.Background()))
		require.NoError(t, mp.Shutdown(context.Background()))
	})
	return memoryFixture{o: o, reader: reader, spans: recorder, logs: logs}
}

func (f memoryFixture) value(t *testing.T, name string) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, f.reader.Collect(t.Context(), &rm))
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != name {
				continue
			}
			data, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok)
			var total int64
			for _, point := range data.DataPoints {
				require.Len(t, point.Attributes.ToSlice(), 2, "only bounded pool/operation labels")
				total += point.Value
			}
			return total
		}
	}
	return 0
}

func (f memoryFixture) checkDuration(t *testing.T, outcome string, slow bool) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, f.reader.Collect(t.Context(), &rm))
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != "storage.memory.admission.wait.duration" {
				continue
			}
			h, ok := m.Data.(metricdata.Histogram[float64])
			require.True(t, ok)
			require.Len(t, h.DataPoints, 1)
			point := h.DataPoints[0]
			require.EqualValues(t, 1, point.Count)
			value, exists := point.Attributes.Value("outcome")
			require.True(t, exists)
			require.Equal(t, outcome, value.AsString())
			require.Len(t, point.Attributes.ToSlice(), 3)
			if slow {
				require.GreaterOrEqual(t, point.Sum, 1.0)
			}
			return
		}
	}
	t.Fatal("missing memory admission duration")
}

func TestMemoryWaitLifecycle(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, outcome string
		slow          bool
		deadline      bool
	}{
		{name: "admitted", outcome: "admitted", slow: true},
		{name: "canceled", outcome: "canceled", slow: true},
		{name: "deadline", outcome: "canceled", slow: true, deadline: true},
		{name: "short-admitted", outcome: "admitted"},
		{name: "short-canceled", outcome: "canceled"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newMemoryFixture(t)
				ctx, parent := f.o.Tracer.Start(t.Context(), "ingest")
				defer parent.End()
				var cancel context.CancelFunc
				if tc.deadline {
					ctx, cancel = context.WithTimeout(ctx, 2*time.Second)
				} else {
					ctx, cancel = context.WithCancel(ctx)
				}
				defer cancel()
				pool := memlimit.NewPool(100)
				held, err := pool.Acquire(t.Context(), 100)
				require.NoError(t, err)
				defer held()
				done := make(chan error, 1)
				go func() {
					release, err := pool.AcquireObserved(ctx, 200, f.o.Memory.BuildFlush)
					if err == nil {
						release()
					}
					done <- err
				}()
				synctest.Wait()
				require.EqualValues(t, 1, f.value(t, "storage.memory.admission.waiters"))
				require.EqualValues(t, 100, f.value(t, "storage.memory.admission.waiting_bytes"))
				require.Zero(t, f.logs.Len())
				var goroutines bytes.Buffer
				require.NoError(t, pprof.Lookup("goroutine").WriteTo(&goroutines, 1))
				require.Contains(t, goroutines.String(), `"storage.memory.pool":"build"`)
				require.Contains(t, goroutines.String(), `"storage.memory.operation":"flush"`)
				if tc.slow {
					time.Sleep(time.Second) // Virtual time; admission remains blocked.
					synctest.Wait()
					require.EqualValues(t, 1, f.value(t, "storage.memory.admission.slow_waiters"))
					require.EqualValues(t, 1, f.value(t, "storage.memory.admission.slow_waits"))
					require.Len(t, f.logs.All(), 1, "warn before the admission returns")
					fields := f.logs.All()[0].ContextMap()
					require.EqualValues(t, 200, fields["requested_bytes"])
					require.EqualValues(t, 100, fields["reserved_bytes"])
					require.EqualValues(t, 100, fields["used_bytes"])
					require.EqualValues(t, 0, fields["free_bytes"])
					require.EqualValues(t, 1, fields["waiters"])
					require.EqualValues(t, 100, fields["head_waiter_bytes"])
				}
				switch {
				case tc.outcome == "admitted":
					held()
				case tc.deadline:
					time.Sleep(time.Second)
				default:
					cancel()
				}
				synctest.Wait()
				err = <-done
				switch {
				case tc.outcome == "admitted":
					require.NoError(t, err)
				case tc.deadline:
					require.ErrorIs(t, err, context.DeadlineExceeded)
				default:
					require.ErrorIs(t, err, context.Canceled)
				}
				require.Zero(t, pool.Waiting())
				require.Zero(t, f.value(t, "storage.memory.admission.waiters"))
				require.Zero(t, f.value(t, "storage.memory.admission.waiting_bytes"))
				require.Zero(t, f.value(t, "storage.memory.admission.slow_waiters"))
				f.checkDuration(t, tc.outcome, tc.slow)
				time.Sleep(2 * time.Second)
				synctest.Wait()
				if tc.slow {
					require.Len(t, f.logs.All(), 2)
					require.Equal(t, tc.outcome, f.logs.All()[1].ContextMap()["outcome"])
				} else {
					require.Zero(t, f.logs.Len(), "completed waits must stop their warning timer")
				}
				spans := f.spans.Ended()
				require.Len(t, spans, 1)
				require.Equal(t, "storage.memory.wait", spans[0].Name())
				require.Equal(t, parent.SpanContext().SpanID(), spans[0].Parent().SpanID())
				attributes := make(map[string]any)
				for _, attr := range spans[0].Attributes() {
					attributes[string(attr.Key)] = attr.Value.AsInterface()
				}
				require.Equal(t, tc.outcome, attributes["storage.memory.outcome"])
				if tc.slow {
					require.Equal(t, spans[0].SpanContext().TraceID().String(), f.logs.All()[0].ContextMap()["trace_id"])
					require.Equal(t, spans[0].SpanContext().SpanID().String(), f.logs.All()[0].ContextMap()["span_id"])
				}
			})
		})
	}
}

func TestMemoryWaitLogsAreBounded(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newMemoryFixture(t)
		pool := memlimit.NewPool(100)
		held, err := pool.Acquire(t.Context(), 100)
		require.NoError(t, err)
		defer held()
		done := make(chan error, 8)
		for range 8 {
			go func() {
				release, err := pool.AcquireObserved(t.Context(), 100, f.o.Memory.BuildFlush)
				if err == nil {
					release()
				}
				done <- err
			}()
		}
		synctest.Wait()
		time.Sleep(time.Second)
		synctest.Wait()
		require.EqualValues(t, 8, f.value(t, "storage.memory.admission.slow_waits"))
		require.EqualValues(t, 8, f.value(t, "storage.memory.admission.slow_waiters"))
		require.Len(t, f.logs.All(), 1)
		held()
		synctest.Wait()
		for range 8 {
			require.NoError(t, <-done)
		}
		require.Len(t, f.logs.All(), 2)
		require.Zero(t, f.value(t, "storage.memory.admission.waiters"))
	})
}

//nolint:paralleltest // Runtime tracing is process-global.
func TestMemoryWaitRuntimeTrace(t *testing.T) {
	if runtimetrace.IsEnabled() {
		t.Skip("a Go execution trace is already active")
	}
	var data bytes.Buffer
	require.NoError(t, runtimetrace.Start(&data))
	defer runtimetrace.Stop()
	synctest.Test(t, func(t *testing.T) {
		o := obs.NewNop()
		pool := memlimit.NewPool(1)
		held, err := pool.Acquire(t.Context(), 1)
		require.NoError(t, err)
		defer held()
		done := make(chan error, 1)
		go func() {
			release, err := pool.AcquireObserved(t.Context(), 1, o.Memory.BuildFlush)
			if err == nil {
				release()
			}
			done <- err
		}()
		synctest.Wait()
		held()
		synctest.Wait()
		require.NoError(t, <-done)
	})
	runtimetrace.Stop()
	require.Contains(t, data.String(), "storage.memory.wait")
	require.Contains(t, data.String(), "build/flush")
}
