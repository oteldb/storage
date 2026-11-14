package engine

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/oteldb/storage/internal/memlimit"
	"github.com/oteldb/storage/internal/obs"
	"github.com/oteldb/storage/internal/obs/obstest"
	"github.com/oteldb/storage/query/fetch"
)

func TestObservedDecodeBudgetForcedAdmission(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		provider, metrics := obstest.Provider(t)
		recorder := tracetest.NewSpanRecorder()
		tracer := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
		defer func() { require.NoError(t, tracer.Shutdown(t.Context())) }()
		o, err := obs.New(obs.Config{MeterProvider: provider, TracerProvider: tracer})
		require.NoError(t, err)
		budget := NewDecodeBudget(100)
		budget.forceAfter = 2 * time.Second
		_, err = budget.acquire(t.Context(), 80)
		require.NoError(t, err)
		defer budget.release(80)
		type result struct {
			forced bool
			err    error
		}
		done := make(chan result, 1)
		go func() {
			forced, err := budget.acquireObserved(t.Context(), 40, o.Memory.Decode)
			done <- result{forced: forced, err: err}
		}()
		synctest.Wait()
		require.EqualValues(t, 1, metrics.Counter("storage.memory.admission.waiters", "pool", "decode"))
		time.Sleep(time.Second)
		synctest.Wait()
		require.EqualValues(t, 1, metrics.Counter("storage.memory.admission.slow_waiters"))
		time.Sleep(time.Second)
		synctest.Wait()
		r := <-done
		require.NoError(t, r.err)
		require.True(t, r.forced)
		require.EqualValues(t, 120, budget.inFlight())
		require.Zero(t, metrics.Counter("storage.memory.admission.waiters"))
		require.Zero(t, metrics.Counter("storage.memory.admission.slow_waiters"))
		spans := recorder.Ended()
		require.Len(t, spans, 1)
		found := false
		for _, a := range spans[0].Attributes() {
			if a.Key == "storage.memory.outcome" {
				require.Equal(t, "forced", a.Value.AsString())
				found = true
			}
		}
		require.True(t, found)
		budget.release(40)
	})
}

func TestObservedDecodeBudgetScopedGrowthDoesNotWait(t *testing.T) {
	t.Parallel()
	budget := NewDecodeBudget(100)
	scope := fetch.NewScope()
	observer := func(_ context.Context, _ memlimit.Wait, wait func() (bool, error)) (bool, error) {
		t.Error("uncontended admission and scoped growth must not enter wait instrumentation")
		return wait()
	}
	for range 2 {
		forced, err := budget.acquireForObserved(t.Context(), scope, 80, observer)
		require.NoError(t, err)
		require.False(t, forced)
	}
	require.EqualValues(t, 160, budget.inFlight())
	require.Zero(t, budget.waiting())
	budget.releaseFor(scope, 160)
	require.Zero(t, budget.inFlight())
}
