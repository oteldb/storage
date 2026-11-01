package recordengine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/internal/obs"
)

func TestEstablishTenureWithoutUnnecessaryLocks(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name                 string
		backend, term, waits bool
	}{
		{name: "head only"},
		{name: "local backend", backend: true},
		{name: "no backend with term", term: true},
		{name: "clustered backend", backend: true, term: true, waits: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			probe := &tenureLockProbe{SpanRecorder: tracetest.NewSpanRecorder(), entered: make(chan struct{})}
			tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(probe))
			t.Cleanup(func() { require.NoError(t, tp.Shutdown(context.Background())) })
			o, err := obs.New(obs.Config{TracerProvider: tp})
			require.NoError(t, err)
			cfg := Config{Prefix: "tenure-lock-test", Obs: o, Schema: headTestSchema}
			if tt.backend {
				cfg.Backend = backend.Memory()
			}
			if tt.term {
				cfg.Term = func() uint64 { return 1 }
			}
			e := New(cfg)
			e.flushMu.RLock()
			e.mu.LockState()
			locked := true
			unlock := func() {
				if locked {
					e.mu.UnlockState()
					e.flushMu.RUnlock()
					locked = false
				}
			}
			done := make(chan error, 1)
			finished := make(chan struct{})
			defer func() { unlock(); <-finished }()
			go func() {
				defer close(finished)
				done <- e.establishTenure(t.Context())
			}()
			completed, entered := false, false
			select {
			case err := <-done:
				require.NoError(t, err)
				completed = true
			case <-probe.entered:
				entered = true
			}
			require.Equal(t, tt.waits, entered, "only a durable clustered engine should enter tenure establishment")
			if entered {
				require.Empty(t, probe.Ended(), "the cluster tenure span includes the blocked locks")
			}
			unlock()
			if !completed {
				require.NoError(t, <-done)
			}
			if tt.waits {
				require.EqualValues(t, 1, e.established, "the real tenure must still be committed")
			} else {
				require.Zero(t, e.established)
				require.Empty(t, probe.Started())
			}
		})
	}
}

type tenureLockProbe struct {
	*tracetest.SpanRecorder

	entered chan struct{}
}

func (p *tenureLockProbe) OnStart(ctx context.Context, span sdktrace.ReadWriteSpan) {
	p.SpanRecorder.OnStart(ctx, span)
	if span.Name() == "recordengine.establishTenure" {
		close(p.entered)
	}
}
