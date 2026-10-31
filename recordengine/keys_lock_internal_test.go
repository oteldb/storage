package recordengine

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/oteldb/storage/internal/obs"
)

func TestKeysLockSpanPrecedesEnumeration(t *testing.T) {
	t.Parallel()
	probe := &keysLockProbe{SpanRecorder: tracetest.NewSpanRecorder(), lock: make(chan struct{}), streams: make(chan struct{})}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(probe))
	t.Cleanup(func() { require.NoError(t, tp.Shutdown(context.Background())) })
	o, err := obs.New(obs.Config{TracerProvider: tp})
	require.NoError(t, err)
	e := New(Config{Schema: headTestSchema, Obs: o})
	e.mu.LockState()
	var once sync.Once
	unlock := func() { once.Do(e.mu.UnlockState) }
	t.Cleanup(unlock)
	done := make(chan []KeyInfo, 1)
	go func() { done <- e.KeysContext(t.Context(), 0, 0) }()
	<-probe.lock
	select {
	case <-probe.streams:
		t.Fatal("enumeration started while the engine state lock was held")
	default:
	}
	for _, span := range probe.Ended() {
		require.NotEqual(t, "recordengine.keys.lock", span.Name())
	}
	unlock()
	require.Empty(t, <-done)
	<-probe.streams
	spans := map[string]sdktrace.ReadOnlySpan{}
	for _, span := range probe.Ended() {
		spans[span.Name()] = span
	}
	require.False(t, spans["recordengine.keys.streams"].StartTime().Before(spans["recordengine.keys.lock"].EndTime()))
}

type keysLockProbe struct {
	*tracetest.SpanRecorder

	lock, streams chan struct{}
}

func (p *keysLockProbe) OnStart(ctx context.Context, s sdktrace.ReadWriteSpan) {
	p.SpanRecorder.OnStart(ctx, s)
	switch s.Name() {
	case "recordengine.keys.lock":
		close(p.lock)
	case "recordengine.keys.streams":
		close(p.streams)
	}
}
