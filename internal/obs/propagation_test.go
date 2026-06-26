package obs_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/oteldb/storage/internal/obs"
)

// TestInjectExtractRoundTrip verifies that, with a W3C propagator configured, a trace context
// injected into outgoing headers is recovered by the receiving side — so a clustered RPC keeps one
// trace.
//
//nolint:paralleltest // sets the global propagator
func TestInjectExtractRoundTrip(t *testing.T) {
	otel.SetTextMapPropagator(propagation.TraceContext{})

	tid, err := trace.TraceIDFromHex("0123456789abcdef0123456789abcdef")
	require.NoError(t, err)
	sid, err := trace.SpanIDFromHex("0123456789abcdef")
	require.NoError(t, err)

	sc := trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: sid, TraceFlags: trace.FlagsSampled})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)

	h := http.Header{}
	obs.InjectHTTP(ctx, h)
	assert.NotEmpty(t, h.Get("Traceparent"), "traceparent injected")

	got := trace.SpanContextFromContext(obs.ExtractHTTP(context.Background(), h))
	assert.Equal(t, tid, got.TraceID(), "trace id survives the round trip")
	assert.True(t, got.IsSampled())
}

// TestNoopPropagatorIsHarmless confirms the default (no propagator) writes nothing and extract is a
// pass-through.
//
//nolint:paralleltest // shares the global propagator with the test above
func TestNoopPropagatorIsHarmless(t *testing.T) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator()) // empty ⇒ no-op

	h := http.Header{}
	obs.InjectHTTP(context.Background(), h)
	assert.Empty(t, h.Get("Traceparent"))

	ctx := obs.ExtractHTTP(context.Background(), h)
	assert.False(t, trace.SpanContextFromContext(ctx).IsValid())
}
