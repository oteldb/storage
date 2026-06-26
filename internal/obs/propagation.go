package obs

import (
	"context"
	"net/http"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// InjectHTTP writes the trace context from ctx into the request headers, so a node-to-node RPC
// carries the distributed trace. It uses the globally-configured OTel propagator (set by the
// embedder, e.g. propagation.TraceContext{}); the default is a no-op that writes nothing, so an
// unconfigured store propagates nothing at no cost.
func InjectHTTP(ctx context.Context, h http.Header) {
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(h))
}

// ExtractHTTP returns ctx augmented with the trace context read from the request headers, so a
// receiving handler's spans (and the engine spans below it) join the caller's trace. With the
// default no-op propagator it returns ctx unchanged.
func ExtractHTTP(ctx context.Context, h http.Header) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, propagation.HeaderCarrier(h))
}
