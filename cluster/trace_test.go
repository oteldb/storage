package cluster_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// TestWithTracerProviderIsExternallyUsable exercises the public wiring path an embedder outside
// this module actually has: cluster.WithTracerProvider takes the plain, importable
// go.opentelemetry.io/otel/trace.TracerProvider — never this module's internal/obs.Obs, which an
// external caller cannot name or construct. This file imports nothing under internal/.
func TestWithTracerProviderIsExternallyUsable(t *testing.T) {
	t.Parallel()

	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))

	fn := func(context.Context, string, int64, int64, []fetch.Matcher) ([]*fetch.Batch, error) {
		return []*fetch.Batch{{Series: signal.Series{}, Timestamps: []int64{1}, Values: []float64{1}}}, nil
	}
	noFn := func(context.Context, string, int64, int64, []fetch.Matcher) ([]*fetch.Batch, error) { return nil, nil }

	mux := http.NewServeMux()
	mux.Handle(cluster.ReadPath, cluster.ReadHandler(fn, noFn, noFn, noFn, cluster.WithTracerProvider(tp)))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	addr := strings.TrimPrefix(srv.URL, "http://")

	it, err := cluster.NewRemoteFetcher(signal.Metric, addr, nil, cluster.WithTracerProvider(tp)).
		Fetch(t.Context(), fetch.Request{Tenant: "acme", End: 10})
	require.NoError(t, err)

	_, err = fetch.Drain(t.Context(), it)
	require.NoError(t, err)

	ended := rec.Ended()
	names := make([]string, 0, len(ended))
	for _, s := range ended {
		names = append(names, s.Name())
	}

	assert.Contains(t, names, "cluster.fetch", "the client span reports through the public TracerProvider")
	assert.Contains(t, names, "cluster.serve.fetch", "the server span reports through the same public TracerProvider")
}
