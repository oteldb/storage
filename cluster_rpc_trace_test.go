package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/query/fetch"
)

// TestClusterFetchFanOutSpans pins the gap this instrumentation closes: before it, a read fan-out to
// a peer left no span around the hop itself — [TestClusterTracePropagationCorrelatesLogs] shows the
// requester and owner correlate by trace_id, but neither emitted a span for the RPC. Here the
// requester must emit "cluster.fetch" (the client-side peer call) and the owner must emit
// "cluster.serve.fetch" (the server-side handler), both tagged with the signal.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestClusterFetchFanOutSpans(t *testing.T) {
	prev := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTextMapPropagator(prev) })

	endpoint := startEtcd(t)
	ctx := context.Background()

	ids := []string{"node-a", "node-b", "node-c"}
	recByID := make(map[string]*tracetest.SpanRecorder, len(ids))
	nodes := make(map[string]*Storage, len(ids))

	for _, id := range ids {
		rec := tracetest.NewSpanRecorder()
		recByID[id] = rec
		nodes[id] = openClusterNodeWith(t, endpoint, id, backend.Memory(),
			WithTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))))
	}

	a := nodes["node-a"]
	require.Eventually(t, func() bool { return ringSize(a) == 3 }, 10*time.Second, 50*time.Millisecond)

	_, err := a.WriteMetrics(ctx, gaugeBatch("api", "http.requests", []int64{100, 200}, []float64{1, 2}))
	require.NoError(t, err)

	owners := a.cluster.membership.Ring().Lookup([]byte("default"), 2)
	ownerID := map[string]bool{owners[0].ID: true, owners[1].ID: true}

	var requesterID string
	var ownerIDs []string
	for _, id := range ids {
		if ownerID[id] {
			ownerIDs = append(ownerIDs, id)
		} else {
			requesterID = id
		}
	}
	require.NotEmpty(t, requesterID)
	require.Len(t, ownerIDs, 2)

	it, err := nodes[requesterID].Fetcher("default").Fetch(ctx, fetch.Request{
		Start: 0, End: 1 << 60, Matchers: []fetch.Matcher{nameMatcher("http.requests")},
	})
	require.NoError(t, err)
	_, err = fetch.Drain(ctx, it)
	require.NoError(t, err)

	clientAttrs := spanAttrs(t, tracetest.SpanStubsFromReadOnlySpans(recByID[requesterID].Ended()), "cluster.fetch")
	assert.Equal(t, "metric", clientAttrs["storage.signal"].AsString())
	assert.NotEmpty(t, clientAttrs["storage.rpc.peer"].AsString())

	served := 0
	for _, id := range ownerIDs {
		if hasSpan(tracetest.SpanStubsFromReadOnlySpans(recByID[id].Ended()), "cluster.serve.fetch") {
			served++

			attrs := spanAttrs(t, tracetest.SpanStubsFromReadOnlySpans(recByID[id].Ended()), "cluster.serve.fetch")
			assert.Equal(t, "metric", attrs["storage.signal"].AsString())
		}
	}
	assert.Positive(t, served, "an owner served the fan-out and emitted cluster.serve.fetch")
}

func hasSpan(spans tracetest.SpanStubs, name string) bool {
	for i := range spans {
		if spans[i].Name == name {
			return true
		}
	}

	return false
}
