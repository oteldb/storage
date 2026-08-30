package router_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/oteldb/storage/cluster/router"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// openRouterTraced is [openRouter], but the Router reports through a tracer backed by rec — the
// hedge/failover spans this file asserts on. It wires the tracer in exactly as an external embedder
// would: router.Config.TracerProvider is a plain [trace.TracerProvider] (no internal/obs import
// needed), unlike the *obs.Obs an embedder could never construct.
func openRouterTraced(t *testing.T, rec *tracetest.SpanRecorder, peers ...*peer) *router.Router {
	t.Helper()

	const root = "/test-trace"

	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))

	endpoint := startEtcd(t)
	for i, p := range peers {
		joinNode(t, endpoint, root, "node-"+string(rune('a'+i)), p.serve(t))
	}

	r, err := router.Open(t.Context(), router.Config{Etcd: []string{endpoint}, Root: root, RF: len(peers), TracerProvider: tp})
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close(context.WithoutCancel(t.Context())) })

	require.Eventually(t, func() bool { return len(r.Owners("acme")) == len(peers) },
		10*time.Second, 10*time.Millisecond, "router resolves every peer as an owner")

	return r
}

func endedSpans(rec *tracetest.SpanRecorder) tracetest.SpanStubs {
	return tracetest.SpanStubsFromReadOnlySpans(rec.Ended())
}

func findSpan(spans tracetest.SpanStubs, name string) (tracetest.SpanStub, bool) {
	for i := range spans {
		if spans[i].Name == name {
			return spans[i], true
		}
	}

	return tracetest.SpanStub{}, false
}

// TestHedgeOwnersSpanOnFailover pins the span [hedgeOwners] opens around the fan-out: it must
// report the owner count and how many disclaimed the shard, and — since a full-cluster
// [cluster.ErrShardAbsent] disclaim is normal control flow, not a failure — the span must not
// record an error even though every owner initially answered absent-turned-empty.
func TestHedgeOwnersSpanOnFailover(t *testing.T) {
	t.Parallel()

	rec := tracetest.NewSpanRecorder()
	r := openRouterTraced(t, rec,
		&peer{absent: true},
		&peer{series: []signal.Series{svcSeries("api"), svcSeries("web")}},
	)

	_, err := r.Series(t.Context(), signal.Log, "acme", nil, 0, 10)
	require.NoError(t, err)

	span, ok := findSpan(endedSpans(rec), "cluster.series.hedge")
	require.True(t, ok, "cluster.series.hedge span recorded")

	attrs := make(map[string]int64)
	for _, kv := range span.Attributes {
		if kv.Value.Type().String() == "INT64" {
			attrs[string(kv.Key)] = kv.Value.AsInt64()
		}
	}

	assert.Equal(t, int64(2), attrs["storage.rpc.owners"])
	assert.Equal(t, int64(1), attrs["storage.rpc.owners_absent"], "exactly one owner disclaimed the shard")
	assert.Empty(t, span.Status.Description)
	assert.Zero(t, span.Status.Code, "a failover is not a span error")

	for _, ev := range span.Events {
		assert.NotEqual(t, "exception", ev.Name, "ErrShardAbsent must not be recorded as an error event")
	}
}

// TestHedgeOwnersSpanAllAbsent pins the all-disclaim case (a read that reports empty rather than an
// error, per [hedgeOwners]'s doc comment): the span still reports every owner absent and is still
// not an error.
func TestHedgeOwnersSpanAllAbsent(t *testing.T) {
	t.Parallel()

	rec := tracetest.NewSpanRecorder()
	r := openRouterTraced(t, rec, &peer{absent: true}, &peer{absent: true})

	series, err := r.Series(t.Context(), signal.Log, "acme", nil, 0, 10)
	require.NoError(t, err)
	assert.Empty(t, series)

	span, ok := findSpan(endedSpans(rec), "cluster.series.hedge")
	require.True(t, ok)

	var owners, absent int64
	for _, kv := range span.Attributes {
		switch string(kv.Key) {
		case "storage.rpc.owners":
			owners = kv.Value.AsInt64()
		case "storage.rpc.owners_absent":
			absent = kv.Value.AsInt64()
		}
	}

	assert.Equal(t, int64(2), owners)
	assert.Equal(t, int64(2), absent)
	assert.Zero(t, span.Status.Code)
}

// TestFetcherHedgeSpan pins [Router.Fetcher]'s own hedge span, the fetch-fan-out twin of
// [hedgeOwners]: "cluster.fetch.hedge" wraps the race across owners, distinct from the per-peer
// "cluster.fetch" client spans nested under it.
func TestFetcherHedgeSpan(t *testing.T) {
	t.Parallel()

	rec := tracetest.NewSpanRecorder()
	r := openRouterTraced(t, rec, &peer{series: []signal.Series{svcSeries("api")}})

	it, err := r.Fetcher(signal.Metric, "acme").Fetch(t.Context(), fetch.Request{Tenant: "acme", End: 10})
	require.NoError(t, err)

	batches, err := fetch.Drain(t.Context(), it)
	require.NoError(t, err)
	assert.NotEmpty(t, batches)

	hedge, ok := findSpan(endedSpans(rec), "cluster.fetch.hedge")
	require.True(t, ok, "cluster.fetch.hedge span recorded")
	assert.Zero(t, hedge.Status.Code)

	_, ok = findSpan(endedSpans(rec), "cluster.fetch")
	assert.True(t, ok, "the per-peer cluster.fetch client span is also recorded")
}
