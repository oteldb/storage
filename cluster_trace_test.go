package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/query/fetch"
)

// msgsWithTraceID returns the messages of every recorded log line carrying trace_id == id.
func msgsWithTraceID(logs *observer.ObservedLogs, id string) []string {
	var msgs []string
	all := logs.All()
	for i := range all {
		if all[i].ContextMap()["trace_id"] == id {
			msgs = append(msgs, all[i].Message)
		}
	}

	return msgs
}

// TestClusterTracePropagationCorrelatesLogs verifies the distributed-tracing contract end to end:
// a read issued on a non-owner under an active span fans out to an owner over the cluster transport,
// and BOTH nodes' logs carry the same trace_id — i.e. W3C trace context propagated across the
// node-to-node HTTP boundary and the zctx-plumbed logger stamped it on each side.
//
//nolint:paralleltest // owns an embedded etcd and sets the global propagator; runs serially
func TestClusterTracePropagationCorrelatesLogs(t *testing.T) {
	// The embedder owns the propagator; emulate that (W3C trace-context) and restore it after.
	prev := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTextMapPropagator(prev) })

	endpoint := startEtcd(t)
	ctx := context.Background()

	ids := []string{"node-a", "node-b", "node-c"}
	logsByID := make(map[string]*observer.ObservedLogs, len(ids))
	nodes := make(map[string]*Storage, len(ids))

	for _, id := range ids {
		core, recorded := observer.New(zap.DebugLevel)
		logsByID[id] = recorded
		nodes[id] = openClusterNodeWith(t, endpoint, id, backend.Memory(), WithLogger(zap.New(core)))
	}

	a := nodes["node-a"]
	require.Eventually(t, func() bool {
		return len(a.cluster.membership.Members()) == 3
	}, 10*time.Second, 50*time.Millisecond)

	_, err := a.WriteMetrics(ctx, gaugeBatch("api", "http.requests", []int64{100, 200}, []float64{1, 2}))
	require.NoError(t, err)

	// Pick a non-owner as the requester so the read must fan out to an owner over the transport.
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

	require.NotEmpty(t, requesterID, "exactly one non-owner exists with RF=2 over 3 nodes")
	require.Len(t, ownerIDs, 2)

	// A valid, sampled span context — what the embedder's tracer would have produced for the query.
	traceID := trace.TraceID{0x0b, 0x1c, 0x2d, 0x3e, 0x4f, 0x50, 0x61, 0x72, 0x83, 0x94, 0xa5, 0xb6, 0xc7, 0xd8, 0xe9, 0xfa}
	spanID := trace.SpanID{0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88}
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: traceID, SpanID: spanID, TraceFlags: trace.FlagsSampled,
	})
	rctx := trace.ContextWithSpanContext(ctx, sc)

	it, err := nodes[requesterID].Fetcher("default").Fetch(rctx, fetch.Request{
		Start: 0, End: 1 << 60, Matchers: []fetch.Matcher{nameMatcher("http.requests")},
	})
	require.NoError(t, err)
	_, err = fetch.Drain(rctx, it)
	require.NoError(t, err)

	want := traceID.String()

	// The requester logged its query boundary under the active span.
	reqMsgs := msgsWithTraceID(logsByID[requesterID], want)
	t.Logf("requester %s correlated lines (trace_id=%s): %v", requesterID, want, reqMsgs)
	assert.NotEmpty(t, reqMsgs, "requester %s logs carry the trace_id", requesterID)

	// The fan-out reaches one owner (failover picks the first reachable); whichever served must
	// have logged under the SAME trace — propagation across the node-to-node read RPC worked.
	served := 0
	for _, id := range ownerIDs {
		if ownerMsgs := msgsWithTraceID(logsByID[id], want); len(ownerMsgs) > 0 {
			served++
			t.Logf("owner %s correlated lines (trace_id=%s): %v", id, want, ownerMsgs)
		}
	}

	assert.Positive(t, served, "an owner (%v) served the fan-out and logged the propagated trace_id %s", ownerIDs, want)
}
