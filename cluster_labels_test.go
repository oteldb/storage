package storage

import (
	"context"
	"strconv"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/metric"
)

// labelSeriesCount is how many distinct series the label tests seed. Each carries a label name only
// it has and a value only it has, so a shard dropped from the gather shows up as a missing string
// rather than passing by luck.
const labelSeriesCount = 12

// writeLabelSeries ingests labelSeriesCount gauges through s, series i carrying attributes
// {only_<i>: "1", slot: "s<i>"}. Every series shares __name__ and service.name; nothing else.
func writeLabelSeries(t *testing.T, s *Storage) (names, values []string) {
	t.Helper()

	for i := range labelSeriesCount {
		only := "only_" + strconv.Itoa(i)
		slot := "s" + strconv.Itoa(i)

		var md metric.Metrics
		rm := md.AddResource()
		rm.Resource = signal.Resource{Attributes: signal.NewAttributes(
			signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte("api"))},
		)}
		mt := rm.AddScope().AddMetric()
		mt.Name = []byte("http.requests")
		mt.Kind = metric.KindGauge
		p := mt.AddPoint()
		p.Ts = 100
		p.Value = float64(i)
		p.Attributes = signal.NewAttributes(
			signal.KeyValue{Key: []byte(only), Value: signal.StringValue([]byte("1"))},
			signal.KeyValue{Key: []byte("slot"), Value: signal.StringValue([]byte(slot))},
		)

		_, err := s.WriteMetrics(context.Background(), md)
		require.NoError(t, err)

		names = append(names, only)
		values = append(values, slot)
	}

	return names, values
}

// requireShardSpread fails unless the seeded series genuinely span more than one shard: without
// that the union assertions below would pass even if the gather asked a single shard.
func requireShardSpread(t *testing.T, s *Storage, shards int) {
	t.Helper()

	series, err := s.MetricSeries(t.Context(), "default", nil, 0, 0)
	require.NoError(t, err)
	require.Len(t, series, labelSeriesCount)

	spread := map[int]struct{}{}
	for i := range series {
		spread[shardOf(signal.HashBytes(series[i].AppendHashInput(nil)), shards)] = struct{}{}
	}

	require.Greaterf(t, len(spread), 1, "the seeded series span more than one of the %d shards", shards)
}

// requireCompleteLabels asserts that s answers the tenant's label metadata through the index-only
// [fetch.LabelLister] seam, and that the answer is the union across every shard.
func requireCompleteLabels(t *testing.T, s *Storage, node string, names, values []string) {
	t.Helper()

	lister := fetch.LabelListerOf(s.Fetcher("default"))
	require.NotNilf(t, lister, "%s exposes the label-metadata capability", node)

	r := fetch.Request{Start: 0, End: 1 << 60}

	gotNames, err := lister.LabelNames(t.Context(), r)
	require.NoErrorf(t, err, "%s label names", node)
	assert.Subsetf(t, gotNames, names, "%s unions every shard's label names", node)
	assert.Subsetf(t, gotNames, []string{"__name__", "service.name", "slot"}, "%s: the shared names", node)

	gotValues, err := lister.LabelValues(t.Context(), r, []byte("slot"))
	require.NoErrorf(t, err, "%s label values", node)
	assert.ElementsMatchf(t, values, gotValues, "%s unions every shard's values of slot", node)

	// A pushed-down matcher narrows the answer without leaving the index path.
	one, err := lister.LabelValues(t.Context(), fetch.Request{
		Start: 0, End: 1 << 60,
		Matchers: []fetch.Matcher{eqMatcher("slot", values[0])},
	}, []byte("slot"))
	require.NoErrorf(t, err, "%s matched label values", node)
	assert.Equalf(t, []string{values[0]}, one, "%s: the matcher reached the shards' indexes", node)
}

// eqMatcher builds a pushable equality matcher: Spec is what lets it cross the wire and be resolved
// against a peer's identity index.
func eqMatcher(name, value string) fetch.Matcher {
	spec := fetch.EqualMatcher{Name: name, Value: value}

	return fetch.Matcher{Name: []byte(name), Match: spec.Predicate(), Spec: &spec}
}

// TestClusteredLabelsUnionAcrossShards is the core of the label RPC: a tenant whose series live on
// several shards must answer label names and values as the union across all of them, from every
// node — the owner of some shards and of none.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestClusteredLabelsUnionAcrossShards(t *testing.T) {
	endpoint := startEtcd(t)

	const shards = 4

	nodes := map[string]*Storage{
		"node-a": openClusterNodeSharded(t, endpoint, "node-a", shards),
		"node-b": openClusterNodeSharded(t, endpoint, "node-b", shards),
		"node-c": openClusterNodeSharded(t, endpoint, "node-c", shards),
	}
	awaitMembership(t, nodes)

	names, values := writeLabelSeries(t, nodes["node-a"])
	requireShardSpread(t, nodes["node-a"], shards)

	for node, s := range nodes {
		requireCompleteLabels(t, s, node, names, values)
	}
}

// TestClusteredLabelsFailOverFromShardlessOwner covers the per-shard failover: a node the ring made
// an owner after the write holds none of the data, so it must disclaim and ask the owners that do —
// exactly as the series/keys enumeration RPCs already do — rather than answering with its own empty
// index.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestClusteredLabelsFailOverFromShardlessOwner(t *testing.T) {
	endpoint := startEtcd(t)

	const shards = 4

	nodes := map[string]*Storage{
		"node-a": openClusterNodeSharded(t, endpoint, "node-a", shards),
		"node-c": openClusterNodeSharded(t, endpoint, "node-c", shards),
	}
	awaitMembership(t, nodes)

	names, values := writeLabelSeries(t, nodes["node-a"])
	requireShardSpread(t, nodes["node-a"], shards)

	// node-b joins *after* the write: the ring re-assigns shards to it, but no data moves.
	nodes["node-b"] = openClusterNodeSharded(t, endpoint, "node-b", shards)
	awaitMembership(t, nodes)

	for node, s := range nodes {
		requireCompleteLabels(t, s, node, names, values)
	}
}

// TestClusteredLabelsTakeIndexRoute observes the route itself: a non-owner's label query must emit
// the label RPC's spans and must not enumerate identities or drain samples to answer.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestClusteredLabelsTakeIndexRoute(t *testing.T) {
	prev := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTextMapPropagator(prev) })

	endpoint := startEtcd(t)

	ids := []string{"node-a", "node-b", "node-c"}
	recByID := make(map[string]*tracetest.SpanRecorder, len(ids))
	nodes := make(map[string]*Storage, len(ids))

	for _, id := range ids {
		rec := tracetest.NewSpanRecorder()
		recByID[id] = rec
		nodes[id] = openClusterNodeWith(t, endpoint, id, backend.Memory(),
			WithTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))))
	}

	awaitMembership(t, nodes)

	_, values := writeLabelSeries(t, nodes["node-a"])

	owners := nodes["node-a"].cluster.membership.Ring().Lookup([]byte("default"), 2)
	require.Len(t, owners, 2)

	ownerID := map[string]bool{owners[0].ID: true, owners[1].ID: true}

	var requester string
	for _, id := range ids {
		if !ownerID[id] {
			requester = id
		}
	}
	require.NotEmpty(t, requester)

	lister := fetch.LabelListerOf(nodes[requester].Fetcher("default"))
	require.NotNil(t, lister)

	got, err := lister.LabelValues(t.Context(), fetch.Request{Start: 0, End: 1 << 60}, []byte("slot"))
	require.NoError(t, err)
	assert.ElementsMatch(t, values, got)

	client := tracetest.SpanStubsFromReadOnlySpans(recByID[requester].Ended())
	require.True(t, hasSpan(client, "cluster.labels"), "the requester called the label RPC")

	// The point of the RPC: no identity gather, no sample drain.
	assert.False(t, hasSpan(client, "cluster.series"), "no identity enumeration fallback")
	assert.False(t, hasSpan(client, "cluster.fetch"), "no sample drain fallback")

	served := 0

	for _, id := range ids {
		if ownerID[id] && hasSpan(tracetest.SpanStubsFromReadOnlySpans(recByID[id].Ended()), "cluster.serve.labels") {
			served++
		}
	}

	assert.Positive(t, served, "an owner served the label RPC")
}

// TestClusterLabelsRecordSignalUnsupported pins the record signals' answer: they have no label
// index, so they report [fetch.ErrLabelsUnsupported] — locally and across the wire — which leaves
// their callers on the path they were already taking instead of failing the query.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestClusterLabelsRecordSignalUnsupported(t *testing.T) {
	endpoint := startEtcd(t)
	ctx := t.Context()

	nodes := map[string]*Storage{
		"node-a": openClusterNode(t, endpoint, "node-a"),
		"node-b": openClusterNode(t, endpoint, "node-b"),
	}
	awaitMembership(t, nodes)

	a := nodes["node-a"]

	_, err := a.WriteLogs(ctx, logBatch("api", [3]any{100, 9, "hello"}))
	require.NoError(t, err)

	for _, sig := range []signal.Signal{signal.Log, signal.Trace, signal.Profile} {
		_, err := a.localLabels(ctx, cluster.LabelsRequest{Signal: sig, Tenant: "default"})
		require.Truef(t, errors.Is(err, fetch.ErrLabelsUnsupported), "%s: got %v", sig, err)

		_, err = cluster.FetchLabels(ctx, a.cluster.httpc, nodes["node-b"].cluster.self,
			cluster.LabelsRequest{Signal: sig, Tenant: "default"})
		require.Truef(t, errors.Is(err, fetch.ErrLabelsUnsupported), "%s over the wire: got %v", sig, err)
	}

	// The observable consequence: record fetchers expose no label capability, so their callers keep
	// their current path, while the metric seam now has one.
	assert.Nil(t, fetch.LabelListerOf(a.LogFetcher("default")), "logs stay on their current path")
	assert.Nil(t, fetch.LabelListerOf(a.TraceFetcher("default")), "traces stay on their current path")
	assert.NotNil(t, fetch.LabelListerOf(a.Fetcher("default")), "metrics reach the index path")
}

// TestClusterLabelsRejectsUnpushableMatcher pins the capability's contract at the cluster seam: the
// shards return strings, not identities, so a matcher that could not be lowered into the index has
// nowhere to be re-checked and the whole call must decline rather than answer too broadly.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestClusterLabelsRejectsUnpushableMatcher(t *testing.T) {
	endpoint := startEtcd(t)

	nodes := map[string]*Storage{"node-a": openClusterNode(t, endpoint, "node-a")}
	awaitMembership(t, nodes)

	writeLabelSeries(t, nodes["node-a"])

	lister := fetch.LabelListerOf(nodes["node-a"].Fetcher("default"))
	require.NotNil(t, lister)

	// nameMatcher carries no Spec, so it cannot be pushed into a shard's index.
	_, err := lister.LabelNames(t.Context(), fetch.Request{
		Start: 0, End: 1 << 60, Matchers: []fetch.Matcher{nameMatcher("http.requests")},
	})
	require.True(t, errors.Is(err, fetch.ErrLabelsUnsupported), "got %v", err)
}
