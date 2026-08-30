package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/oteldb/storage/backend"
)

// TestClusterRoutedPointsCountedOnRoutingNode pins the coordinator side of a clustered write:
// storage.cluster.routed_points is recorded by the node that routed the batch, whichever peer the
// shard primary turned out to be. Without it a cluster has no counter attributable to the node
// that took a write in — storage.ingest.accepted lands on the storing engine and
// storage.flush.total on the flushing one, and on a replicated cluster those are other nodes.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestClusterRoutedPointsCountedOnRoutingNode(t *testing.T) {
	endpoint := startEtcd(t)
	ctx := context.Background()

	ids := []string{"node-a", "node-b", "node-c"}
	readers := make(map[string]*sdkmetric.ManualReader, len(ids))
	nodes := make(map[string]*Storage, len(ids))

	for _, id := range ids {
		r := sdkmetric.NewManualReader()
		readers[id] = r
		nodes[id] = openClusterNodeWith(t, endpoint, id, backend.Memory(),
			WithMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(r))))
	}

	awaitMembership(t, nodes)

	collect := func(id string) metricdata.ResourceMetrics {
		t.Helper()

		var rm metricdata.ResourceMetrics
		require.NoError(t, readers[id].Collect(ctx, &rm))

		return rm
	}

	const routedPoints = "storage.cluster.routed_points"

	writer := nodes["node-a"]

	_, err := writer.WriteMetrics(ctx, gaugeBatch("api", "m", []int64{1, 2, 3, 4}, []float64{1, 2, 3, 4}))
	require.NoError(t, err)

	_, err = writer.WriteLogs(ctx, logBatch("api", [3]any{100, 1, "x"}, [3]any{101, 1, "y"}))
	require.NoError(t, err)

	rm := collect("node-a")
	require.Equal(t, int64(4), sumCounter(t, rm, routedPoints,
		map[string]string{"signal": "metric", "result": routedAccepted}))
	require.Equal(t, int64(2), sumCounter(t, rm, routedPoints,
		map[string]string{"signal": "log", "result": routedAccepted}))
	require.Zero(t, sumCounter(t, rm, routedPoints, map[string]string{"result": routedRejected}))
	require.Zero(t, sumCounter(t, rm, routedPoints, map[string]string{"result": routedFailed}))

	for _, id := range ids[1:] {
		require.Zerof(t, sumCounter(t, collect(id), routedPoints, nil),
			"%s routed nothing: the counter belongs to the routing node, not the storing one", id)
	}
}
