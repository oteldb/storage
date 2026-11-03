package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/cluster/etcd"
	"github.com/oteldb/storage/cluster/etcd/etcdtest"
	"github.com/oteldb/storage/query/fetch"
)

//nolint:paralleltest // Owns an embedded etcd and three node listeners.
func TestGroupedMetricsReplicatePartsAfterPrimaryLoss(t *testing.T) {
	ctx := t.Context()
	endpoint := etcdtest.Start(t)
	nodes := map[string]*Storage{}
	for _, id := range []string{"group-a", "group-b", "group-c"} {
		files, err := backendfile.New(t.TempDir())
		require.NoError(t, err)
		s, err := Open(ctx, Options{}, WithBackend(files), WithWALDir(t.TempDir()), WithMetricFloatGroups(), WithFlushInterval(-1), WithCluster(&cluster.Config{Etcd: []string{endpoint}, Self: etcd.Member{ID: id, Addr: "127.0.0.1:0"}, RF: 3, PrivateBackend: true}))
		require.NoError(t, err)
		nodes[id] = s
		t.Cleanup(func() { _ = s.Close(context.Background()) })
	}
	a := nodes["group-a"]
	require.Eventually(t, func() bool {
		for _, s := range nodes {
			if ringSize(s) != 3 {
				return false
			}
		}
		return true
	}, 10*time.Second, 10*time.Millisecond)
	_, err := a.WriteMetrics(ctx, gaugeBatch("api", "grouped_metric", []int64{100, 200, 300}, []float64{1, 2, 3}))
	require.NoError(t, err)
	owner, ok := a.cluster.membership.Ring().Primary([]byte("default"))
	require.True(t, ok)
	primary := nodes[owner.ID]
	primary.maintain(ctx)
	for id, s := range nodes {
		if id != owner.ID {
			s.maintain(ctx)
		}
		e, ok := s.lookupEngine("default")
		require.True(t, ok)
		require.Equal(t, 1, e.PartCount())
		require.Zero(t, e.HeadSampleCount())
		parts, err := e.PartsDetailed(ctx)
		require.NoError(t, err)
		require.Len(t, parts, 1)
		grouped := false
		for _, c := range parts[0].Columns {
			grouped = grouped || c.Name == "value" && c.Codec == "floatgroups"
		}
		require.True(t, grouped)
	}
	require.NoError(t, primary.Close(ctx))
	for id, s := range nodes {
		if id == owner.ID {
			continue
		}
		it, err := s.Fetcher("default").Fetch(ctx, fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{nameMatcher("grouped_metric")}})
		require.NoError(t, err)
		got, err := fetch.Drain(ctx, it)
		require.NoError(t, err)
		require.Len(t, got, 1)
		require.Equal(t, []float64{1, 2, 3}, got[0].Values)
	}
}
