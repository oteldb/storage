package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/cluster/etcd/etcdtest"
	"github.com/oteldb/storage/internal/reproduce"
	"github.com/oteldb/storage/signal"
)

// TestClusterHandoffUnsyncedFlushReadsOnce is the shared-nothing duplicate of #725: the old owner
// flushes a part its secondary never mirrors, then loses the shard. The secondary's head still holds
// those rows — a replica trims only against parts it has loaded — so the new owner flushes them again
// under its own tenure. Once the old owner is back and the two part sets meet, every row must still
// be read once.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestClusterHandoffUnsyncedFlushReadsOnce(t *testing.T) {
	endpoint := etcdtest.Start(t)
	ctx := context.Background()

	const shard = signal.TenantID("default")

	proxies := map[string]*etcdProxy{"node-a": newEtcdProxy(t, endpoint), "node-b": newEtcdProxy(t, endpoint)}
	nodes := map[string]*Storage{}

	for id, p := range proxies {
		nodes[id] = openClusterNodeWith(t, p.endpoint, id, backend.Memory())
	}

	awaitMembership(t, nodes)

	primary, ok := nodes["node-a"].cluster.membership.Ring().Primary([]byte(shard))
	require.True(t, ok)

	oldOwner, oldProxy := nodes[primary.ID], proxies[primary.ID]

	var secondary *Storage

	for id, s := range nodes {
		if id != primary.ID {
			secondary = s
		}
	}

	samples := []float64{1, 2, 3}
	_, err := oldOwner.WriteMetrics(ctx, gaugeBatch("svc", "tenure_metric", []int64{100, 200, 300}, samples))
	require.NoError(t, err)
	_, err = oldOwner.WriteLogs(ctx, logBatch("svc", [3]any{100, 9, "a"}, [3]any{200, 9, "b"}, [3]any{300, 9, "c"}))
	require.NoError(t, err)

	// Flushed directly rather than by a maintenance pass, which would notify the secondary: this is
	// the notify lost to the partition, so the secondary never mirrors the part.
	_, owns := oldOwner.ownedTenants(ctx, map[signal.TenantID]struct{}{shard: {}})[shard]
	require.True(t, owns, "the ring primary claims the shard")

	om, ok := oldOwner.lookupEngine(shard)
	require.True(t, ok)
	require.NoError(t, om.Flush(ctx))

	ol, ok := oldOwner.lookupRecordEngine(signal.Log, shard)
	require.True(t, ok)
	require.NoError(t, ol.Flush(ctx))

	sm, ok := secondary.lookupEngine(shard)
	require.True(t, ok)
	sl, ok := secondary.lookupRecordEngine(signal.Log, shard)
	require.True(t, ok)
	require.Equal(t, len(samples), sm.HeadSampleCount(), "the owner's flush leaves the secondary's head alone")
	require.Equal(t, len(samples), sl.HeadRecordCount(), "the owner's flush leaves the secondary's head alone")

	oldLease := oldOwner.cluster.membership.LeaseID()
	oldProxy.cut()

	direct, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
	require.NoError(t, err)

	defer func() { _ = direct.Close() }()

	// Revoking is the lease expiring, without waiting out the TTL: the claim goes with it.
	_, err = direct.Revoke(ctx, oldLease)
	require.NoError(t, err)

	require.Eventually(t, func() bool { return ringSize(secondary) == 1 }, 10*time.Second, 20*time.Millisecond,
		"the secondary drops the partitioned owner from its ring")

	secondary.maintain(ctx)

	_, held := secondary.cluster.ownership.Term(string(shard))
	require.True(t, held, "the secondary takes the shard under a new term")
	require.Zero(t, sm.HeadSampleCount(), "the new owner flushed its head")
	require.Zero(t, sl.HeadRecordCount(), "the new owner flushed its head")

	oldProxy.restore()
	require.Eventually(t, func() bool {
		return oldOwner.cluster.membership.LeaseID() != oldLease && ringSize(oldOwner) == 2 && ringSize(secondary) == 2
	}, 30*time.Second, 20*time.Millisecond, "the old owner rejoins")

	// The old owner installs the new owner's index beside the part it holds, takes the shard back once
	// the secondary releases it, and merges; the secondary mirrors the result.
	for range 3 {
		oldOwner.maintain(ctx)
		secondary.maintain(ctx)
	}

	// The metric half does not duplicate: a read merges a series by timestamp, and so does the merge
	// that folds the two parts together.
	t.Run("metrics", func(t *testing.T) {
		for id, s := range nodes {
			assert.Equalf(t, samples, readSamples(t, s, "tenure_metric"), "%s reads every sample once", id)

			m, _ := s.lookupEngine(shard)

			var stored int64
			for _, p := range m.Parts() {
				stored += p.Rows
			}

			assert.Equalf(t, int64(len(samples)), stored, "%s stores every sample once", id)
		}
	})

	t.Run("logs", func(t *testing.T) {
		reproduce.Unfixed(t, 725, "the new owner re-flushes head rows the old owner's unsynced part holds, and once "+
			"the old owner is back both parts stay live and merge into one part holding every record twice")

		for id, s := range nodes {
			got, err := fetchLogs(ctx, s, 0, 1<<62)
			require.NoError(t, err)
			assert.Equalf(t, len(samples), logRows(got), "%s reads every log record once", id)
		}
	})
}
