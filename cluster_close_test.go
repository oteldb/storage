package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/cluster/etcd"
	"github.com/oteldb/storage/cluster/etcd/etcdtest"
	"github.com/oteldb/storage/signal"
)

// TestClusterCloseFlushesWhileTheClaimIsHeld pins the order of a clustered Close: an owner drains
// its head while it still holds the shard's claim, since once it has left the cluster no commit of
// its is a tenure's. A node that does not own a shard closes cleanly too, leaving that head to the
// WAL.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestClusterCloseFlushesWhileTheClaimIsHeld(t *testing.T) {
	endpoint := etcdtest.Start(t)
	ctx := context.Background()
	dataDir, walDir := t.TempDir(), t.TempDir()

	open := func() *Storage {
		t.Helper()

		be, err := file.New(dataDir)
		require.NoError(t, err)

		s, err := Open(ctx, Options{}, WithBackend(be), WithWALDir(walDir), WithFlushInterval(-1),
			WithCluster(&cluster.Config{
				Etcd:           []string{endpoint},
				Self:           etcd.Member{ID: "node-a", Addr: "127.0.0.1:0"},
				RF:             1,
				PrivateBackend: true,
			}))
		require.NoError(t, err)

		return s
	}

	s := open()
	_, err := s.WriteMetrics(ctx, gaugeBatch("api", "http.requests", []int64{100, 200}, []float64{1, 2}))
	require.NoError(t, err)

	const shard = signal.TenantID("default")

	_, owned := s.ownedTenants(ctx, map[signal.TenantID]struct{}{shard: {}})[shard]
	require.True(t, owned)
	require.NoError(t, s.Close(ctx))

	s = open()
	t.Cleanup(func() { _ = s.Close(ctx) })

	e, ok := s.lookupEngine(shard)
	require.True(t, ok)
	assert.Equal(t, 1, e.PartCount(), "the head was flushed before the node left the cluster")
	assert.Zero(t, e.HeadSampleCount(), "and the WAL replays nothing a part already holds")

	_, err = s.WriteMetrics(ctx, gaugeBatch("api", "http.requests", []int64{300}, []float64{3}))
	require.NoError(t, err)
	require.NoError(t, s.Close(ctx), "a node holding no claim closes cleanly, its head left to the WAL")
}
