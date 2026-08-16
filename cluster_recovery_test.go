package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/cluster/etcd"
	"github.com/oteldb/storage/query/fetch"
)

// TestClusterDurableWithoutWALRejected pins the configuration behind issue #340: a clustered node on
// a durable backend and no WAL recovers only its flushed parts, then immediately answers reads as a
// ring owner — serving everything written since its last flush as absent, which the read path cannot
// tell from real absence. Open refuses it instead of serving the hole.
func TestClusterDurableWithoutWALRejected(t *testing.T) {
	t.Parallel()

	be, err := file.New(t.TempDir())
	require.NoError(t, err)

	_, err = Open(context.Background(), Options{}, WithBackend(be), WithCluster(&cluster.Config{
		Etcd: []string{"127.0.0.1:1"},
		Self: etcd.Member{ID: "node-a", Addr: "127.0.0.1:0"},
		RF:   2,
	}))
	require.ErrorContains(t, err, "WALDir is required in cluster mode")
}

// TestClusterDurableWithWALAccepted guards the other side of the rule: the same durable clustered
// node *with* a WAL is valid, and an ephemeral (in-memory) clustered node needs no WAL at all — a
// restart there starts empty everywhere, so there is no partial state to serve.
func TestClusterDurableWithWALAccepted(t *testing.T) {
	t.Parallel()

	be, err := file.New(t.TempDir())
	require.NoError(t, err)

	durable := Options{Backend: be, WALDir: t.TempDir(), Cluster: &cluster.Config{}}
	require.NoError(t, durable.validate())

	ephemeral := Options{Backend: backend.Memory(), Cluster: &cluster.Config{}}
	require.NoError(t, ephemeral.validate())
}

// TestClusterRestartServesUnflushedHead is the end-to-end remedy: a clustered durable node crashes
// with an unflushed head, restarts over the same backend and WAL, and its very first read as a ring
// owner is complete. Without the WAL replay this read returns an empty result — a silent hole, since
// a successful answer from one owner is taken as the whole shard.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestClusterRestartServesUnflushedHead(t *testing.T) {
	endpoint := startEtcd(t)
	ctx := context.Background()
	dataDir, walDir := t.TempDir(), t.TempDir()

	open := func() *Storage {
		be, err := file.New(dataDir)
		require.NoError(t, err)

		// A negative flush interval disables the background flush, so the head stays unflushed and
		// the WAL is the only copy the restart can recover from.
		s, err := Open(ctx, Options{}, WithBackend(be), WithWALDir(walDir), WithFlushInterval(-1),
			WithCluster(&cluster.Config{
				Etcd: []string{endpoint},
				Self: etcd.Member{ID: "node-a", Addr: "127.0.0.1:0"},
				RF:   1,
			}))
		require.NoError(t, err)

		return s
	}

	s1 := open()
	_, err := s1.WriteMetrics(ctx, gaugeBatch("api", "http.requests", []int64{100, 200}, []float64{1, 2}))
	require.NoError(t, err)
	crash(t, s1) // release the WAL handles without flushing or checkpointing

	s2 := open()
	t.Cleanup(func() { _ = s2.Close(ctx) })

	e, ok := s2.lookupEngine("default")
	require.True(t, ok)
	require.Zero(t, e.PartCount(), "nothing was ever flushed: the head is the only copy")

	got, err := fetch.Drain(ctx, must(s2.Fetcher("default").Fetch(ctx, fetch.Request{
		Start: 0, End: 1 << 60, Matchers: []fetch.Matcher{nameMatcher("http.requests")},
	})))
	require.NoError(t, err)
	require.Len(t, got, 1, "the restarted owner serves its recovered head, not a hole")
	assert.Equal(t, []int64{100, 200}, got[0].Timestamps)
	assert.Equal(t, []float64{1, 2}, got[0].Values)
}
