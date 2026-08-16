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
	"github.com/oteldb/storage/signal"
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

// TestClusterRestartedReplicaFailsOverInsteadOfServingAHole is the read-completeness half of #340: a
// replica holds the shard's unflushed head in memory only, so a restart brings it back with the
// flushed parts and nothing else. It is still a ring owner, and the read path takes one owner's
// answer as complete — so before the gate it answered such a query short, with no error and nothing
// metered. It must disclaim the window it lost instead, letting the owner that kept the head answer.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestClusterRestartedReplicaFailsOverInsteadOfServingAHole(t *testing.T) {
	endpoint := startEtcd(t)
	ctx := context.Background()
	dataDir := t.TempDir() // one shared durable backend, as a cluster over an object store has
	walDirs := map[string]string{"node-a": t.TempDir(), "node-b": t.TempDir()}

	// A negative flush interval disables the background loop: the flushes below are driven by hand,
	// so "flushed" and "still only in the head" stay exact.
	open := func(id string) *Storage {
		be, err := file.New(dataDir)
		require.NoError(t, err)

		s, err := Open(ctx, Options{}, WithBackend(be), WithWALDir(walDirs[id]), WithFlushInterval(-1),
			WithCluster(&cluster.Config{
				Etcd: []string{endpoint},
				Self: etcd.Member{ID: id, Addr: "127.0.0.1:0"},
				RF:   2,
			}))
		require.NoError(t, err)

		return s
	}

	nodes := map[string]*Storage{"node-a": open("node-a"), "node-b": open("node-b")}
	t.Cleanup(func() {
		for _, s := range nodes {
			_ = s.Close(ctx)
		}
	})
	awaitMembership(t, nodes)

	_, err := nodes["node-a"].WriteMetrics(ctx, gaugeBatch("api", "http.requests", []int64{100, 200}, []float64{1, 2}))
	require.NoError(t, err)

	p, ok := nodes["node-a"].cluster.membership.Ring().Primary([]byte("default"))
	require.True(t, ok)

	primaryID := p.ID
	replicaID := "node-a"

	if primaryID == replicaID {
		replicaID = "node-b"
	}

	// The first batch reaches a part on the shared store, and the replica adopts it.
	nodes[primaryID].maintain(ctx)
	nodes[replicaID].maintain(ctx)

	re, ok := nodes[replicaID].lookupEngine("default")
	require.True(t, ok)
	require.Equal(t, 1, re.PartCount(), "the replica sees the flushed part")

	// The second batch is quorum-acked but unflushed: the replica holds it in memory only.
	_, err = nodes[primaryID].WriteMetrics(ctx, gaugeBatch("api", "http.requests", []int64{300}, []float64{3}))
	require.NoError(t, err)

	// The replica dies without flushing (leaving the ring as a lease expiry would, then dropping its
	// WAL handles) and comes back over the same backend and WAL.
	require.NoError(t, nodes[replicaID].cluster.close(ctx))
	crash(t, nodes[replicaID])
	nodes[replicaID] = open(replicaID)
	awaitMembership(t, nodes)

	restarted := nodes[replicaID]
	re, ok = restarted.lookupEngine("default")
	require.True(t, ok)
	require.Zero(t, re.HeadSampleCount(), "the restarted replica came back without the unflushed head")

	got, err := fetch.Drain(ctx, must(restarted.Fetcher("default").Fetch(ctx, fetch.Request{
		Start: 0, End: 1 << 60, Matchers: []fetch.Matcher{nameMatcher("http.requests")},
	})))
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, []int64{100, 200, 300}, got[0].Timestamps,
		"the restarted owner disclaims the window it lost, so the owner that kept the head answers")
	require.True(t, restarted.hasReadGap(signal.Metric, "default"), "the gap is what routed that read away")

	// The owner flushes what the restarted node lost; adopting that part makes it whole again, so it
	// stops failing over and answers the same query locally.
	nodes[primaryID].maintain(ctx)
	restarted.maintain(ctx)

	assert.False(t, restarted.hasReadGap(signal.Metric, "default"), "parts past the gap close it")

	got, err = fetch.Drain(ctx, must(restarted.Fetcher("default").Fetch(ctx, fetch.Request{
		Start: 0, End: 1 << 60, Matchers: []fetch.Matcher{nameMatcher("http.requests")},
	})))
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, []int64{100, 200, 300}, got[0].Timestamps, "and serves the whole shard itself")
}
