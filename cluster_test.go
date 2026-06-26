package storage

import (
	"context"
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/server/v3/embed"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/cluster/etcd"
	"github.com/oteldb/storage/query/fetch"
)

func freeAddr(t *testing.T) string {
	t.Helper()
	var lc net.ListenConfig
	l, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())

	return addr
}

// startEtcd boots an in-process single-node etcd and returns its client endpoint URL.
func startEtcd(t *testing.T) string {
	t.Helper()

	clientURL := "http://" + freeAddr(t)
	peerURL := "http://" + freeAddr(t)
	lc, _ := url.Parse(clientURL)
	lp, _ := url.Parse(peerURL)

	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	cfg.LogLevel = "error"
	cfg.ListenClientUrls = []url.URL{*lc}
	cfg.AdvertiseClientUrls = []url.URL{*lc}
	cfg.ListenPeerUrls = []url.URL{*lp}
	cfg.AdvertisePeerUrls = []url.URL{*lp}
	cfg.InitialCluster = cfg.Name + "=" + peerURL

	e, err := embed.StartEtcd(cfg)
	require.NoError(t, err)
	t.Cleanup(e.Close)

	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(30 * time.Second):
		t.Fatal("embedded etcd did not become ready")
	}

	return clientURL
}

func openClusterNode(t *testing.T, endpoint, id string) *Storage {
	t.Helper()

	s, err := Open(context.Background(), Options{}, WithBackend(backend.Memory()),
		WithCluster(&cluster.Config{
			Etcd: []string{endpoint},
			Self: etcd.Member{ID: id, Addr: freeAddr(t)},
			RF:   2,
		}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	return s
}

// TestClusteredStorageReplicatesAcrossNodes is the M6 facade capstone: two clustered Storage
// nodes share an etcd; a write to one is routed by the ring and replicated to both, so each
// node serves it from its own engine.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestClusteredStorageReplicatesAcrossNodes(t *testing.T) {
	endpoint := startEtcd(t)
	ctx := context.Background()

	a := openClusterNode(t, endpoint, "node-a")
	b := openClusterNode(t, endpoint, "node-b")

	// Wait for both nodes to see the full 2-node membership before writing.
	require.Eventually(t, func() bool {
		return len(a.cluster.membership.Members()) == 2 && len(b.cluster.membership.Members()) == 2
	}, 10*time.Second, 50*time.Millisecond, "membership converges to two nodes")

	// Write to node A; with RF=2 the tenant's owners are both nodes (quorum 2 ⇒ both apply).
	_, err := a.WriteMetrics(ctx, gaugeBatch("api", "http.requests", []int64{100, 200}, []float64{1, 2}))
	require.NoError(t, err)

	// Both nodes' engines independently hold the replicated series.
	for name, s := range map[string]*Storage{"node-a": a, "node-b": b} {
		it, err := s.Fetcher("default").Fetch(ctx, fetch.Request{
			Start: 0, End: 1 << 60, Matchers: []fetch.Matcher{nameMatcher("http.requests")},
		})
		require.NoError(t, err)
		got, err := fetch.Drain(ctx, it)
		require.NoError(t, err)
		require.Lenf(t, got, 1, "%s serves the replicated series", name)
		assert.Equalf(t, []int64{100, 200}, got[0].Timestamps, "%s timestamps", name)
		assert.Equalf(t, []float64{1, 2}, got[0].Values, "%s values", name)
	}
}

// TestClusteredReadFansOutToOwners is the read-fan-out capstone: with three nodes and RF=2,
// a tenant is owned by two of them; a query on the third (a non-owner, which holds none of the
// tenant's data) fans out to an owner over HTTP and returns the result.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestClusteredReadFansOutToOwners(t *testing.T) {
	endpoint := startEtcd(t)
	ctx := context.Background()

	nodes := map[string]*Storage{
		"node-a": openClusterNode(t, endpoint, "node-a"),
		"node-b": openClusterNode(t, endpoint, "node-b"),
		"node-c": openClusterNode(t, endpoint, "node-c"),
	}
	a := nodes["node-a"]

	require.Eventually(t, func() bool {
		return len(a.cluster.membership.Members()) == 3
	}, 10*time.Second, 50*time.Millisecond, "membership converges to three nodes")

	// Write via node A; it routes to the tenant's two owners.
	_, err := a.WriteMetrics(ctx, gaugeBatch("api", "http.requests", []int64{100, 200}, []float64{1, 2}))
	require.NoError(t, err)

	// The tenant's owners are two of the three nodes; find the third (the non-owner).
	owners := a.cluster.membership.Ring().Lookup([]byte("default"), 2)
	require.Len(t, owners, 2)
	ownerID := map[string]bool{owners[0].ID: true, owners[1].ID: true}

	var nonOwner *Storage
	var nonOwnerName string
	for name, s := range nodes {
		if !ownerID[name] {
			nonOwner, nonOwnerName = s, name
		}
	}
	require.NotNil(t, nonOwner, "one node is not an owner")

	// The non-owner holds no local data for the tenant...
	_, hasLocal := nonOwner.lookupEngine("default")
	assert.Falsef(t, hasLocal, "%s (non-owner) has no local engine for the tenant", nonOwnerName)

	// ...yet its Fetcher fans out to an owner and returns the data.
	it, err := nonOwner.Fetcher("default").Fetch(ctx, fetch.Request{
		Start: 0, End: 1 << 60, Matchers: []fetch.Matcher{nameMatcher("http.requests")},
	})
	require.NoError(t, err)
	got, err := fetch.Drain(ctx, it)
	require.NoError(t, err)
	require.Lenf(t, got, 1, "%s served the series via read fan-out", nonOwnerName)
	assert.Equal(t, []int64{100, 200}, got[0].Timestamps)
	assert.Equal(t, []float64{1, 2}, got[0].Values)
}
