package router_test

import (
	"context"
	"net"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"

	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/cluster/etcd"
	"github.com/oteldb/storage/cluster/router"
	"github.com/oteldb/storage/signal"
)

const httpScheme = "http"

func freeAddr(t *testing.T) string {
	t.Helper()

	var lc net.ListenConfig

	l, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = l.Close() }()

	return l.Addr().String()
}

// startEtcd runs an embedded etcd for the test and returns its client endpoint.
func startEtcd(t *testing.T) string {
	t.Helper()

	lc := url.URL{Scheme: httpScheme, Host: freeAddr(t)}
	lp := url.URL{Scheme: httpScheme, Host: freeAddr(t)}

	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	cfg.LogLevel = "error"
	cfg.ListenClientUrls = []url.URL{lc}
	cfg.AdvertiseClientUrls = []url.URL{lc}
	cfg.ListenPeerUrls = []url.URL{lp}
	cfg.AdvertisePeerUrls = []url.URL{lp}
	cfg.InitialCluster = cfg.Name + "=" + lp.String()

	e, err := embed.StartEtcd(cfg)
	require.NoError(t, err)
	t.Cleanup(e.Close)

	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(30 * time.Second):
		t.Fatal("embedded etcd did not become ready")
	}

	return lc.String()
}

// joinNode registers a member the way a storage node does, so the router resolves against a real
// membership rather than a hand-built ring.
func joinNode(t *testing.T, endpoint, root, id, addr string) {
	t.Helper()

	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	m, err := etcd.Join(t.Context(), client, root, etcd.Member{ID: id, Addr: addr}, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close(context.WithoutCancel(t.Context())) })
}

// TestRouterCloseReportsSuccess pins that a clean close reports success. errors.Wrap builds a
// non-nil error from a nil one, so Close could never return nil: every caller's shutdown path took
// the error branch and a normal exit looked like a failure. Every other test here discards Close's
// error, which is why it went unnoticed.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestRouterCloseReportsSuccess(t *testing.T) {
	const root = "/test"

	endpoint := startEtcd(t)
	joinNode(t, endpoint, root, "node-a", "10.0.0.1:9000")

	r, err := router.Open(t.Context(), router.Config{
		Etcd: []string{endpoint}, Root: root, RF: 1, ShardsPerTenant: 1,
	})
	require.NoError(t, err)

	require.NoError(t, r.Close(context.WithoutCancel(t.Context())), "a clean close reports success")
}

func TestRouterResolvesPlacement(t *testing.T) {
	t.Parallel()

	const root = "/test"

	endpoint := startEtcd(t)
	for _, n := range []struct{ id, addr string }{
		{"node-a", "10.0.0.1:9000"},
		{"node-b", "10.0.0.2:9000"},
		{"node-c", "10.0.0.3:9000"},
	} {
		joinNode(t, endpoint, root, n.id, n.addr)
	}

	r, err := router.Open(t.Context(), router.Config{
		Etcd: []string{endpoint}, Root: root, RF: 2, ShardsPerTenant: 4,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close(context.WithoutCancel(t.Context())) })

	require.Eventually(t, func() bool { return len(r.Members()) == 3 },
		10*time.Second, 10*time.Millisecond, "router follows membership")

	assert.Equal(t, 4, r.ShardCount())

	owners := r.Owners("acme/_s1")
	require.Len(t, owners, 2, "RF owners")

	primary, ok := r.Primary("acme/_s1")
	require.True(t, ok)
	assert.Equal(t, owners[0], primary, "the primary is the first owner")

	// The router itself is not placed: every owner is one of the registered nodes.
	for _, addr := range owners {
		assert.Contains(t, []string{"10.0.0.1:9000", "10.0.0.2:9000", "10.0.0.3:9000"}, addr)
	}
}

// TestRouterShardKeysMatchClusterDerivation pins the property the whole split depends on: an
// ingester's routing and a node's placement must derive the same shard key from the same series,
// or writes land on a shard nobody reads.
func TestRouterShardKeysMatchClusterDerivation(t *testing.T) {
	t.Parallel()

	const (
		root   = "/test"
		shards = 8
	)

	endpoint := startEtcd(t)
	joinNode(t, endpoint, root, "node-a", "10.0.0.1:9000")

	r, err := router.Open(t.Context(), router.Config{
		Etcd: []string{endpoint}, Root: root, ShardsPerTenant: shards,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close(context.WithoutCancel(t.Context())) })

	keys := r.ShardKeys("acme")
	require.Len(t, keys, shards)

	for i := range uint64(500) {
		id := signal.SeriesID{Lo: i, Hi: i * 7}

		got := r.ShardKey("acme", id)
		want := cluster.ShardKeyOf("acme", cluster.ShardOf(id, shards), shards)

		require.Equal(t, want, got)
		require.Contains(t, keys, got, "every series lands on an enumerated shard")
	}
}

func TestRouterPrimaryWrite(t *testing.T) {
	t.Parallel()

	const root = "/test"

	var (
		gotSig   signal.Signal
		gotShard string
		gotWAL   []byte
	)

	mux := http.NewServeMux()
	mux.Handle(cluster.PrimaryWritePath, cluster.PrimaryWriteHandler(
		func(_ context.Context, sig signal.Signal, shardKey string, wal []byte) (cluster.Reject, error) {
			gotSig, gotShard, gotWAL = sig, shardKey, wal

			return cluster.Reject{OOO: 2}, nil
		}))

	addr := freeAddr(t)
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	var lc net.ListenConfig

	ln, err := lc.Listen(t.Context(), "tcp", addr)
	require.NoError(t, err)

	go func() { _ = srv.Serve(ln) }()

	t.Cleanup(func() { _ = srv.Close() })

	endpoint := startEtcd(t)
	joinNode(t, endpoint, root, "node-a", addr)

	r, err := router.Open(t.Context(), router.Config{Etcd: []string{endpoint}, Root: root, RF: 1})
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close(context.WithoutCancel(t.Context())) })

	wal := []byte{0x0a, 0x0b}
	rej, err := r.PrimaryWrite(t.Context(), signal.Trace, "acme", wal)
	require.NoError(t, err)

	assert.Equal(t, cluster.Reject{OOO: 2}, rej)
	assert.Equal(t, signal.Trace, gotSig)
	assert.Equal(t, "acme", gotShard)
	assert.Equal(t, wal, gotWAL)
}

func TestRouterPrimaryWriteFailsOnEmptyRing(t *testing.T) {
	t.Parallel()

	endpoint := startEtcd(t)

	r, err := router.Open(t.Context(), router.Config{Etcd: []string{endpoint}, Root: "/test"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close(context.WithoutCancel(t.Context())) })

	// With nobody to route to, the write must fail loudly rather than report a silent success.
	_, err = r.PrimaryWrite(t.Context(), signal.Metric, "acme", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no primary")

	assert.Empty(t, r.Owners("acme"))

	_, ok := r.Primary("acme")
	assert.False(t, ok)
}

func TestRouterOpenRequiresEndpoints(t *testing.T) {
	t.Parallel()

	_, err := router.Open(t.Context(), router.Config{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "etcd endpoints are required")
}
