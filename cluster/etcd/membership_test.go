package etcd

import (
	"context"
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

const httpScheme = "http"

func TestMemberEncodeDecodeRoundTrip(t *testing.T) {
	t.Parallel()

	in := Member{ID: "node-7", Zone: "eu-1", Addr: "10.0.0.7:9000"}
	out, err := decodeMember(in.encode())
	require.NoError(t, err)
	assert.Equal(t, in, out)
}

func TestDecodeMemberRejectsGarbage(t *testing.T) {
	t.Parallel()

	_, err := decodeMember([]byte("not json"))
	require.Error(t, err)
}

// freeAddr returns a free localhost host:port (the listener is closed before returning, so
// there is a small TOCTOU window — acceptable for a test harness).
func freeAddr(t *testing.T) string {
	t.Helper()
	var lc net.ListenConfig
	l, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())

	return addr
}

// startEtcd boots an in-process single-node etcd and returns a client for it.
func startEtcd(t *testing.T) *clientv3.Client {
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

	client, err := clientv3.New(clientv3.Config{Endpoints: []string{lc.String()}, DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	return client
}

func memberIDs(ms []Member) []string {
	out := make([]string, len(ms))
	for i, m := range ms {
		out[i] = m.ID
	}

	return out
}

//nolint:paralleltest // owns an embedded etcd; runs serially
func TestMembershipJoinWatchLeave(t *testing.T) {
	client := startEtcd(t)
	ctx := context.Background()

	// Node A joins.
	a, err := Join(ctx, client, "/oteldb", Member{ID: "node-a", Zone: "z1", Addr: "a:1"}, 5*time.Second)
	require.NoError(t, err)
	assert.Equal(t, []string{"node-a"}, memberIDs(a.Members()))
	assert.Equal(t, 1, a.Ring().Len())

	// Node B joins; A's watch must see it and rebuild the ring.
	b, err := Join(ctx, client, "/oteldb", Member{ID: "node-b", Zone: "z2", Addr: "b:1"}, 5*time.Second)
	require.NoError(t, err)

	require.Eventually(t, func() bool { return a.Ring().Len() == 2 }, 5*time.Second, 25*time.Millisecond,
		"node A's ring picks up node B")
	assert.Equal(t, []string{"node-a", "node-b"}, memberIDs(a.Members()))

	// Both nodes place a key on the same owners (deterministic, shared membership).
	assert.Equal(t,
		a.Ring().Lookup([]byte("series-42"), 2),
		b.Ring().Lookup([]byte("series-42"), 2),
		"placement agrees across nodes")

	// Node B leaves (lease revoked on Close); A's ring shrinks back.
	require.NoError(t, b.Close(ctx))
	require.Eventually(t, func() bool { return a.Ring().Len() == 1 }, 5*time.Second, 25*time.Millisecond,
		"node A drops node B after it leaves")
	assert.Equal(t, []string{"node-a"}, memberIDs(a.Members()))

	require.NoError(t, a.Close(ctx))
}

func TestJoinRequiresID(t *testing.T) {
	t.Parallel()

	_, err := Join(context.Background(), nil, "/oteldb", Member{}, time.Second)
	require.Error(t, err)
}
