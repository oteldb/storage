package etcd

// Membership maintenance under lease loss (oteldb/storage#375). Registration used to be a
// one-shot startup step: a node that lost its lease — an etcd restart, a node drain, a GC pause
// longer than the TTL — vanished from every peer's ring and never came back, because nothing
// ever wrote its member key again. Four pods Running and Ready, two leases.
//
// Every test here kills a live node's registration out from under it and asserts the ring
// reconverges **without restarting the node**: the same *Membership keeps serving throughout.
// The kills are direct etcd operations (revoke the lease, delete the key), not waits on a TTL,
// so they are instantaneous and deterministic; only the etcd-outage test spends real time, and
// it runs on a short TTL rather than the default.

import (
	"context"
	"net/url"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"

	"github.com/oteldb/storage/cluster/ring"
)

const (
	testRoot   = "/oteldb"
	testPrefix = testRoot + "/members/"
	// settle bounds the reconvergence assertions. It is slack, not a sleep: every test below
	// reaches its condition as soon as a watch event and one Put round-trip complete.
	settle = 30 * time.Second
	tick   = 10 * time.Millisecond
)

// etcdServer is an embedded etcd that can be stopped and restarted on the same client URL and
// data directory — what an etcd rollout looks like to a node that keeps running across it.
type etcdServer struct {
	t      *testing.T
	client url.URL
	peer   url.URL
	dir    string
	e      *embed.Etcd
}

func startEtcdServer(t *testing.T) *etcdServer {
	t.Helper()

	s := &etcdServer{
		t:      t,
		client: url.URL{Scheme: httpScheme, Host: freeAddr(t)},
		peer:   url.URL{Scheme: httpScheme, Host: freeAddr(t)},
		dir:    t.TempDir(),
	}
	s.start()
	t.Cleanup(s.stop)

	return s
}

func (s *etcdServer) start() {
	s.t.Helper()

	cfg := embed.NewConfig()
	cfg.Dir = s.dir
	cfg.LogLevel = "error"
	cfg.ListenClientUrls = []url.URL{s.client}
	cfg.AdvertiseClientUrls = []url.URL{s.client}
	cfg.ListenPeerUrls = []url.URL{s.peer}
	cfg.AdvertisePeerUrls = []url.URL{s.peer}
	cfg.InitialCluster = cfg.Name + "=" + s.peer.String()

	e, err := embed.StartEtcd(cfg)
	require.NoError(s.t, err)

	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(30 * time.Second):
		e.Close()
		s.t.Fatal("embedded etcd did not become ready")
	}

	s.e = e
}

func (s *etcdServer) stop() {
	if s.e == nil {
		return
	}

	s.e.Close()
	s.e = nil
}

// dial returns a client for this server, closed with the test.
func (s *etcdServer) dial() *clientv3.Client {
	s.t.Helper()

	c, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{s.client.String()},
		DialTimeout: 5 * time.Second,
	})
	require.NoError(s.t, err)
	s.t.Cleanup(func() { _ = c.Close() })

	return c
}

// memberKeys returns the live member keys with the lease each hangs off, or nil if etcd cannot
// be read right now — it is polled from inside Eventually predicates, where a read against a
// server that is still coming back is a "not yet", not a failure.
func memberKeys(client *clientv3.Client) map[string]int64 {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.Get(ctx, testPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil
	}

	out := make(map[string]int64, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		out[string(kv.GetKey())] = kv.GetLease()
	}

	return out
}

// requireConverged waits for the whole cluster to agree again: etcd holds exactly one member
// key per id, each key hangs off the lease its node currently believes it holds, and every
// node's own member view lists exactly ids with no node reporting itself absent.
//
// It has to be a single predicate over all of that. Any one condition can read as satisfied
// while the disturbance is still in flight — a node's watch has not yet delivered a deletion
// that already happened in etcd, so its ring still looks whole — and asserting them one by one
// would pass on the state the test set out to break.
func requireConverged(t *testing.T, client *clientv3.Client, nodes []*Membership, ids ...string) {
	t.Helper()

	require.Eventually(t, func() bool {
		keys := memberKeys(client)
		if len(keys) != len(ids) {
			return false
		}

		for _, m := range nodes {
			if m.SelfAbsent() || !slices.Equal(memberIDs(m.Members()), ids) {
				return false
			}

			if keys[testPrefix+m.self.ID] != int64(m.LeaseID()) {
				return false
			}
		}

		return true
	}, settle, tick, "the ring reconverges on %v with no node restarted", ids)
}

// joinAll registers n nodes on one client and returns them, closed with the test.
func joinAll(t *testing.T, client *clientv3.Client, ttl time.Duration, ids ...string) []*Membership {
	t.Helper()

	ctx := context.Background()

	out := make([]*Membership, 0, len(ids))

	for _, id := range ids {
		m, err := Join(ctx, client, testRoot, Member{ID: id, Zone: "z", Addr: id + ":1"}, ttl)
		require.NoError(t, err)

		out = append(out, m)
	}

	for _, m := range out {
		require.Eventually(t, func() bool { return m.Ring().Len() == len(ids) }, settle, tick,
			"initial ring converges on %s", m.self.ID)
	}

	t.Cleanup(func() {
		for _, m := range out {
			_ = m.Close(context.Background())
		}
	})

	return out
}

// TestMembershipRejoinsAfterLeaseExpiry is the observed field case: etcd is healthy and
// reachable throughout, and a single node's lease goes away — a stall longer than the TTL is
// indistinguishable from the revoke below, since either way etcd drops the lease and deletes
// the member key while the node keeps running. The node must come back on its own.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestMembershipRejoinsAfterLeaseExpiry(t *testing.T) {
	srv := startEtcdServer(t)
	client := srv.dial()
	ctx := context.Background()

	nodes := joinAll(t, client, 5*time.Second, "node-a", "node-b")
	a, b := nodes[0], nodes[1]

	lost := b.LeaseID()
	require.NotEqual(t, clientv3.NoLease, lost)

	// Expire node-b's lease from outside, exactly as etcd does when keep-alives stop arriving.
	_, err := srv.dial().Revoke(ctx, lost)
	require.NoError(t, err)

	require.Eventually(t, func() bool { return b.Rejoins() >= 1 }, settle, tick,
		"node-b notices it lost its registration and re-registers")

	// One key per node, node-b's on a fresh lease: the re-registration replaced node-b's
	// registration rather than adding a second one, and node-a picked it back up.
	requireConverged(t, client, nodes, "node-a", "node-b")
	assert.NotEqual(t, lost, b.LeaseID(), "node-b registered under a fresh lease")
	assert.Zero(t, a.Rejoins(), "the undisturbed node did not re-register")
}

// TestMembershipRejoinsAfterWholeRingLeaseLoss reproduces the reported end state directly:
// every member key gone, etcd healthy, every node still running. Before the fix this is
// terminal — the ring stays empty until each pod is deleted by hand.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestMembershipRejoinsAfterWholeRingLeaseLoss(t *testing.T) {
	srv := startEtcdServer(t)
	client := srv.dial()
	ctx := context.Background()

	nodes := joinAll(t, client, 5*time.Second, "node-a", "node-b", "node-c")

	killer := srv.dial()
	for _, m := range nodes {
		_, err := killer.Revoke(ctx, m.LeaseID())
		require.NoError(t, err)
	}

	for _, m := range nodes {
		require.Eventually(t, func() bool { return m.Rejoins() >= 1 }, settle, tick,
			"%s notices its own eviction", m.self.ID)
	}

	requireConverged(t, client, nodes, "node-a", "node-b", "node-c")
}

// TestMembershipRejoinsAfterExternalKeyDelete covers the case the keep-alive cannot see: the
// lease is alive and renewing, but the member key is gone. Only the node observing its own id
// in a "member left" event detects it — a contradiction, since a node is authoritative about
// its own liveness.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestMembershipRejoinsAfterExternalKeyDelete(t *testing.T) {
	srv := startEtcdServer(t)
	client := srv.dial()
	ctx := context.Background()

	nodes := joinAll(t, client, 5*time.Second, "node-a", "node-b")
	a, b := nodes[0], nodes[1]

	held := b.LeaseID()

	_, err := srv.dial().Delete(ctx, testPrefix+"node-b")
	require.NoError(t, err)

	require.Eventually(t, func() bool { return b.Rejoins() >= 1 }, settle, tick,
		"node-b re-registers off its own departure event, with its lease still alive")

	requireConverged(t, client, nodes, "node-a", "node-b")
	assert.NotEqual(t, held, b.LeaseID(), "the stale lease is not reused")
	assert.Zero(t, a.Rejoins(), "the undisturbed node did not re-register")
}

// TestMembershipRejoinIsIdempotent hits the same node repeatedly. Each loss must produce
// exactly one registration — never a duplicate key, never a node stuck absent — since in the
// field the trigger recurs (a flapping etcd, a node that keeps stalling).
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestMembershipRejoinIsIdempotent(t *testing.T) {
	srv := startEtcdServer(t)
	client := srv.dial()
	ctx := context.Background()

	nodes := joinAll(t, client, 5*time.Second, "node-a", "node-b")
	b := nodes[1]

	killer := srv.dial()

	const rounds = 5

	for i := range rounds {
		lost := b.LeaseID()
		before := b.Rejoins()

		_, err := killer.Revoke(ctx, lost)
		require.NoError(t, err)

		require.Eventually(t, func() bool { return b.Rejoins() > before && b.LeaseID() != lost }, settle, tick,
			"round %s: node-b re-registers", strconv.Itoa(i))
		requireConverged(t, client, nodes, "node-a", "node-b")
	}

	assert.GreaterOrEqual(t, b.Rejoins(), int64(rounds))
}

// TestMembershipRejoinsAfterEtcdRestart is the original report: etcd goes away entirely for
// longer than the TTL and comes back. Nothing about the nodes changes — no restart, no new
// client — and the ring has to reassemble itself. This one does spend real time, because
// nothing but a real TTL can expire a lease against an absent server; the TTL is short.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestMembershipRejoinsAfterEtcdRestart(t *testing.T) {
	srv := startEtcdServer(t)
	client := srv.dial()

	nodes := joinAll(t, client, 2*time.Second, "node-a", "node-b")

	srv.stop()

	for _, m := range nodes {
		require.Eventually(t, m.SelfAbsent, settle, tick,
			"%s notices it is absent while etcd is down", m.self.ID)
	}

	srv.start()

	requireConverged(t, client, nodes, "node-a", "node-b")
}

// TestOwnershipRebindsToNewLease pins the coupling the rejoin creates: compaction claims hang
// off the membership lease, so a lease replaced by a re-registration takes every claim with it.
// Without the rebind the node keeps believing it owns shards whose claims no longer exist, and
// its next Reconcile writes nothing — a shard with no compaction owner at all.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestOwnershipRebindsToNewLease(t *testing.T) {
	srv := startEtcdServer(t)
	client := srv.dial()
	ctx := context.Background()

	first, err := client.Grant(ctx, 30)
	require.NoError(t, err)

	own := NewOwnership(client, testRoot, "node-a", first.ID)
	r := ring.New(ring.Node{ID: "node-a"})
	shards := []string{"tenant-1"}

	owned, err := own.Reconcile(ctx, r, shards)
	require.NoError(t, err)
	require.Equal(t, shards, owned)

	// The lease dies, and the claim with it.
	_, err = client.Revoke(ctx, first.ID)
	require.NoError(t, err)

	claims, err := own.Claims(ctx)
	require.NoError(t, err)
	require.Empty(t, claims, "the claim died with the lease")

	second, err := client.Grant(ctx, 30)
	require.NoError(t, err)

	own.SetLease(second.ID)
	assert.Empty(t, own.Owned(), "claims lost with the old lease are no longer believed held")

	// Without the rebind this pass is a no-op — the node still believes it holds the shard —
	// and the shard ends up with no compaction owner anywhere in the cluster.
	owned, err = own.Reconcile(ctx, r, shards)
	require.NoError(t, err)
	require.Equal(t, shards, owned)

	resp, err := client.Get(ctx, joinKey(testRoot, "owners")+"tenant-1")
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 1)
	assert.Equal(t, int64(second.ID), resp.Kvs[0].GetLease(), "the claim hangs off the new lease")
}

// TestWatchObserverNeverRejoins guards the observer path: a router registers nothing, holds no
// lease, and must not acquire one by way of the rejoin machinery.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestWatchObserverNeverRejoins(t *testing.T) {
	srv := startEtcdServer(t)
	client := srv.dial()
	ctx := context.Background()

	nodes := joinAll(t, client, 5*time.Second, "node-a")

	obs, err := Watch(ctx, client, testRoot)
	require.NoError(t, err)

	t.Cleanup(func() { _ = obs.Close(ctx) })

	require.Eventually(t, func() bool { return obs.Ring().Len() == 1 }, settle, tick)

	_, err = srv.dial().Revoke(ctx, nodes[0].LeaseID())
	require.NoError(t, err)

	require.Eventually(t, func() bool { return nodes[0].Rejoins() >= 1 }, settle, tick)
	requireConverged(t, client, nodes, "node-a")

	require.Eventually(t, func() bool { return obs.Ring().Len() == 1 }, settle, tick,
		"the observer follows the node back into the ring")

	assert.Equal(t, clientv3.NoLease, obs.LeaseID())
	assert.Zero(t, obs.Rejoins())
	assert.False(t, obs.SelfAbsent())
	assert.Len(t, memberKeys(client), 1, "the observer registered nothing")
}
