package storage

import (
	"context"
	"io"
	"net"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/cluster/etcd"
	"github.com/oteldb/storage/query/fetch"
)

// etcdProxy is a TCP relay in front of an embedded etcd that a test can cut and restore. It is how
// a node is partitioned *from etcd alone*: killing the etcd server would partition every node, and
// revoking a lease under a node that can still reach etcd only makes it re-register at once. The
// listener stays bound while cut (connections are accepted and immediately closed), so restoring is
// a flag flip rather than a race to reclaim the port.
type etcdProxy struct {
	endpoint string
	target   string

	mu      sync.Mutex
	severed bool
	conns   []net.Conn
}

func newEtcdProxy(t *testing.T, endpoint string) *etcdProxy {
	t.Helper()

	u, err := url.Parse(endpoint)
	require.NoError(t, err)

	var lc net.ListenConfig

	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	p := &etcdProxy{endpoint: (&url.URL{Scheme: httpScheme, Host: ln.Addr().String()}).String(), target: u.Host}
	t.Cleanup(p.restore) // heal before the nodes close, so their lease revoke is not a 5s timeout

	go p.serve(ln)

	return p
}

func (p *etcdProxy) serve(ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}

		p.mu.Lock()
		severed := p.severed

		if !severed {
			p.conns = append(p.conns, conn)
		}
		p.mu.Unlock()

		if severed {
			_ = conn.Close()

			continue
		}

		go p.pipe(conn)
	}
}

func (p *etcdProxy) pipe(conn net.Conn) {
	var d net.Dialer

	up, err := d.DialContext(context.Background(), "tcp", p.target)
	if err != nil {
		_ = conn.Close()

		return
	}

	p.mu.Lock()
	p.conns = append(p.conns, up)
	p.mu.Unlock()

	go func() { _, _ = io.Copy(up, conn); _ = up.Close() }()

	_, _ = io.Copy(conn, up)
	_ = conn.Close()
}

// cut partitions the node behind the proxy from etcd: no new connection is relayed and every open
// one is dropped.
func (p *etcdProxy) cut() {
	p.mu.Lock()
	p.severed = true
	conns := p.conns
	p.conns = nil
	p.mu.Unlock()

	for _, c := range conns {
		_ = c.Close()
	}
}

func (p *etcdProxy) restore() {
	p.mu.Lock()
	p.severed = false
	p.mu.Unlock()
}

// TestClusterFencedPrimaryRejectsWrites reproduces issue #390: a node partitioned from etcd keeps
// resolving itself as its shards' primary from a ring frozen at its last etcd view, and — before
// the fix — acknowledged writes for them long after another node had taken them over. Those writes
// go nowhere the cluster will ever read them.
//
// The partition is real (an etcd proxy is cut), the takeover is real (the lease is revoked, exactly
// as its expiry would, so the member key and the compaction claim both go), and no TTL is waited
// out: the fence is a deadline the node computes for itself, so moving the node's own clock past it
// is the whole of "the lease has lapsed".
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestClusterFencedPrimaryRejectsWrites(t *testing.T) {
	endpoint := startEtcd(t)
	ctx := context.Background()

	proxies := map[string]*etcdProxy{"node-a": newEtcdProxy(t, endpoint), "node-b": newEtcdProxy(t, endpoint)}
	nodes := map[string]*Storage{
		"node-a": openClusterNodeWith(t, proxies["node-a"].endpoint, "node-a", backend.Memory()),
		"node-b": openClusterNodeWith(t, proxies["node-b"].endpoint, "node-b", backend.Memory()),
	}

	awaitMembership(t, nodes)

	// The write is only routed to the node under test if that node is the tenant's ring primary,
	// so the partitioned node is chosen rather than assumed.
	primary, ok := nodes["node-a"].cluster.membership.Ring().Primary([]byte("default"))
	require.True(t, ok)

	victim, victimProxy := nodes[primary.ID], proxies[primary.ID]

	var survivor *Storage
	for id, s := range nodes {
		if id != primary.ID {
			survivor = s
		}
	}

	require.NotNil(t, survivor)

	// Baseline: the primary acks, and the write is genuinely in the cluster (RF=2 replicates it).
	_, err := victim.WriteMetrics(ctx, gaugeBatch("svc", "fenced_metric", []int64{100}, []float64{1}))
	require.NoError(t, err)
	assert.Equal(t, []float64{1}, readSamples(t, survivor, "fenced_metric"))

	lease := victim.cluster.membership.LeaseID()

	// Partition the primary from etcd, then let its lease go, which is what hands the shard on:
	// the member key and the compaction claim both hang off it.
	victimProxy.cut()

	direct, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
	require.NoError(t, err)

	defer func() { _ = direct.Close() }()

	_, err = direct.Revoke(ctx, lease)
	require.NoError(t, err)

	// The survivor is now the whole cluster, so it is the shard's primary and its owner set.
	require.Eventually(t, func() bool { return ringSize(survivor) == 1 }, 10*time.Second, 20*time.Millisecond,
		"the survivor drops the partitioned node from its ring")

	// The partitioned node's own view is frozen: it still believes both nodes are present and that
	// it is the tenant's primary. That is precisely why it must fence itself.
	assert.Equal(t, 2, ringSize(victim), "the partitioned node's ring is frozen at its last etcd view")

	// Move the node past its own lease deadline. Nothing sleeps: the deadline is last keep-alive +
	// TTL − margin, and the node compares it against this clock.
	victim.cluster.membership.SetClock(func() time.Time { return time.Now().Add(2 * etcd.DefaultTTL) })

	require.True(t, victim.cluster.membership.Fenced(), "past the lease deadline the node is fenced")
	assert.Empty(t, victim.cluster.ownership.Owned(), "a fenced node owns nothing: it neither flushes nor stamps an index")

	// The defect: this write is routed to the partitioned node as "primary" and must not be acked.
	_, err = victim.WriteMetrics(ctx, gaugeBatch("svc", "fenced_metric", []int64{200}, []float64{2}))
	require.Error(t, err, "a fenced node must not acknowledge a write the cluster will not serve")
	require.ErrorIs(t, err, cluster.ErrNotPrimary)

	// And the reason it must not: the cluster cannot serve it.
	assert.Equal(t, []float64{1}, readSamples(t, survivor, "fenced_metric"),
		"the shard's real owner never saw the fenced write")

	// Fencing is write-only. Reads stay served from what the node already holds.
	assert.Equal(t, []float64{1}, readSamples(t, victim, "fenced_metric"), "a fenced node keeps serving reads")

	// Recovery: the partition heals, the node re-registers under a fresh lease, and the fence lifts
	// on the next keep-alive — no restart, no operator action.
	victimProxy.restore()
	victim.cluster.membership.SetClock(nil)

	require.Eventually(t, func() bool { return !victim.cluster.membership.Fenced() && ringSize(victim) >= 1 },
		30*time.Second, 50*time.Millisecond, "the fence lifts once the lease is provable again")

	_, err = victim.WriteMetrics(ctx, gaugeBatch("svc", "fenced_metric", []int64{300}, []float64{3}))
	require.NoError(t, err, "a node that can prove its lease again accepts writes")
}

// readSamples drains one metric's values from s, sorted by the order the fetch returns them.
func readSamples(t *testing.T, s *Storage, name string) []float64 {
	t.Helper()

	it, err := s.Fetcher("default").Fetch(context.Background(), fetch.Request{
		Start: 0, End: 1 << 60, Matchers: []fetch.Matcher{nameMatcher(name)},
	})
	require.NoError(t, err)

	got, err := fetch.Drain(context.Background(), it)
	require.NoError(t, err)

	var out []float64
	for _, b := range got {
		out = append(out, b.Values...)
	}

	return out
}
