package etcd

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// compactPast advances the store past rev and compacts, so a watch starting at rev is canceled
// by etcd — the outage-longer-than-the-compaction-interval case, reproduced exactly.
func compactPast(ctx context.Context, t *testing.T, client *clientv3.Client, rev int64) {
	t.Helper()

	for i := range 5 {
		_, err := client.Put(ctx, "/filler", strconv.Itoa(i))
		require.NoError(t, err)
	}

	resp, err := client.Get(ctx, "/filler")
	require.NoError(t, err)
	require.Greater(t, resp.Header.GetRevision(), rev)

	_, err = client.Compact(ctx, resp.Header.GetRevision(), clientv3.WithCompactPhysical())
	require.NoError(t, err)
}

func currentRev(ctx context.Context, t *testing.T, client *clientv3.Client) int64 {
	t.Helper()

	resp, err := client.Get(ctx, "/probe")
	require.NoError(t, err)

	return resp.Header.GetRevision()
}

//nolint:paralleltest // owns an embedded etcd; runs serially
func TestConsumeReportsCancellation(t *testing.T) {
	client := startEtcd(t)
	ctx := context.Background()

	m := &Membership{client: client, prefix: "/oteldb/members/", members: map[string]Member{}}

	stale := currentRev(ctx, t, client)
	compactPast(ctx, t, client, stale)

	// A canceled stream delivers nothing, which is what tells the loop to back off rather than
	// treat the end as an ordinary one.
	assert.False(t, m.consume(ctx, stale), "a compacted watch is canceled without delivering")
}

// The whole point of the loop: a canceled watch must not end membership. It resynchronizes and
// resubscribes, so both the members it missed and the ones that arrive afterwards land.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestWatchResubscribesAfterCancellation(t *testing.T) {
	client := startEtcd(t)
	ctx := context.Background()

	stale := currentRev(ctx, t, client)

	// A member that joined while nothing was watching: only a resync can find it.
	a, err := Join(ctx, client, "/oteldb", Member{ID: "node-a", Zone: "z1", Addr: "a:1"}, 5*time.Second)
	require.NoError(t, err)
	t.Cleanup(func() { _ = a.Close(context.Background()) })

	compactPast(ctx, t, client, stale)

	m := &Membership{client: client, prefix: "/oteldb/members/", members: map[string]Member{}}
	bg, cancel := context.WithCancel(ctx)
	m.cancel = cancel
	m.wg.Add(1)

	go m.watch(bg, stale)

	require.Eventually(t, func() bool { return len(m.Members()) == 1 }, 5*time.Second, 25*time.Millisecond,
		"the resync picks up the member the canceled watch never saw")
	assert.Equal(t, []string{"node-a"}, memberIDs(m.Members()))

	// A member that joins after the resync proves the watch was resubscribed, not merely
	// snapshotted once.
	b, err := Join(ctx, client, "/oteldb", Member{ID: "node-b", Zone: "z2", Addr: "b:1"}, 5*time.Second)
	require.NoError(t, err)
	t.Cleanup(func() { _ = b.Close(context.Background()) })

	require.Eventually(t, func() bool { return len(m.Members()) == 2 }, 5*time.Second, 25*time.Millisecond,
		"the new watch delivers changes made after the resync")

	cancel()
	m.wg.Wait()
}

// A key deleted while no watch was covering the gap is invisible to the keep-alive, so the
// resync is the only place the node can notice it is gone.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestResyncSignalsSelfAbsence(t *testing.T) {
	client := startEtcd(t)
	ctx := context.Background()

	m := &Membership{
		client:  client,
		prefix:  "/oteldb/members/",
		self:    Member{ID: "node-a"},
		evicted: make(chan struct{}, 1),
		members: map[string]Member{},
	}

	_, err := m.resync(ctx)
	require.NoError(t, err)

	m.checkSelf()

	select {
	case <-m.evicted:
	default:
		t.Fatal("a resync without this node's own key must signal eviction")
	}

	// Registered, the same resync is silent.
	lease, err := m.register(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = client.Revoke(context.Background(), lease) })

	_, err = m.resync(ctx)
	require.NoError(t, err)

	m.checkSelf()

	select {
	case <-m.evicted:
		t.Fatal("a resync that found this node must not signal eviction")
	default:
	}
}

// An observer holds no registration, so it has no absence to report.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestObserverCheckSelfIsNoop(t *testing.T) {
	client := startEtcd(t)

	m := &Membership{client: client, prefix: "/oteldb/members/", members: map[string]Member{}}
	m.checkSelf() // Must not panic on the nil channel.
}
