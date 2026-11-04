package etcd

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/oteldb/storage/cluster/etcd/etcdtest"
)

//nolint:paralleltest // owns an embedded etcd
func TestRejoinKeepsFenceUntilOwnershipReset(t *testing.T) {
	client := etcdtest.NewServer(t).Dial()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	m := &Membership{client: client, prefix: testPrefix, self: Member{ID: "node-a"}, ttl: DefaultTTL}
	lease, err := m.register(ctx)
	require.NoError(t, err)
	m.leaseID.Store(int64(lease))
	own := NewOwnership(client, testRoot, "node-a", lease)
	own.SetFence(m.Fenced)
	oldTerm, held, err := own.Ensure(ctx, "shard")
	require.NoError(t, err)
	require.True(t, held)
	require.False(t, m.Fenced())
	entered, release := make(chan struct{}), make(chan struct{})
	m.OnRejoin(func(id clientv3.LeaseID) {
		close(entered)
		<-release
		own.SetLease(id)
	})
	done := make(chan struct{})
	var rejoined bool
	m.selfAbsent.Store(true)
	go func() { rejoined = m.rejoin(ctx); close(done) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-release:
		default:
			close(release)
		}
		<-done
	})
	<-entered
	require.True(t, m.Fenced(), "a fresh lease must not expose claims from the old lease")
	_, held = own.Term("shard")
	require.False(t, held)
	close(release)
	<-done
	require.True(t, rejoined)
	require.False(t, m.Fenced())
	require.NotEqual(t, lease, m.LeaseID())
	_, held = own.Term("shard")
	require.False(t, held, "ownership reset precedes lifting the fence")
	term, held, err := own.Ensure(ctx, "shard")
	require.NoError(t, err)
	require.True(t, held)
	require.Greater(t, term, oldTerm)
}

//nolint:paralleltest // owns an embedded etcd
func TestCancelledRejoinKeepsFence(t *testing.T) {
	client := etcdtest.NewServer(t).Dial()
	m := &Membership{client: client, prefix: testPrefix, self: Member{ID: "node-a"}, ttl: DefaultTTL}
	lease, err := m.register(t.Context())
	require.NoError(t, err)
	m.leaseID.Store(int64(lease))
	require.False(t, m.Fenced())
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	m.selfAbsent.Store(true)
	require.False(t, m.rejoin(ctx))
	require.True(t, m.Fenced())
	require.Equal(t, lease, m.LeaseID())
}
