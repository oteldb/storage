package etcd

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/oteldb/storage/cluster/ring"
)

//nolint:paralleltest // owns an embedded etcd
func TestOwnershipEnsureTracksIndependentClaims(t *testing.T) {
	ctx, a, b := twoOwners(t)
	for _, key := range []string{"a", "b"} {
		term, ok, err := a.Ensure(ctx, key)
		require.NoError(t, err)
		require.True(t, ok)
		cached, held := a.Term(key)
		require.True(t, held)
		require.Equal(t, term, cached)
		_, ok, err = b.Ensure(ctx, key)
		require.NoError(t, err)
		require.False(t, ok)
	}
	require.Equal(t, []string{"a", "b"}, a.Owned())
	require.NoError(t, a.Release(ctx, "a"))
	_, held := a.Term("a")
	require.False(t, held, "release must invalidate a tracked claim")
	require.Equal(t, []string{"b"}, a.Owned())
	_, ok, err := b.Ensure(ctx, "a")
	require.NoError(t, err)
	require.True(t, ok)
}

func TestOwnershipEnsureWarmAndFenced(t *testing.T) {
	t.Parallel()
	o := NewOwnership(nil, "/test", "node", 1)
	o.held["warm"] = 42
	term, ok, err := o.Ensure(t.Context(), "warm")
	require.NoError(t, err)
	require.True(t, ok)
	require.EqualValues(t, 42, term)
	o.SetFence(func() bool { return true })
	_, ok, err = o.Ensure(t.Context(), "warm")
	require.NoError(t, err)
	require.False(t, ok)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, ok, err = o.Ensure(ctx, "cold")
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, ok)
}

func TestOwnershipEnsureSerializesLifecycle(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		change func(*Ownership)
	}{
		{"new lease", func(o *Ownership) { o.SetLease(2) }},
		{"forget", func(o *Ownership) { o.Forget() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				kv := &claimGateKV{entered: make(chan struct{}), release: make(chan struct{})}
				o := NewOwnership(&clientv3.Client{KV: kv}, "/test", "node", 1)
				ensured := make(chan error, 1)
				go func() { _, _, err := o.Ensure(ctx, "shard"); ensured <- err }()
				<-kv.entered
				if o.mu.TryLock() {
					o.mu.Unlock()
					t.Fatal("claim bookkeeping is not protected during acquisition")
				}
				changed := make(chan struct{})
				go func() { tc.change(o); close(changed) }()

				close(kv.release)
				require.NoError(t, <-ensured)
				<-changed
				_, held := o.Term("shard")
				require.False(t, held, "an old acquisition must not publish a term after the lifecycle change")
			})
		})
	}
}

func TestOwnershipEnsureChecksFenceAfterAcquisition(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		kv := &claimGateKV{entered: make(chan struct{}), release: make(chan struct{})}
		o := NewOwnership(&clientv3.Client{KV: kv}, "/test", "node", 1)
		var fenced atomic.Bool
		o.SetFence(fenced.Load)
		done := make(chan struct{})
		var term uint64
		var held bool
		var err error
		go func() { term, held, err = o.Ensure(ctx, "shard"); close(done) }()
		<-kv.entered
		fenced.Store(true)
		close(kv.release)
		<-done
		require.NoError(t, err)
		require.False(t, held)
		require.Zero(t, term)
		fenced.Store(false)
		assert.Empty(t, o.Owned(), "a fenced acquisition was never published locally")
	})
}

type claimGateKV struct {
	clientv3.KV

	entered, release chan struct{}
}

func (kv *claimGateKV) Txn(ctx context.Context) clientv3.Txn {
	return &claimGateTxn{commit: func() (*clientv3.TxnResponse, error) {
		close(kv.entered)
		select {
		case <-kv.release:
			return &clientv3.TxnResponse{Succeeded: true, Header: &etcdserverpb.ResponseHeader{Revision: 42}}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}}
}

type claimGateTxn struct {
	commit func() (*clientv3.TxnResponse, error)
}

func (t *claimGateTxn) If(...clientv3.Cmp) clientv3.Txn        { return t }
func (t *claimGateTxn) Then(...clientv3.Op) clientv3.Txn       { return t }
func (t *claimGateTxn) Else(...clientv3.Op) clientv3.Txn       { return t }
func (t *claimGateTxn) Commit() (*clientv3.TxnResponse, error) { return t.commit() }

//nolint:paralleltest // owns an embedded etcd
func TestOwnershipReleaseFailureForgetsClaim(t *testing.T) {
	ctx, a, _ := twoOwners(t)
	for _, tc := range []struct {
		name    string
		release func(context.Context) error
	}{
		{"explicit", func(ctx context.Context) error { return a.Release(ctx, "shard") }},
		{"reconcile", func(ctx context.Context) error { _, err := a.Reconcile(ctx, ring.New(), nil); return err }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, held, err := a.Ensure(ctx, "shard")
			require.NoError(t, err)
			require.True(t, held)
			failed, cancel := context.WithCancel(ctx)
			cancel()
			require.Error(t, tc.release(failed))
			_, held = a.Term("shard")
			require.False(t, held, "a failed release response cannot prove the claim still exists")
			_, held, err = a.Ensure(ctx, "shard")
			require.NoError(t, err)
			require.True(t, held, "a subsequent write verifies the claim again")
		})
	}
}

//nolint:paralleltest // owns an embedded etcd
func TestOwnershipReleasePreservesReplacementLease(t *testing.T) {
	ctx, old, _ := twoOwners(t)
	oldTerm, held, err := old.Ensure(ctx, "shard")
	require.NoError(t, err)
	require.True(t, held)
	lease, err := old.client.Grant(ctx, 30)
	require.NoError(t, err)
	fresh := NewOwnership(old.client, "/oteldb", old.id, lease.ID)
	freshTerm, held, err := fresh.Ensure(ctx, "shard")
	require.NoError(t, err)
	require.True(t, held)
	require.Greater(t, freshTerm, oldTerm)
	require.NoError(t, old.Release(ctx, "shard"))
	_, held = old.Term("shard")
	require.False(t, held)
	reply, err := old.client.Get(ctx, fresh.prefix+"shard")
	require.NoError(t, err)
	require.Len(t, reply.Kvs, 1, "the stale incarnation must not delete its replacement's claim")
	require.Equal(t, int64(lease.ID), reply.Kvs[0].GetLease())
	term, held := fresh.Term("shard")
	require.True(t, held)
	require.Equal(t, freshTerm, term)
}

func TestOwnershipFenceAfterLockWait(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		held func(*Ownership) bool
	}{
		{"term", func(o *Ownership) bool { _, held := o.Term("shard"); return held }},
		{"owned", func(o *Ownership) bool { return len(o.Owned()) != 0 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			o := NewOwnership(nil, "/test", "node", 1)
			o.held["shard"] = 42
			var fenced atomic.Bool
			checked := make(chan struct{}, 1)
			o.SetFence(func() bool {
				if fenced.Load() {
					return true
				}
				select {
				case checked <- struct{}{}:
				default:
				}
				return false
			})
			o.mu.Lock()
			result := make(chan bool, 1)
			go func() { result <- tc.held(o) }()
			<-checked
			fenced.Store(true)
			o.mu.Unlock()
			require.False(t, <-result, "a pre-lock lease check cannot authorize a later read")
		})
	}
}

//nolint:paralleltest // owns an embedded etcd
func TestOwnershipFailedReleaseStillHandsOff(t *testing.T) {
	ctx, a, b := twoOwners(t)
	_, held, err := a.Ensure(ctx, "shard")
	require.NoError(t, err)
	require.True(t, held)
	toB := ring.New(ring.Node{ID: b.id})
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = a.Reconcile(canceled, toB, []string{"shard"})
	require.Error(t, err)
	_, held = a.Term("shard")
	require.False(t, held)
	_, err = a.Reconcile(ctx, toB, []string{"shard"})
	require.NoError(t, err)
	_, held, err = b.Ensure(ctx, "shard")
	require.NoError(t, err)
	require.True(t, held, "forgetting authority must not forget a still-live claim that needs releasing")
}

//nolint:paralleltest // owns an embedded etcd
func TestOwnershipColdClaimSurvivesStaleSnapshot(t *testing.T) {
	ctx, a, b := twoOwners(t)
	term, held, err := a.Ensure(ctx, "cold")
	require.NoError(t, err)
	require.True(t, held)
	owned, err := a.Reconcile(ctx, ring.New(ring.Node{ID: a.id}), nil)
	require.NoError(t, err)
	require.Equal(t, []string{"cold"}, owned, "a snapshot taken before engine creation cannot release its claim")
	current, held := a.Term("cold")
	require.True(t, held)
	require.Equal(t, term, current)
	owned, err = a.Reconcile(ctx, ring.New(ring.Node{ID: b.id}), nil)
	require.NoError(t, err)
	require.Empty(t, owned)
	_, held, err = b.Ensure(ctx, "cold")
	require.NoError(t, err)
	require.True(t, held, "ring movement still releases the cold claim")
}

//nolint:paralleltest // owns an embedded etcd
func TestOwnershipEnsureCancelsPendingRelease(t *testing.T) {
	ctx, a, b := twoOwners(t)
	_, held, err := a.Ensure(ctx, "shard")
	require.NoError(t, err)
	require.True(t, held)
	failed, cancel := context.WithCancel(ctx)
	cancel()
	require.Error(t, a.Release(failed, "shard"))
	_, held, err = a.Ensure(ctx, "shard")
	require.NoError(t, err)
	require.True(t, held)
	owned, err := a.Reconcile(ctx, ring.New(ring.Node{ID: a.id}), []string{"shard"})
	require.NoError(t, err)
	require.Equal(t, []string{"shard"}, owned)
	_, held, err = b.Ensure(ctx, "shard")
	require.NoError(t, err)
	require.False(t, held, "a verified claim cannot retain an earlier pending release")
}

func TestOwnershipLifecycleClearsPendingReleases(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		change func(*Ownership)
	}{
		{"new lease", func(o *Ownership) { o.SetLease(2) }},
		{"forget", func(o *Ownership) { o.Forget() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			o := NewOwnership(nil, "/test", "node", 1)
			o.held["held"] = 42
			o.pendingRelease = map[string]struct{}{"uncertain": {}}
			tc.change(o)
			require.Empty(t, o.held)
			require.Empty(t, o.pendingRelease)
		})
	}
}
