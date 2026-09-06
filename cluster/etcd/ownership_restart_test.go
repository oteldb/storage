package etcd

// Restart-within-the-TTL fault cases for compaction claims. A crashed node's membership lease
// outlives the process by up to the TTL, so its claim keys are still in etcd when the replacement
// incarnation starts under a fresh lease. These assert that adoption rebinds the claim to the live
// lease rather than inheriting a key etcd is about to delete.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// claimLease returns the lease a shard's claim key is bound to, and whether the key exists.
func claimLease(t *testing.T, client *clientv3.Client, shard string) (clientv3.LeaseID, bool) {
	t.Helper()

	resp, err := client.Get(context.Background(), "/oteldb/owners/"+shard)
	require.NoError(t, err)

	if len(resp.Kvs) == 0 {
		return 0, false
	}

	return clientv3.LeaseID(resp.Kvs[0].GetLease()), true
}

// TestOwnershipRestartRebindsClaimToLiveLease crashes a node and restarts it under the same ring id
// inside the old lease's TTL. The replacement adopts the claim its dead incarnation left behind, so
// the claim must be rebound to the live lease: when the dead lease finally expires the shard is
// still claimed, still this node's, and still closed to a peer.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestOwnershipRestartRebindsClaimToLiveLease(t *testing.T) {
	client := startEtcd(t)
	ctx := context.Background()

	dead, err := client.Grant(ctx, 30)
	require.NoError(t, err)

	before := NewOwnership(client, "/oteldb", "a", dead.ID)
	r := ringOf("a", "b")
	shards := []string{"t1"}

	// The shard must be one node a is primary of, or the reconcile below claims nothing.
	primary, ok := r.Primary([]byte("t1"))
	require.True(t, ok)
	require.Equal(t, "a", primary.ID, "test fixture: t1 must hash to node a")

	owned, err := before.Reconcile(ctx, r, shards)
	require.NoError(t, err)
	require.Equal(t, shards, owned)

	lease, exists := claimLease(t, client, "t1")
	require.True(t, exists)
	require.Equal(t, dead.ID, lease)

	// Crash and restart within the TTL: the process is gone but its lease is still live, so the
	// claim key is still there. The replacement registers under a fresh lease and starts with an
	// empty held set, exactly as a newly-opened Storage does.
	live, err := client.Grant(ctx, 30)
	require.NoError(t, err)

	after := NewOwnership(client, "/oteldb", "a", live.ID)

	owned, err = after.Reconcile(ctx, r, shards)
	require.NoError(t, err)
	require.Equal(t, shards, owned, "the restarted node reclaims the shard it is primary of")

	lease, exists = claimLease(t, client, "t1")
	require.True(t, exists)
	assert.Equal(t, live.ID, lease, "adoption must rebind the claim to the live lease")

	// The dead lease reaches its TTL. Revoking is the same event, without the wait.
	_, err = client.Revoke(ctx, dead.ID)
	require.NoError(t, err)

	_, exists = claimLease(t, client, "t1")
	require.True(t, exists, "the claim must outlive the dead incarnation's lease")

	term, held := after.Term("t1")
	assert.True(t, held, "the node still believes it holds the shard")
	assert.NotZero(t, term)

	// The exactly-one-flusher arbitration must still hold: a peer promoted to primary cannot take a
	// claim this node is (correctly) still flushing under.
	b := ownerOn(t, client, "b")

	_, acquired, err := b.Acquire(ctx, "t1")
	require.NoError(t, err)
	assert.False(t, acquired, "a peer must not acquire a shard this node still holds")
}

// TestOwnershipRestartAdoptionAdvancesTerm asserts the restart opens a new tenure: the adopted
// claim's term must be above the dead incarnation's, so the bucket index generation this node
// stamps outranks everything written under the tenure that died.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestOwnershipRestartAdoptionAdvancesTerm(t *testing.T) {
	client := startEtcd(t)
	ctx := context.Background()

	dead, err := client.Grant(ctx, 30)
	require.NoError(t, err)

	before := NewOwnership(client, "/oteldb", "a", dead.ID)

	deadTerm, ok, err := before.Acquire(ctx, "t1")
	require.NoError(t, err)
	require.True(t, ok)

	live, err := client.Grant(ctx, 30)
	require.NoError(t, err)

	after := NewOwnership(client, "/oteldb", "a", live.ID)

	liveTerm, ok, err := after.Acquire(ctx, "t1")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Greater(t, liveTerm, deadTerm, "a restart is a new tenure and must take a higher term")

	// The rebound claim is still stable across repeated reconciles.
	again, ok, err := after.Acquire(ctx, "t1")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, liveTerm, again, "reacquiring a held claim keeps its term")
}

// TestOwnershipRestartDoesNotStealPeerClaim guards the rebinding against over-reach: a claim whose
// value is another node's is never rebound, whatever lease it hangs off.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestOwnershipRestartDoesNotStealPeerClaim(t *testing.T) {
	client := startEtcd(t)
	ctx := context.Background()

	b := ownerOn(t, client, "b")

	_, ok, err := b.Acquire(ctx, "t1")
	require.NoError(t, err)
	require.True(t, ok)

	lease, exists := claimLease(t, client, "t1")
	require.True(t, exists)

	a := ownerOn(t, client, "a")

	_, ok, err = a.Acquire(ctx, "t1")
	require.NoError(t, err)
	assert.False(t, ok)

	got, exists := claimLease(t, client, "t1")
	require.True(t, exists)
	assert.Equal(t, lease, got, "a peer's claim keeps its own lease")

	claims := readClaims(t, client, []string{"t1"})
	assert.Equal(t, "b", claims["t1"])
}
