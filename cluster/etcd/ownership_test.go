package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/cluster/rebalance"
	"github.com/oteldb/storage/cluster/ring"
)

func twoOwners(t *testing.T) (ctx context.Context, a, b *Ownership) {
	t.Helper()

	client := startEtcd(t)
	ctx = context.Background()

	la, err := client.Grant(ctx, 30)
	require.NoError(t, err)
	lb, err := client.Grant(ctx, 30)
	require.NoError(t, err)

	return ctx, NewOwnership(client, "/oteldb", "node-a", la.ID), NewOwnership(client, "/oteldb", "node-b", lb.ID)
}

//nolint:paralleltest // owns an embedded etcd
func TestOwnershipExclusiveAndIdempotent(t *testing.T) {
	ctx, a, b := twoOwners(t)

	ok, err := a.Acquire(ctx, "t1")
	require.NoError(t, err)
	assert.True(t, ok, "first claim succeeds")

	ok, err = a.Acquire(ctx, "t1")
	require.NoError(t, err)
	assert.True(t, ok, "re-claim by the holder is idempotent")

	ok, err = b.Acquire(ctx, "t1")
	require.NoError(t, err)
	assert.False(t, ok, "another node cannot claim a held shard")

	// Release frees it for the other node.
	require.NoError(t, a.Release(ctx, "t1"))
	ok, err = b.Acquire(ctx, "t1")
	require.NoError(t, err)
	assert.True(t, ok, "after release the other node claims it")
}

//nolint:paralleltest // owns an embedded etcd
func TestOwnershipReleaseIsGuarded(t *testing.T) {
	ctx, a, b := twoOwners(t)

	_, err := a.Acquire(ctx, "t1")
	require.NoError(t, err)

	// B releasing a shard it does not hold must not free A's claim.
	require.NoError(t, b.Release(ctx, "t1"))
	ok, err := b.Acquire(ctx, "t1")
	require.NoError(t, err)
	assert.False(t, ok, "A still holds the shard")
}

//nolint:paralleltest // owns an embedded etcd
func TestOwnershipReconcileSplitsByPrimary(t *testing.T) {
	ctx, a, b := twoOwners(t)

	r := ring.New(ring.Node{ID: "node-a"}, ring.Node{ID: "node-b"})
	shards := []string{"t1", "t2", "t3", "t4", "t5", "t6"}

	ownedA, err := a.Reconcile(ctx, r, shards)
	require.NoError(t, err)
	ownedB, err := b.Reconcile(ctx, r, shards)
	require.NoError(t, err)

	// Every shard is owned by exactly one node — its ring primary.
	assert.Equal(t, len(shards), len(ownedA)+len(ownedB), "each shard owned once")

	ownedAll := map[string]bool{}
	for _, s := range append(append([]string{}, ownedA...), ownedB...) {
		assert.False(t, ownedAll[s], "no shard owned twice")
		ownedAll[s] = true
	}

	for _, s := range shards {
		p, _ := r.Primary([]byte(s))
		if p.ID == "node-a" {
			assert.Contains(t, ownedA, s)
		} else {
			assert.Contains(t, ownedB, s)
		}
	}
}

//nolint:paralleltest // owns an embedded etcd
func TestOwnershipReconcileMinimalMoveAndHandoffPlan(t *testing.T) {
	ctx, a, b := twoOwners(t)

	r1 := ring.New(ring.Node{ID: "node-a"}, ring.Node{ID: "node-b"})
	shards := []string{"t1", "t2", "t3", "t4", "t5", "t6"}

	ownedA, err := a.Reconcile(ctx, r1, shards)
	require.NoError(t, err)
	ownedB, err := b.Reconcile(ctx, r1, shards)
	require.NoError(t, err)

	// Owned() mirrors the reconcile result, sorted and stable across an unchanged-ring re-pass.
	assert.Equal(t, ownedA, a.Owned())
	again, err := a.Reconcile(ctx, r1, shards)
	require.NoError(t, err)
	assert.Equal(t, ownedA, again, "re-reconciling the same ring is a no-op for ownership")
	assert.Empty(t, a.LastPlan(), "no ring change ⇒ no handoff plan")

	// Remove node-b: node-a becomes primary of every shard, node-b of none. Handoff converges
	// over repeated passes — node-b must release a claim before node-a can acquire it — exactly
	// as the maintenance loop reconciles every tick.
	r2 := r1.Without("node-b")

	var ownedA2 []string

	require.Eventually(t, func() bool {
		_, err := b.Reconcile(ctx, r2, shards) // releases the shards it no longer owns
		require.NoError(t, err)
		ownedA2, err = a.Reconcile(ctx, r2, shards) // acquires the freed shards
		require.NoError(t, err)

		return len(ownedA2) == len(shards)
	}, 2*time.Second, 20*time.Millisecond, "node-a takes over every shard")

	assert.ElementsMatch(t, shards, ownedA2)
	assert.Empty(t, b.Owned(), "node-b (gone from the ring) releases everything")

	// node-a's handoff plan records exactly the shards node-b previously owned, each as a
	// one-in (node-a) / one-out (node-b) primary move.
	plan := a.LastPlan()
	assert.ElementsMatch(t, ownedB, shardsOf(plan), "plan covers the shards that changed primary")

	for _, r := range plan {
		assert.Equal(t, []string{"node-a"}, r.Added)
		assert.Equal(t, []string{"node-b"}, r.Removed)
	}
}

func shardsOf(rs []rebalance.Reassignment) []string {
	out := make([]string, len(rs))
	for i, r := range rs {
		out[i] = r.Shard
	}

	return out
}

//nolint:paralleltest // owns an embedded etcd
func TestOwnershipHandoffOnLeaseExpiry(t *testing.T) {
	client := startEtcd(t)
	ctx := context.Background()

	la, err := client.Grant(ctx, 1) // short TTL
	require.NoError(t, err)
	lb, err := client.Grant(ctx, 30)
	require.NoError(t, err)

	a := NewOwnership(client, "/oteldb", "node-a", la.ID)
	b := NewOwnership(client, "/oteldb", "node-b", lb.ID)

	ok, err := a.Acquire(ctx, "t1")
	require.NoError(t, err)
	require.True(t, ok)

	// Simulate node A crashing: revoke its lease, freeing the claim.
	_, err = client.Revoke(ctx, la.ID)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		ok, err := b.Acquire(ctx, "t1")

		return err == nil && ok
	}, 5*time.Second, 100*time.Millisecond, "the surviving node takes over the orphaned shard")
}
