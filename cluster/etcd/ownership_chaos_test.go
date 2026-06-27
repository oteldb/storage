package etcd

// Fault testing for etcd-coordinated compaction-ownership claims under contention and churn. The
// happy-path claim/release/reconcile cases live in ownership_test.go; these assert the adversarial
// safety+liveness properties: a contended shard has exactly one winner, concurrent reconciliation
// converges to exactly-one-owner-per-shard across a membership change, and a node's death (lease
// expiry) hands all of its shards to the survivors with none left orphaned. They run against a real
// embedded etcd, so they exercise the actual CAS + lease semantics the cluster relies on.

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/oteldb/storage/cluster/ring"
)

// ownerOn returns an Ownership for id backed by its own fresh lease (a node's membership lease).
func ownerOn(t *testing.T, client *clientv3.Client, id string) *Ownership {
	t.Helper()

	l, err := client.Grant(context.Background(), 30)
	require.NoError(t, err)

	return NewOwnership(client, "/oteldb", id, l.ID)
}

func ringOf(ids ...string) *ring.Ring {
	nodes := make([]ring.Node, len(ids))
	for i, id := range ids {
		nodes[i] = ring.Node{ID: id}
	}

	return ring.New(nodes...)
}

func shardNames(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = "shard-" + string(rune('A'+i/26)) + string(rune('a'+i%26))
	}

	return out
}

// readClaims reads the authoritative owner of each shard straight from etcd: the value of the
// single claim key, or "" if unclaimed. Because each shard is one key, double-ownership is
// structurally impossible — this surfaces who actually holds it.
func readClaims(t *testing.T, client *clientv3.Client, shards []string) map[string]string {
	t.Helper()

	out := make(map[string]string, len(shards))
	for _, s := range shards {
		resp, err := client.Get(context.Background(), "/oteldb/owners/"+s)
		require.NoError(t, err)

		if len(resp.Kvs) == 1 {
			out[s] = string(resp.Kvs[0].Value)
		}
	}

	return out
}

// reconcileRounds runs every owner's Reconcile against r a few times — enough for a release in one
// round to be observed and claimed in the next (reconciliation converges over rounds, not in one).
func reconcileRounds(t *testing.T, owners []*Ownership, r *ring.Ring, shards []string, rounds int) {
	t.Helper()

	for range rounds {
		for _, o := range owners {
			_, err := o.Reconcile(context.Background(), r, shards)
			require.NoError(t, err)
		}
	}
}

// assertOwnedByPrimary asserts every shard's etcd claim is held by its ring primary.
func assertOwnedByPrimary(t *testing.T, client *clientv3.Client, r *ring.Ring, shards []string) {
	t.Helper()

	claims := readClaims(t, client, shards)
	for _, s := range shards {
		p, ok := r.Primary([]byte(s))
		require.True(t, ok)
		assert.Equalf(t, p.ID, claims[s], "shard %s must be claimed by its ring primary", s)
	}
}

// TestOwnershipConcurrentClaimSingleWinner races many nodes to claim one hot shard at once: the CAS
// must admit exactly one winner (no split-brain), every loser cleanly observing it lost.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestOwnershipConcurrentClaimSingleWinner(t *testing.T) {
	client := startEtcd(t)
	ctx := context.Background()

	const contenders = 12
	owners := make([]*Ownership, contenders)
	for i := range owners {
		owners[i] = ownerOn(t, client, "node-"+string(rune('a'+i)))
	}

	type result struct {
		ok  bool
		err error
	}
	results := make([]result, contenders)

	var wg sync.WaitGroup
	for i := range owners {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ok, err := owners[i].Acquire(ctx, "hot") // distinct index per goroutine ⇒ race-free
			results[i] = result{ok, err}
		}(i)
	}
	wg.Wait()

	wins := 0
	for _, r := range results {
		require.NoError(t, r.err)
		if r.ok {
			wins++
		}
	}
	assert.Equal(t, 1, wins, "exactly one node may hold a contended claim")
}

// TestOwnershipReconcileConvergesUnderChurn has all nodes reconcile concurrently, then removes a
// node and reconciles again, asserting that after convergence every shard is owned by exactly its
// (new) ring primary and the departed node owns nothing.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestOwnershipReconcileConvergesUnderChurn(t *testing.T) {
	client := startEtcd(t)

	ids := []string{"a", "b", "c", "d"}
	owners := make([]*Ownership, len(ids))
	for i, id := range ids {
		owners[i] = ownerOn(t, client, id)
	}
	shards := shardNames(20)

	r := ringOf(ids...)
	reconcileRounds(t, owners, r, shards, 2)
	assertOwnedByPrimary(t, client, r, shards)

	// Membership change: node d leaves. Every node reconciles against the new ring — d releases its
	// shards, and their new primaries claim them over the following rounds.
	r2 := r.Without("d")
	reconcileRounds(t, owners, r2, shards, 3)
	assertOwnedByPrimary(t, client, r2, shards)

	for s, owner := range readClaims(t, client, shards) {
		assert.NotEqualf(t, "d", owner, "departed node still holds %s", s)
	}
}

// TestOwnershipMassFailoverOnLeaseExpiry gives a node a set of shards, kills it (revokes its lease,
// auto-deleting all its claims), and asserts the survivors reconcile to take over *every* orphaned
// shard — no shard left permanently unowned after a node death.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestOwnershipMassFailoverOnLeaseExpiry(t *testing.T) {
	client := startEtcd(t)
	ctx := context.Background()

	dying, err := client.Grant(ctx, 30)
	require.NoError(t, err)
	a := NewOwnership(client, "/oteldb", "a", dying.ID)
	b := ownerOn(t, client, "b")
	c := ownerOn(t, client, "c")

	shards := shardNames(20)
	r := ringOf("a", "b", "c")
	reconcileRounds(t, []*Ownership{a, b, c}, r, shards, 2)
	assertOwnedByPrimary(t, client, r, shards)

	// Node a held at least one shard before the crash.
	var aShards int
	for _, owner := range readClaims(t, client, shards) {
		if owner == "a" {
			aShards++
		}
	}
	require.Positive(t, aShards, "node a should own some shards before it dies")

	// Crash a: revoking its lease auto-deletes every claim bound to it.
	_, err = client.Revoke(ctx, dying.ID)
	require.NoError(t, err)

	// The survivors reconcile against the new ring until every shard is owned by a live node.
	r2 := r.Without("a")
	require.Eventually(t, func() bool {
		reconcileRounds(t, []*Ownership{b, c}, r2, shards, 1)
		for _, s := range shards {
			if owner := readClaims(t, client, []string{s})[s]; owner == "" || owner == "a" {
				return false
			}
		}

		return true
	}, 10*time.Second, 100*time.Millisecond, "survivors take over all of the dead node's shards")

	assertOwnedByPrimary(t, client, r2, shards)
}
