package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/cluster/etcd"
	"github.com/oteldb/storage/signal"
)

// openRepairNode opens a shared-nothing clustered node at an explicit replication factor, which is
// what decides whether a repair pass can ever see the shard's complete owner set.
func openRepairNode(t *testing.T, endpoint, id string, rf int) *Storage {
	t.Helper()

	s, err := Open(context.Background(), Options{},
		WithBackend(backend.Memory()),
		WithCluster(&cluster.Config{
			Etcd:           []string{endpoint},
			Self:           etcd.Member{ID: id, Addr: "127.0.0.1:0"},
			RF:             rf,
			PrivateBackend: true,
		}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	return s
}

// TestRepairCompleteOwnerSetIsAbsence verifies the one answer that may become a hole: every owner
// the shard is expected to have was reachable, and none of them held the part.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestRepairCompleteOwnerSetIsAbsence(t *testing.T) {
	endpoint := startEtcd(t)
	ctx := context.Background()

	a := openRepairNode(t, endpoint, "a", 2)
	b := openRepairNode(t, endpoint, "b", 2)
	awaitMembership(t, map[string]*Storage{"a": a, "b": b})

	tid := a.normalizeTenant("acme")

	complete, remotes := a.completeOwners(tid)
	require.True(t, complete, "both owners of an RF=2 shard are up and resolvable")
	require.Len(t, remotes, 1)

	r := a.repairerFor(tid, "acme/metrics")
	require.NotNil(t, r)

	_, outcome, err := r.FetchWant(ctx, bucketindex.Want{Prefix: "acme/metrics/0000000001"})
	require.NoError(t, err)
	assert.Equal(t, bucketindex.WantAbsent, outcome, "no owner holds it, and every owner answered")
}

// TestRepairShortOwnerSetIsIncomplete is the cluster half of the guard: the ring cannot fill the
// shard's owner set — the shape a rolling restart takes, with the restarting owner deregistered —
// so the peers that answer are a strict subset and their answer is not evidence.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestRepairShortOwnerSetIsIncomplete(t *testing.T) {
	endpoint := startEtcd(t)
	ctx := context.Background()

	a := openRepairNode(t, endpoint, "a", 3)
	b := openRepairNode(t, endpoint, "b", 3)
	awaitMembership(t, map[string]*Storage{"a": a, "b": b})

	tid := a.normalizeTenant("acme")

	complete, remotes := a.completeOwners(tid)
	assert.False(t, complete, "two nodes cannot be the whole owner set of an RF=3 shard")
	assert.Len(t, remotes, 1, "the peer that is up is still asked — repair proceeds, loss is not concluded")

	r := a.repairerFor(tid, "acme/metrics")
	require.NotNil(t, r)

	_, outcome, err := r.FetchWant(ctx, bucketindex.Want{Prefix: "acme/metrics/0000000001"})
	require.NoError(t, err)
	assert.Equal(t, bucketindex.WantIncomplete, outcome,
		"absence over a subset of the owners must never read as absence")
}

// TestRepairSingleOwnerShardIsComplete verifies the degenerate case is not treated as unknowable:
// at RF=1 this node is the whole owner set, so its own absence is definitive.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestRepairSingleOwnerShardIsComplete(t *testing.T) {
	endpoint := startEtcd(t)
	ctx := context.Background()

	a := openRepairNode(t, endpoint, "solo", 1)
	awaitMembership(t, map[string]*Storage{"solo": a})

	tid := a.normalizeTenant("acme")

	complete, remotes := a.completeOwners(tid)
	assert.True(t, complete)
	assert.Empty(t, remotes)

	r := a.repairerFor(tid, "acme/metrics")
	require.NotNil(t, r)

	_, outcome, err := r.FetchWant(ctx, bucketindex.Want{Prefix: "acme/metrics/0000000001"})
	require.NoError(t, err)
	assert.Equal(t, bucketindex.WantAbsent, outcome)
}

// TestRepairerAbsentWithoutPrivateBackend verifies the seam stays nil where there is nothing to
// copy: on a shared store every replica reads the same objects.
func TestRepairerAbsentWithoutPrivateBackend(t *testing.T) {
	t.Parallel()

	s := &Storage{}
	assert.Nil(t, s.repairerFor(signal.TenantID("acme"), "acme/metrics"))
	assert.Nil(t, s.metricRepairerFor(signal.TenantID("acme"), "acme/metrics"))
	assert.Nil(t, s.recordRepairerFor(signal.TenantID("acme"), "acme/metrics"))
}
