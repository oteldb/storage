package etcd

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// eventually polls cond until it holds, so a watch-driven assertion does not depend on timing.
func eventually(t *testing.T, cond func() bool, what string) {
	t.Helper()

	assert.Eventually(t, cond, 10*time.Second, 10*time.Millisecond, what)
}

func TestWatchSeesMembersWithoutJoining(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	client := startEtcd(t)

	joined, err := Join(ctx, client, "/test", Member{ID: "node-a", Zone: "z1", Addr: "10.0.0.1:9000"}, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = joined.Close(ctx) })

	obs, err := Watch(ctx, client, "/test")
	require.NoError(t, err)
	t.Cleanup(func() { _ = obs.Close(ctx) })

	// The observer sees the member that registered before it started.
	require.Equal(t, []string{"node-a"}, memberIDs(obs.Members()))
	assert.Equal(t, "10.0.0.1:9000", obs.AddrOf("node-a"))

	// It holds no lease of its own, and adds nothing to anyone's ring.
	assert.Zero(t, obs.LeaseID())
	assert.Equal(t, 1, obs.Ring().Len(), "the observer is not a ring node")
	assert.Equal(t, []string{"node-a"}, memberIDs(joined.Members()), "the observer is invisible to members")

	// It follows changes live, in both directions.
	second, err := Join(ctx, client, "/test", Member{ID: "node-b", Zone: "z2", Addr: "10.0.0.2:9000"}, 0)
	require.NoError(t, err)

	eventually(t, func() bool { return obs.Ring().Len() == 2 }, "observer sees the new member")
	assert.Equal(t, "10.0.0.2:9000", obs.AddrOf("node-b"))

	require.NoError(t, second.Close(ctx))
	eventually(t, func() bool { return obs.Ring().Len() == 1 }, "observer sees the member leave")
}

func TestWatchCloseRevokesNothing(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	client := startEtcd(t)

	joined, err := Join(ctx, client, "/test", Member{ID: "node-a", Addr: "10.0.0.1:9000"}, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = joined.Close(ctx) })

	obs, err := Watch(ctx, client, "/test")
	require.NoError(t, err)

	// Closing an observer must not disturb the cluster: it has no lease to revoke, and revoking
	// the zero lease would take every member's registration down with it.
	require.NoError(t, obs.Close(ctx))
	assert.Equal(t, []string{"node-a"}, memberIDs(joined.Members()))
	assert.Equal(t, 1, joined.Ring().Len())
}

func TestWatchOnEmptyClusterIsUsable(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	client := startEtcd(t)

	obs, err := Watch(ctx, client, "/test")
	require.NoError(t, err)
	t.Cleanup(func() { _ = obs.Close(ctx) })

	require.NotNil(t, obs.Ring(), "an empty cluster still has a ring")
	assert.Zero(t, obs.Ring().Len())
	assert.Empty(t, obs.Members())
	assert.Empty(t, obs.AddrOf("nobody"))
}
