package rebalance_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/cluster/rebalance"
	"github.com/oteldb/storage/cluster/ring"
)

func nodes(ids ...string) []ring.Node {
	out := make([]ring.Node, len(ids))
	for i, id := range ids {
		out[i] = ring.Node{ID: id}
	}

	return out
}

func shards(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("tenant-%d", i)
	}

	return out
}

func TestPlanNoChange(t *testing.T) {
	t.Parallel()

	r := ring.New(nodes("n1", "n2", "n3")...)
	assert.Empty(t, rebalance.Plan(shards(500), r, r, 2), "same ring ⇒ no reassignments")
}

func TestPlanAddNodeIsMinimalAndOneInOneOut(t *testing.T) {
	t.Parallel()

	const rf = 3

	prev := ring.New(nodes("n1", "n2", "n3", "n4")...)
	next := prev.With(ring.Node{ID: "n5"})

	all := shards(5000)
	plan := rebalance.Plan(all, prev, next, rf)

	for _, r := range plan {
		// Adding one node ⇒ each affected shard gains exactly the new node and drops one old.
		assert.Equal(t, []string{"n5"}, r.Added, "the new node is the only one added")
		require.Len(t, r.Removed, 1, "exactly one prior owner is displaced")
		assert.NotEqual(t, "n5", r.Removed[0])
	}

	// Only the ~rf/(N+1) share of shards the new node scores into are reassigned.
	frac := float64(len(plan)) / float64(len(all))
	assert.InDelta(t, float64(rf)/5.0, frac, 0.08, "minimal movement (~rf/(N+1) of shards)")
}

func TestPlanRemoveNode(t *testing.T) {
	t.Parallel()

	const rf = 3

	prev := ring.New(nodes("n1", "n2", "n3", "n4", "n5")...)
	next := prev.Without("n3")

	plan := rebalance.Plan(shards(3000), prev, next, rf)
	require.NotEmpty(t, plan)

	for _, r := range plan {
		// Every reassignment is because n3 left: it is removed, and one replacement is added.
		assert.Equal(t, []string{"n3"}, r.Removed, "only the departed node is removed")
		require.Len(t, r.Added, 1)
		assert.NotEqual(t, "n3", r.Added[0])
	}
}

func TestPlanDeterministicAndShardScoped(t *testing.T) {
	t.Parallel()

	prev := ring.New(nodes("n1", "n2", "n3")...)
	next := prev.With(ring.Node{ID: "n4"})

	all := shards(200)
	a := rebalance.Plan(all, prev, next, 2)
	b := rebalance.Plan(all, prev, next, 2)
	assert.Equal(t, a, b, "pure function of the rings")

	// The plan only references the shards passed in.
	in := map[string]struct{}{}
	for _, s := range all {
		in[s] = struct{}{}
	}
	for _, r := range a {
		_, ok := in[r.Shard]
		assert.True(t, ok)
	}
}
