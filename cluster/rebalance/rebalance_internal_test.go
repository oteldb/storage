package rebalance

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/cluster/ring"
)

// TestPlanWithPerShardRF verifies per-shard replication factors: each shard's owner-set diff is
// computed at its own rf, so one plan mixes rf=1 and rf=3 tenants correctly, and the constant-rf
// Plan stays equivalent to PlanWith with a constant resolver.
func TestPlanWithPerShardRF(t *testing.T) {
	t.Parallel()

	prev := ring.New(ring.Node{ID: "a"}, ring.Node{ID: "b"}, ring.Node{ID: "c"})
	next := prev.Without("c")
	shards := []string{"s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8"}

	rfOf := func(shard string) int {
		if shard == "s1" || shard == "s2" {
			return 3 // the "gold" tenants
		}

		return 1
	}

	plan := PlanWith(shards, prev, next, rfOf)

	byShard := make(map[string]Reassignment, len(plan))
	for _, r := range plan {
		byShard[r.Shard] = r
	}

	for _, shard := range shards {
		rf := rfOf(shard)
		before := ownerIDs(prev.Lookup([]byte(shard), rf))
		after := ownerIDs(next.Lookup([]byte(shard), rf))

		r, changed := byShard[shard]
		if !changed {
			assert.Equalf(t, before, after, "shard %s omitted ⇒ owner set unchanged at rf=%d", shard, rf)

			continue
		}

		assert.Equalf(t, missingFrom(after, before), r.Added, "shard %s added at rf=%d", shard, rf)
		assert.Equalf(t, missingFrom(before, after), r.Removed, "shard %s removed at rf=%d", shard, rf)
	}

	// An rf=3 shard on a 3→2 node ring always loses "c" from its owner set (it owned every node).
	for _, shard := range []string{"s1", "s2"} {
		r, ok := byShard[shard]
		require.Truef(t, ok, "rf=3 shard %s must appear: c was an owner", shard)
		assert.Equalf(t, []string{"c"}, r.Removed, "shard %s: c removed", shard)
	}

	// Plan(rf) ≡ PlanWith(constant rf).
	for _, rf := range []int{1, 2, 3} {
		assert.Equalf(t, Plan(shards, prev, next, rf), PlanWith(shards, prev, next, func(string) int { return rf }),
			"Plan and constant PlanWith agree at rf=%d", rf)
	}
}
