// Package rebalance computes the minimal ownership changes to apply a cluster membership
// change (DESIGN.md §11). Because the data lives in the shared object store, a rebalance is an
// **ownership handoff**, not a data copy: when a shard's ring-owners change, the nodes that
// gained it start serving/compacting it (reading parts from S3 via the bucket index) and the
// nodes that lost it stop — no bytes move between nodes.
//
// The plan is a pure function of the old and new [ring.Ring]: thanks to HRW hashing only the
// ~1/N shards whose owner set actually changed appear, each with a one-in/one-out handoff, so
// the orchestrator that executes it (etcd-coordinated, a later piece) moves the minimum.
package rebalance

import (
	"slices"

	"github.com/oteldb/storage/cluster/ring"
)

// Reassignment is how one shard's owner set changes between two rings: the node IDs that newly
// own it (Added — must take it on) and those that no longer do (Removed — may drop it), each
// sorted. A shard with unchanged ownership produces no Reassignment.
type Reassignment struct {
	Shard   string
	Added   []string
	Removed []string
}

// Plan returns the reassignments needed to move ownership of shards from prev to next at
// replication factor rf, omitting shards whose owner set is unchanged. The result is ordered
// by shard, so it is deterministic.
func Plan(shards []string, prev, next *ring.Ring, rf int) []Reassignment {
	return PlanWith(shards, prev, next, func(string) int { return rf })
}

// PlanWith is [Plan] with a per-shard replication factor: rfOf(shard) returns the owner count
// for that shard, so tenants with different durability policies (per-tenant RF) produce their
// actual owner-set diffs in one plan. rfOf must be pure for the duration of the call.
func PlanWith(shards []string, prev, next *ring.Ring, rfOf func(shard string) int) []Reassignment {
	var out []Reassignment

	for _, shard := range shards {
		key := []byte(shard)
		rf := rfOf(shard)
		before := ownerIDs(prev.Lookup(key, rf))
		after := ownerIDs(next.Lookup(key, rf))

		added := missingFrom(after, before)
		removed := missingFrom(before, after)
		if len(added) == 0 && len(removed) == 0 {
			continue
		}

		out = append(out, Reassignment{Shard: shard, Added: added, Removed: removed})
	}

	return out
}

// ownerIDs extracts a sorted set of node IDs from ring owners.
func ownerIDs(owners []ring.Node) []string {
	out := make([]string, len(owners))
	for i, n := range owners {
		out[i] = n.ID
	}

	slices.Sort(out)

	return out
}

// missingFrom returns the elements of a not present in b (both sorted), preserving order.
func missingFrom(a, b []string) []string {
	var out []string
	for _, x := range a {
		if !slices.Contains(b, x) {
			out = append(out, x)
		}
	}

	return out
}
