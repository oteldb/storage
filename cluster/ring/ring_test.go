package ring_test

import (
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/cluster/ring"
)

func nodes(ids ...string) []ring.Node {
	out := make([]ring.Node, len(ids))
	for i, id := range ids {
		out[i] = ring.Node{ID: id}
	}

	return out
}

func keys(n int) [][]byte {
	out := make([][]byte, n)
	for i := range out {
		out[i] = fmt.Appendf(nil, "series-%d", i)
	}

	return out
}

func ids(ns []ring.Node) []string {
	out := make([]string, len(ns))
	for i, n := range ns {
		out[i] = n.ID
	}

	return out
}

func TestNewDedupsAndDropsEmpty(t *testing.T) {
	t.Parallel()

	r := ring.New(ring.Node{ID: "a"}, ring.Node{ID: "a"}, ring.Node{ID: ""}, ring.Node{ID: "b"})
	assert.Equal(t, 2, r.Len())
	assert.Equal(t, []string{"a", "b"}, ids(r.Nodes()), "sorted, deduped, no empties")
}

func TestLookupDeterministic(t *testing.T) {
	t.Parallel()

	a := ring.New(nodes("n1", "n2", "n3", "n4", "n5")...)
	// A second, independently-built ring with the same members must place keys identically.
	b := ring.New(nodes("n5", "n3", "n1", "n4", "n2")...)

	for _, k := range keys(200) {
		assert.Equal(t, ids(a.Lookup(k, 3)), ids(b.Lookup(k, 3)), "placement depends only on membership")
	}
}

func TestLookupReplicaCountAndDistinct(t *testing.T) {
	t.Parallel()

	r := ring.New(nodes("n1", "n2", "n3", "n4", "n5")...)

	for _, k := range keys(100) {
		owners := r.Lookup(k, 3)
		require.Len(t, owners, 3)

		seen := map[string]struct{}{}
		for _, n := range owners {
			_, dup := seen[n.ID]
			require.False(t, dup, "replicas are distinct")
			seen[n.ID] = struct{}{}
		}
	}

	// rf larger than the ring returns the whole ring; rf ≤ 0 / empty ring return nil.
	assert.Len(t, r.Lookup([]byte("k"), 99), 5)
	assert.Nil(t, r.Lookup([]byte("k"), 0))
	assert.Nil(t, ring.New().Lookup([]byte("k"), 3))
}

func TestPrimary(t *testing.T) {
	t.Parallel()

	r := ring.New(nodes("n1", "n2", "n3")...)
	for _, k := range keys(50) {
		p, ok := r.Primary(k)
		require.True(t, ok)
		assert.Equal(t, r.Lookup(k, 1)[0].ID, p.ID, "primary is the top-scoring owner")
	}

	_, ok := ring.New().Primary([]byte("k"))
	assert.False(t, ok)
}

func TestDistributionRoughlyBalanced(t *testing.T) {
	t.Parallel()

	r := ring.New(nodes("n1", "n2", "n3", "n4", "n5")...)
	const n = 50_000

	count := map[string]int{}
	for _, k := range keys(n) {
		p, _ := r.Primary(k)
		count[p.ID]++
	}

	expected := n / 5
	for id, c := range count {
		// HRW spreads primaries ~uniformly; allow a generous ±15% band.
		assert.InDeltaf(t, expected, c, float64(expected)*0.15, "node %s owns ~1/N of keys", id)
	}
}

// setDiff returns the elements of a not present in b.
func setDiff(a, b []string) []string {
	var out []string
	for _, x := range a {
		if !slices.Contains(b, x) {
			out = append(out, x)
		}
	}

	return out
}

// TestAddNodeMovesMinimally is the core HRW property: adding a node moves at most ONE replica
// per key (always *to* the new node) and never reshuffles assignments between existing nodes;
// the new node ends up with its fair ~1/(N+1) share of replica slots.
func TestAddNodeMovesMinimally(t *testing.T) {
	t.Parallel()

	const (
		n  = 50_000
		rf = 3
	)

	before := ring.New(nodes("n1", "n2", "n3", "n4")...)
	after := before.With(ring.Node{ID: "n5"})

	newNodeSlots := 0
	for _, k := range keys(n) {
		b := ids(before.Lookup(k, rf))
		a := ids(after.Lookup(k, rf))

		// At most one owner is added and one dropped — a single replica moves, not a reshuffle.
		added := setDiff(a, b)
		require.LessOrEqualf(t, len(added), 1, "key %q gained %v", k, added)
		require.LessOrEqual(t, len(setDiff(b, a)), 1)

		// Any added owner is the new node: existing pairings are never disturbed.
		for _, id := range added {
			require.Equalf(t, "n5", id, "key %q gained a non-new owner", k)
		}

		if slices.Contains(a, "n5") {
			newNodeSlots++
		}
	}

	// The new node receives its fair share of replica slots: rf/(N+1) of all keys hold it.
	expected := n * rf / 5
	assert.InDeltaf(t, expected, newNodeSlots, float64(expected)*0.1,
		"new node took %d slots, expected ~%d (its 1/(N+1) share)", newNodeSlots, expected)
}

// TestRemoveNodeOnlyMovesItsKeys: removing a node only redistributes keys it owned.
func TestRemoveNodeOnlyMovesItsKeys(t *testing.T) {
	t.Parallel()

	before := ring.New(nodes("n1", "n2", "n3", "n4", "n5")...)
	after := before.Without("n3")
	require.Equal(t, 4, after.Len())

	for _, k := range keys(20_000) {
		b := before.Lookup(k, 3)
		a := after.Lookup(k, 3)

		if !slices.Contains(ids(b), "n3") {
			// n3 didn't own this key, so its owners must be unchanged.
			assert.Equalf(t, ids(b), ids(a), "key %q not owned by n3 must be unaffected", k)
		} else {
			// n3 owned it: the other owners stay, n3 is replaced by one new node.
			for _, owner := range a {
				assert.NotEqual(t, "n3", owner.ID, "removed node never appears")
			}
		}
	}
}

func TestWithoutMissingIsNoOp(t *testing.T) {
	t.Parallel()

	r := ring.New(nodes("n1", "n2")...)
	assert.Equal(t, ids(r.Nodes()), ids(r.Without("nope").Nodes()))
}
