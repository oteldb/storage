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

// zoned builds nodes from (id, zone) pairs.
func zoned(pairs ...[2]string) []ring.Node {
	out := make([]ring.Node, len(pairs))
	for i, p := range pairs {
		out[i] = ring.Node{ID: p[0], Zone: p[1]}
	}

	return out
}

func distinctZones(ns []ring.Node) int {
	z := map[string]struct{}{}
	for _, n := range ns {
		z[n.Zone] = struct{}{}
	}

	return len(z)
}

// TestLookupZoneAwareSpread checks that a key's replicas land in as many distinct zones as
// possible, for every key, and that the primary is unaffected by zone spreading.
func TestLookupZoneAwareSpread(t *testing.T) {
	t.Parallel()

	// Three zones, two nodes each.
	r := ring.New(zoned(
		[2]string{"a1", "z1"}, [2]string{"a2", "z1"},
		[2]string{"b1", "z2"}, [2]string{"b2", "z2"},
		[2]string{"c1", "z3"}, [2]string{"c2", "z3"},
	)...)

	for _, k := range keys(500) {
		for _, rf := range []int{1, 2, 3} {
			got := r.Lookup(k, rf)
			require.Len(t, got, rf)
			assert.Equal(t, rf, distinctZones(got), "rf=%d replicas span rf distinct zones", rf)
			assert.Equal(t, ids([]ring.Node{r.Lookup(k, 1)[0]}), ids(got[:1]), "primary stable across rf")
			assert.Len(t, ids(got), len(dedup(ids(got))), "no duplicate nodes")
		}
	}
}

// TestLookupZoneAwareFallback checks graceful degradation when there are fewer zones than rf:
// every zone is represented and the extra slots fill by score (a repeat zone), with no dup nodes.
func TestLookupZoneAwareFallback(t *testing.T) {
	t.Parallel()

	// Two zones but rf=3 ⇒ one zone must repeat.
	r := ring.New(zoned(
		[2]string{"a1", "z1"}, [2]string{"a2", "z1"},
		[2]string{"b1", "z2"}, [2]string{"b2", "z2"},
	)...)

	for _, k := range keys(500) {
		got := r.Lookup(k, 3)
		require.Len(t, got, 3)
		assert.Equal(t, 2, distinctZones(got), "both zones present")
		assert.Len(t, dedup(ids(got)), 3, "no duplicate nodes despite the repeated zone")
	}
}

// TestLookupEmptyZonesIsPureHRW confirms zone-awareness is a no-op when zones are unset: the
// result keeps the HRW prefix property (Lookup(k, n) is a prefix of Lookup(k, n+1)).
func TestLookupEmptyZonesIsPureHRW(t *testing.T) {
	t.Parallel()

	r := ring.New(nodes("a", "b", "c", "d", "e")...)
	for _, k := range keys(300) {
		for rf := 1; rf < 5; rf++ {
			assert.Equal(t, ids(r.Lookup(k, rf)), ids(r.Lookup(k, rf+1))[:rf],
				"empty-zone Lookup is a stable HRW prefix")
		}
	}
}

func dedup(s []string) []string {
	seen := map[string]struct{}{}
	out := s[:0]
	for _, v := range s {
		if _, ok := seen[v]; !ok {
			seen[v] = struct{}{}
			out = append(out, v)
		}
	}

	return out
}

// maxPerZone returns the largest number of nodes sharing any one zone.
func maxPerZone(ns []ring.Node) int {
	counts := map[string]int{}
	most := 0
	for _, n := range ns {
		counts[n.Zone]++
		if counts[n.Zone] > most {
			most = counts[n.Zone]
		}
	}

	return most
}

// TestLookupBalancedSpreadsEvenly checks the rack-aware placement: with 3 zones of 2 nodes and
// rf=6 (an ec(4,2) part), every node is selected and no zone holds more than ceil(6/3)=2 shards
// — so losing a whole rack costs at most Parity=2 shards, keeping the part recoverable.
func TestLookupBalancedSpreadsEvenly(t *testing.T) {
	t.Parallel()

	r := ring.New(zoned(
		[2]string{"a1", "z1"}, [2]string{"a2", "z1"},
		[2]string{"b1", "z2"}, [2]string{"b2", "z2"},
		[2]string{"c1", "z3"}, [2]string{"c2", "z3"},
	)...)

	for _, k := range keys(500) {
		got := r.LookupBalanced(k, 6)
		require.Len(t, got, 6)
		assert.Len(t, dedup(ids(got)), 6, "all distinct nodes")
		assert.LessOrEqual(t, maxPerZone(got), 2, "no zone holds more than ceil(6/3) shards")

		// out[0] is the primary, matching Lookup(k,1).
		assert.Equal(t, ids(r.Lookup(k, 1)), ids(got[:1]), "primary is out[0]")

		// Balanced is never worse than Lookup's pass-2 zone-blind fill.
		assert.LessOrEqual(t, maxPerZone(got), maxPerZone(r.Lookup(k, 6)),
			"balanced max-per-zone ≤ plain Lookup's")
	}
}

// TestLookupBalancedMinimizesMaxZone checks the general minimization: for a smaller rf the
// spread stays ceil(rf/zones), and for a subset the primary's zone is used first.
func TestLookupBalancedMinimizesMaxZone(t *testing.T) {
	t.Parallel()

	r := ring.New(zoned(
		[2]string{"a1", "z1"}, [2]string{"a2", "z1"}, [2]string{"a3", "z1"},
		[2]string{"b1", "z2"}, [2]string{"b2", "z2"}, [2]string{"b3", "z2"},
		[2]string{"c1", "z3"}, [2]string{"c2", "z3"}, [2]string{"c3", "z3"},
	)...)

	for _, k := range keys(300) {
		for _, rf := range []int{2, 3, 4, 5, 6, 9} {
			got := r.LookupBalanced(k, rf)
			require.Len(t, got, rf)
			want := (rf + 2) / 3 // ceil(rf/3 zones)
			assert.LessOrEqualf(t, maxPerZone(got), want, "rf=%d: max-per-zone ≤ ceil(rf/3)", rf)
		}
	}
}

// TestLookupBalancedEmptyZonesIsPureHRW confirms the placement is a no-op when zones are unset:
// it reduces to score order, keeping the HRW prefix property, identical to Lookup.
func TestLookupBalancedEmptyZonesIsPureHRW(t *testing.T) {
	t.Parallel()

	r := ring.New(nodes("a", "b", "c", "d", "e")...)
	for _, k := range keys(300) {
		for rf := 1; rf <= 5; rf++ {
			assert.Equalf(t, ids(r.Lookup(k, rf)), ids(r.LookupBalanced(k, rf)),
				"empty-zone LookupBalanced == Lookup (pure HRW), rf=%d", rf)
		}
	}
}

// TestLookupBalancedDeterministicAndBounded pins determinism and the small-ring bound.
func TestLookupBalancedDeterministicAndBounded(t *testing.T) {
	t.Parallel()

	r := ring.New(zoned([2]string{"a1", "z1"}, [2]string{"b1", "z2"}, [2]string{"c1", "z3"})...)
	for _, k := range keys(100) {
		got := r.LookupBalanced(k, 10) // rf > ring size
		assert.Len(t, got, 3, "capped at ring size")
		assert.Equal(t, ids(got), ids(r.LookupBalanced(k, 10)), "deterministic")
		assert.Equal(t, 3, distinctZones(got), "all three zones used")
	}

	assert.Nil(t, r.LookupBalanced(keys(1)[0], 0), "rf ≤ 0 ⇒ nil")
	assert.Nil(t, (&ring.Ring{}).LookupBalanced(keys(1)[0], 3), "empty ring ⇒ nil")
}

// hierNode builds a node with a rack/server domain path.
func hierNode(id, rack, server string) ring.Node {
	return ring.Node{ID: id, Domains: []string{rack, server}}
}

// maxPerLevel returns the largest number of nodes sharing any one domain at the given level.
func maxPerLevel(ns []ring.Node, level int) int {
	counts := map[string]int{}
	most := 0
	for _, n := range ns {
		counts[n.DomainAt(level)]++
		if counts[n.DomainAt(level)] > most {
			most = counts[n.DomainAt(level)]
		}
	}

	return most
}

// TestLookupBalancedHierarchical checks the disk/server/rack-aware spread: 2 racks × 2 servers ×
// 2 nodes (disks). Placing an ec(4,2) part (rf=6, taking 6 of 8 nodes) must balance at every
// level — at most 3 per rack, and within a rack the shards spread across both servers.
func TestLookupBalancedHierarchical(t *testing.T) {
	t.Parallel()

	r := ring.New(
		hierNode("r1s1d1", "rack1", "srv1"), hierNode("r1s1d2", "rack1", "srv1"),
		hierNode("r1s2d1", "rack1", "srv2"), hierNode("r1s2d2", "rack1", "srv2"),
		hierNode("r2s1d1", "rack2", "srv3"), hierNode("r2s1d2", "rack2", "srv3"),
		hierNode("r2s2d1", "rack2", "srv4"), hierNode("r2s2d2", "rack2", "srv4"),
	)

	for _, k := range keys(500) {
		got := r.LookupBalanced(k, 6)
		require.Len(t, got, 6)
		assert.Len(t, dedup(ids(got)), 6, "distinct nodes (disks)")
		assert.LessOrEqual(t, maxPerLevel(got, 0), 3, "≤ ceil(6/2) per rack")

		// Within each rack the 3 shards spread across its two servers (2+1, never 3+0).
		perServer := map[string]int{}
		for _, n := range got {
			perServer[n.DomainAt(0)+"/"+n.DomainAt(1)]++
		}
		for srv, c := range perServer {
			assert.LessOrEqualf(t, c, 2, "server %s holds ≤ 2 shards (balanced within rack)", srv)
		}
	}
}

// TestLookupBalancedRackDominatesServer confirms the level priority: with 3 racks the part
// spreads one-per-rack first (rack failure is worst), even though servers differ.
func TestLookupBalancedRackDominatesServer(t *testing.T) {
	t.Parallel()

	r := ring.New(
		hierNode("a", "rack1", "s1"), hierNode("b", "rack1", "s2"),
		hierNode("c", "rack2", "s3"), hierNode("d", "rack2", "s4"),
		hierNode("e", "rack3", "s5"), hierNode("f", "rack3", "s6"),
	)

	for _, k := range keys(300) {
		got := r.LookupBalanced(k, 3)
		require.Len(t, got, 3)
		assert.Equal(t, 1, maxPerLevel(got, 0), "3 shards over 3 racks ⇒ one per rack")
	}
}
