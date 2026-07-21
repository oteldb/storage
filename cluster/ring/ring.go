// Package ring implements rendezvous (highest-random-weight, HRW) hashing — the L0 sharding
// primitive (DESIGN.md §11). Each key is placed on the nodes that score highest for it, where
// a node's score for a key is a hash of the key seeded by the node's identity. This gives:
//
//   - Deterministic placement: every node computes the same owners for a key with no shared
//     state beyond the membership list, so routing needs no coordinator on the hot path.
//   - Minimal movement (~1/N) on membership change: adding a node only ever steals keys *to
//     itself*, and removing a node only redistributes *its* keys — existing assignments
//     between other nodes never reshuffle. This is the property that makes rebalancing move
//     the minimal set of parts.
//
// The ring is immutable; membership changes ([Ring.With] / [Ring.Without]) return a new ring.
package ring

import (
	"slices"
	"sort"

	"github.com/zeebo/xxh3"
)

// Node is a cluster member. ID is its stable, unique identity (the only thing that affects a
// node's HRW score).
//
// Failure domains are hierarchical, coarsest first: Domains holds the node's location as a path
// such as {"rack1", "hostA"} — rack, then server — with the node itself the finest domain (a
// disk). [Ring.LookupBalanced] spreads a key's shards to minimize how many land in any one
// domain at each level (fewest per rack, then per server, then one per node/disk), so a whole
// rack, server, or disk failure loses as few shards as the topology allows. Zone is the
// single-level shorthand — equivalent to Domains == {Zone} — kept for the replica path
// ([Ring.Lookup], which spreads across the coarsest domain only). When Domains is set it
// supersedes Zone; when both are empty placement is pure HRW.
type Node struct {
	ID      string
	Zone    string
	Domains []string
}

// DomainAt returns the node's failure-domain label at the given level (0 = coarsest, e.g. the
// rack), or "" when the node has no domain that deep.
func (n Node) DomainAt(level int) string {
	d := n.domains()
	if level < len(d) {
		return d[level]
	}

	return ""
}

// Depth returns the number of failure-domain levels the node declares.
func (n Node) Depth() int { return len(n.domains()) }

// domains returns the node's failure-domain path (coarsest first), falling back to the
// single-level Zone shorthand, or nil when neither is set.
func (n Node) domains() []string {
	if len(n.Domains) > 0 {
		return n.Domains
	}

	if n.Zone != "" {
		return []string{n.Zone}
	}

	return nil
}

// Ring is an immutable set of nodes over which keys are placed by HRW hashing. The zero value
// is an empty ring; construct one with [New]. Safe for concurrent use (read-only).
type Ring struct {
	nodes []seededNode // sorted by ID for deterministic tie-breaks
}

type seededNode struct {
	node Node
	seed uint64 // xxh3 seed derived from the node ID; HRW score = HashSeed(key, seed)
}

// New returns a ring over nodes. Nodes with an empty or duplicate ID are dropped (ID is the
// identity), so membership is a well-formed set.
func New(nodes ...Node) *Ring {
	seen := make(map[string]struct{}, len(nodes))
	out := make([]seededNode, 0, len(nodes))

	for _, n := range nodes {
		if n.ID == "" {
			continue
		}

		if _, dup := seen[n.ID]; dup {
			continue
		}

		seen[n.ID] = struct{}{}
		out = append(out, seededNode{node: n, seed: xxh3.HashString(n.ID)})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].node.ID < out[j].node.ID })

	return &Ring{nodes: out}
}

// Len returns the number of nodes.
func (r *Ring) Len() int { return len(r.nodes) }

// Nodes returns the membership, sorted by ID (a copy; callers may mutate it).
func (r *Ring) Nodes() []Node {
	out := make([]Node, len(r.nodes))
	for i := range r.nodes {
		out[i] = r.nodes[i].node
	}

	return out
}

// Lookup returns up to rf nodes responsible for key — the first is the primary, the rest are
// replicas. It returns fewer than rf nodes only when the ring has fewer than rf members, and nil
// for an empty ring or rf ≤ 0. Selection is **zone-aware**: among nodes sorted by descending HRW
// score (ties broken by ID), it greedily picks the highest-scoring node from each not-yet-used
// zone first, so a key's replicas spread across as many distinct failure domains as possible;
// once distinct zones are exhausted (fewer zones than rf) it fills the remaining slots in score
// order. The primary is always the single highest-scoring node, and when every zone is empty the
// result is exactly the score-ordered top-rf (pure HRW). The result is fully deterministic.
func (r *Ring) Lookup(key []byte, rf int) []Node {
	if rf <= 0 || len(r.nodes) == 0 {
		return nil
	}

	rf = min(rf, len(r.nodes))
	scored := r.scoreSorted(key)

	out := make([]Node, 0, rf)
	usedZones := make(map[string]struct{}, rf)

	// Pass 1: the highest-scoring node from each distinct zone (in score order), so replicas
	// land in different failure domains. The primary (scored[0]) is always taken first.
	for i := range scored {
		if len(out) == rf {
			break
		}

		zone := scored[i].node.DomainAt(0)
		if _, used := usedZones[zone]; used {
			continue
		}

		usedZones[zone] = struct{}{}
		out = append(out, scored[i].node)
	}

	// Pass 2: zones are exhausted but rf is not filled — take the remaining highest-scoring nodes
	// regardless of zone (graceful degradation when there are fewer zones than replicas).
	if len(out) < rf {
		picked := make(map[string]struct{}, len(out))
		for _, n := range out {
			picked[n.ID] = struct{}{}
		}

		for i := range scored {
			if len(out) == rf {
				break
			}

			if _, ok := picked[scored[i].node.ID]; !ok {
				out = append(out, scored[i].node)
			}
		}
	}

	return out
}

// LookupBalanced returns up to rf nodes for key, spread across the failure-domain hierarchy
// **as evenly as possible** — it minimizes the maximum number of returned nodes in any one
// domain at each level (fewest per rack, then per server, then one per node/disk), rather than
// only guaranteeing distinct coarse domains up front like [Ring.Lookup]. This is the placement
// erasure coding needs: an ec(k,m) part must not lose more than m of its k+m shards to a single
// failure at any level, so the shards have to be balanced across the whole topology. With
// enough racks the maximum per rack is `ceil(rf / racks)`, and within a rack the shards
// balance across its servers the same way.
//
// out[0] is always the primary (the single highest-scoring node), so the compaction owner is
// always among the returned set. When every node's domains are empty (the default), the result
// is exactly score order — identical to [Ring.Lookup] / pure HRW. Fully deterministic; returns
// fewer than rf nodes only when the ring is smaller, and nil for an empty ring or rf ≤ 0.
func (r *Ring) LookupBalanced(key []byte, rf int) []Node {
	if rf <= 0 || len(r.nodes) == 0 {
		return nil
	}

	rf = min(rf, len(r.nodes))

	return balancedOrder(r.scoreSorted(key), 0)[:rf]
}

// balancedOrder reorders score-sorted nodes so that any prefix is spread as evenly as possible
// across the failure-domain hierarchy from `level` down: it groups the nodes by their domain
// label at `level` (preserving score order, groups ranked by their best node), recursively
// balances each group at the next finer level, then round-robins across the groups. Taking the
// first N of the result therefore minimizes the count in any one domain at the coarsest level
// first, then the next, and so on — with the nodes themselves (distinct disks) the finest
// level. At or past the deepest declared level it is plain score order.
func balancedOrder(nodes []scoredNode, level int) []Node {
	if len(nodes) <= 1 || level >= maxDepth(nodes) {
		return justNodes(nodes)
	}

	groups := make(map[string][]scoredNode, len(nodes))

	var order []string // group labels in best-score order

	for _, n := range nodes {
		label := n.node.DomainAt(level)
		if _, seen := groups[label]; !seen {
			order = append(order, label)
		}

		groups[label] = append(groups[label], n)
	}

	// A single group at this level carries no diversity: descend to the next finer level.
	if len(order) == 1 {
		return balancedOrder(nodes, level+1)
	}

	ordered := make(map[string][]Node, len(order))
	for _, label := range order {
		ordered[label] = balancedOrder(groups[label], level+1)
	}

	// Round r takes the r-th balanced pick of every group (groups in best-score order), so the
	// coarsest split is even and finer levels stay balanced within it.
	out := make([]Node, 0, len(nodes))
	for round := 0; ; round++ {
		progressed := false

		for _, label := range order {
			if round >= len(ordered[label]) {
				continue
			}

			out = append(out, ordered[label][round])
			progressed = true
		}

		if !progressed {
			break
		}
	}

	return out
}

// maxDepth is the deepest failure-domain level any node in the set declares.
func maxDepth(nodes []scoredNode) int {
	d := 0
	for _, n := range nodes {
		if dd := n.node.Depth(); dd > d {
			d = dd
		}
	}

	return d
}

// justNodes strips the scores, preserving order.
func justNodes(nodes []scoredNode) []Node {
	out := make([]Node, len(nodes))
	for i := range nodes {
		out[i] = nodes[i].node
	}

	return out
}

type scoredNode struct {
	node  Node
	score uint64
}

// Primary returns the single owner of key (the highest-scoring node) and true, or a zero Node
// and false for an empty ring.
func (r *Ring) Primary(key []byte) (Node, bool) {
	owners := r.Lookup(key, 1)
	if len(owners) == 0 {
		return Node{}, false
	}

	return owners[0], true
}

// With returns a new ring with n added (a no-op clone if n.ID is empty or already present).
func (r *Ring) With(n Node) *Ring {
	return New(append(r.Nodes(), n)...)
}

// Without returns a new ring with the node identified by id removed.
func (r *Ring) Without(id string) *Ring {
	kept := slices.DeleteFunc(r.Nodes(), func(n Node) bool { return n.ID == id })

	return New(kept...)
}

// scoreSorted returns the membership scored for key and sorted by descending HRW score, ties
// broken by ascending node ID (so placement is fully deterministic).
func (r *Ring) scoreSorted(key []byte) []scoredNode {
	scored := make([]scoredNode, len(r.nodes))
	for i := range r.nodes {
		scored[i] = scoredNode{node: r.nodes[i].node, score: xxh3.HashSeed(key, r.nodes[i].seed)}
	}

	sort.Slice(scored, func(i, j int) bool {
		if scored[i].score != scored[j].score {
			return scored[i].score > scored[j].score
		}

		return scored[i].node.ID < scored[j].node.ID
	})

	return scored
}
