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

// Node is a cluster member. ID is its stable, unique identity (the only thing that affects
// placement). Zone is an optional failure domain for zone-aware replica spreading (carried
// now, used by the replication layer later).
type Node struct {
	ID   string
	Zone string
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

// Lookup returns up to rf nodes responsible for key, ordered by descending HRW score — the
// first is the primary, the rest are replicas. It returns fewer than rf nodes only when the
// ring has fewer than rf members, and nil for an empty ring or rf ≤ 0. Ties (equal scores)
// break by node ID, so the result is fully deterministic.
func (r *Ring) Lookup(key []byte, rf int) []Node {
	if rf <= 0 || len(r.nodes) == 0 {
		return nil
	}

	rf = min(rf, len(r.nodes))

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

	out := make([]Node, rf)
	for i := range out {
		out[i] = scored[i].node
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
