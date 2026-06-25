// Package cluster implements the L0 distribution layer (DESIGN.md §3, §11, §14 M6–M7):
// rendezvous (HRW) hashing with spread-minimizing tokens, etcd-backed ring state and
// leases, RF=3 quorum replication, and rebalancing.
//
// This is a scaffold stub: the ring, etcd integration, replication, and rebalancing
// are filled in at M6. Single-node users skip this package entirely
// ([Options.Cluster] == nil ⇒ no cluster layer).
package cluster
