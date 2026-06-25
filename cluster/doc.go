// Package cluster implements the L0 distribution layer (DESIGN.md §3, §11, §14 M6–M7):
// rendezvous (HRW) hashing with spread-minimizing tokens, etcd-backed ring state and
// leases, RF=3 quorum replication, and rebalancing. Single-node users skip this
// entirely. Not yet implemented (M6).
package cluster
