// Package engine implements the L3 storage engine (DESIGN.md §3, §14 M3): an in-memory
// sorted head with a bounded out-of-order window, head→immutable-part flush, and one
// background-merge engine that drives compaction, retention, and downsampling.
// Not yet implemented (M3).
package engine
