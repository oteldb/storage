// Package engine is the L3 single-node storage engine: an in-memory head that absorbs
// writes (indexing labels and buffering samples) with a bounded out-of-order window,
// optionally backed by a write-ahead log for crash recovery. It implements the fetch
// contract over the head. Flush to immutable parts and background merge (compaction,
// retention, downsampling) are added in later slices.
package engine
