# Immutable series runs

A run is a generic block part with one uncompressed canonical-ID column and a manifest-bound
`series-index`. Descriptor coverage and aggregates are zero. The engine owns publication, WAL,
sample liveness, compaction, retention and object reclamation; this package owns none of them.

Opening checks the catalog digest, nonce, schema and index binding, then streams the ID column
and descriptors together. Fixed-size ranged reads validate IDs, row count, CRC and canonical
payloads without materializing the run. Native sizing and ranged reads are required.

Readers and iterators reserve operation-local page scratch and pin retained file handles.
Returned descriptors are borrowed until the next cursor operation. Closing a run rejects new
operations and closes its handles after existing operations finish; the engine must keep its
objects reachable for the same lifetime. Write requires an unused caller-owned prefix.

An optional byte-bounded ID cache retains fixed-width sorted canonical IDs from validation. Array
position is the descriptor ordinal, checked during validation. Cache hits and exact negative probes
bypass the generic ID-key table; descriptors retain page CRC/hash checks. Pending arrays are charged
before allocation and published only after validation. Readers pin arrays once and binary-search without per-ID locks; pinned arrays remain charged and
cannot be evicted during an operation. LRU eviction leaves the bounded disk fallback
available, and run closure removes its retained IDs after active readers drain.
