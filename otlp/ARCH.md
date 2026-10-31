# OTLP ingestion adapters

`otlp/otlpdecode` converts standard OTLP protobuf requests directly into native logs,
metrics, traces and profiles using `easyproto`. It imports native signal models and
the shared chunked arena; its runtime dependency graph does not include pdata.
`otlp/pdataconv` remains the optional bridge for embedders already consuming pdata.

Converters are reusable, single-request owners. Their batches borrow request bytes
and converter scratch until the next conversion, so concurrent requests need separate
converters. A sink copies or interns any data it retains before recycling the input.
The storage facade satisfies that ownership boundary; file/WAL integration verifies
that clearing a decoded request before flushing preserves values and identities.

Repeated protobuf fields may be interleaved or reordered. Unknown fields are skipped;
malformed data is rejected, and unrepresentable points/exemplars are reported by the
same counts as the pdata adapter. Profiles resolve shared string-table references.
The conformance and fuzz tests compare every signal with pdata and cover reuse,
field ordering, malformed ids and observed-time fallback for logs.

Chunked `pool.Arena` storage keeps earlier slices stable when a request grows and
retains capacity across resets. Reset invalidates prior views. HTTP/gRPC servers,
request limits, routing and partial-success responses belong to the embedder.

The optional `pdataconv.MetricsProjector` hashes numeric metric point attributes directly from
pdata for the facade's local projected-write path. Its stable source materializes owned native
attributes only on registry misses/soft overflow. Whole requests containing exemplars or decomposed
metric types use the existing conversion path; clustered storage rejects projection before ingestion.
Pdata stays outside the core storage dependency graph, and canonical IDs/WAL formats are unchanged.
