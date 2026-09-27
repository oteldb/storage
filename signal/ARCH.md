# `signal/` — ingest models & projection

`signal` itself holds the signal-neutral model: the `Signal` enum, `TenantID`, the `Aggregation`
enum (the shared rollup vocabulary), and the typed identity primitives — see
[`../index/ARCH.md`](../index/ARCH.md).

Each sub-package holds one signal's **ingest batch** and its **projection** into engine columns.

## Common shape

**The ingest boundary is the internal model, not pdata.** Each batch mirrors the OTLP hierarchy
but holds all identity as `[]byte`, so an embedder decoding OTLP protobuf builds it by aliasing
the decode buffer and projection copies nothing. Batches are **resettable and pool-friendly**: the
`Add*` builders reuse retained capacity (resettable-arena `grow`), so a reset-then-rebuild cycle
allocates nothing across ingest calls.

## `metric`

`Project` walks Gauge and Sum number points; every point in the batch is well-formed by
construction, so projection rejects nothing (out-of-order rejection is the engine's job). Metric
identity folds name/unit/kind/temporality/monotonicity into **reserved labels** (`__name__`,
`__unit__`, …) on a `signal.Series`, so one identity/index machinery covers metrics and a query
matches `__name__` like any other label.

`emit` is called **once per metric** with a `*Batch` (id/timestamp/value columns + the context to
materialize a `signal.Series` lazily), so the engine takes its lock and resolves the tenant once
per metric, not per point. The `Batch` is pooled; it and the data it aliases are valid only for the
`emit` call.

**The series id is computed without allocating or sorting**: the resource‖scope hash pre-image is
built once per scope group and kept resident at the front of a reused buffer; the reserved labels
once per metric in sorted-key order; per point only the point's already-sorted attributes are
merged in one pass, emitting the hash pre-image directly — never materializing a combined sorted
`[]KeyValue`. The result is byte-identical to the reference materialization (fuzz-pinned), which
is used only when the engine reports a new series. This is what makes ingest ~zero-alloc.

## `log`

Schema: `observed`/`severity`/`flags`/`dropped` (int) + `severity_text`/`body`(FullText)/
`trace_id`(Equality)/`span_id`/`attrs`(Attrs) (bytes).

## `trace`

A span is a record. Schema adds ingest-computed **nested-set ids** (`parent_id`/`nested_set_left`/
`nested_set_right`): `Project` groups by trace id across services, builds the parent→child tree and
preorder-DFS assigns them, so an embedder's TraceQL does ancestor/descendant/sibling as range
comparisons — no `SeekTo`. A cross-batch parent is treated as a root (the raw `parent_span_id`
stays present to reconcile). `trace_id`/`span_id` use the dictionary-free fixed-width
`CodecBytesRaw`: at production cardinality the dictionary degrades to its flat fallback (17 B/row
for a 16-byte id) while fixed-width stores 16 B/row and decodes far faster. `trace_id` still
carries an equality bloom for trace-by-id pruning.

## `exemplar`

An exemplar is record-shaped (zero or more per data point, variable-width payload), so it lives in
the record engine rather than the dense metrics engine, whose one-row-per-point layout and
downsampling merge cannot express it.

**An exemplar stream *is* its metric series.** The stream id is the point's
`metric.Identity.SeriesID`, byte for byte, and it is not recomputed: `Project` takes an
already-projected `*metric.Batch` and reads the id the metrics path resolved, so the two cannot
drift. That is what makes the series index, tenant routing, and shard placement fall out for free —
a matcher set selecting metric series selects exactly the matching exemplar streams. One `emit` is
one point (within a metric, points have distinct attribute sets, so a point is a stream); points
with no exemplars — nearly all of them — never reach the engine.

Because the metric identity puts its labels in `Series.Attributes` — where a log/trace stream has
nothing — the record engine had to start indexing that third attribute set for a metric selector to
resolve exemplar streams at all. It is a no-op for the other record signals; see
[`../recordengine/ARCH.md`](../recordengine/ARCH.md).

Schema: `value` (int) + `trace_id`(Equality)/`span_id`/`attrs`(Attrs) (bytes). `trace_id` is
near-unique by construction, so it takes the dictionary-free `CodecBytesRaw`; its equality bloom is
what prunes a trace-to-metrics lookup. The record engine has no float kind, so `value` carries the
float64 **bit pattern** in an int64 column (`EncodeValue`/`DecodeValue`) — lossless, but it forfeits
compression and the float-precision recompression policy; issue #251 tracks the proper fix.

Exemplars on histogram, exponential-histogram, and summary points are **rejected and counted**, not
stored: classic decomposition splits those into several series, leaving an exemplar with no
unambiguous home. Issue #252 holds the decision for when native histograms land.

## `profile`

A profile is a pprof-style graph with a large shared symbol dictionary — Pyroscope's **two-table
split**: a columnar sample table + a deduplicated symbol store. Each sample flattens to one record
row (a sample with `timestamps_unix_nano` explodes to one row per (timestamp, value)). The
**profile type folds into the stream identity** as reserved `otel.profile.*` labels — like a
metric's `__name__` — so a type is selected by an ordinary matcher and enumerated through postings
rather than a per-sample column. `stack_id` is content-addressed, computed Merkle-style bottom-up
(string→function→location→stack), so the same stack has the same id everywhere; the symbol store
rides the part lifecycle through the record engine's side-store hook.

**Symbol tables are compressed.** Each table (`sym-{name}.bin`, and each table of a batch delta) is
an `OTSP` blob: version 2 is `[magic][version][algorithm][uvarint raw length][compress block][CRC32C]`
over a body of `[count]` then `[16B id][len][bytes]` sorted by id. The body is mostly 16-byte ids,
random individually but repeated across the stacks and locations that share frames, which zstd finds.
On `profile/testdata/cpu.pprof` (a CPU profile of this module's benchmarks: 5067 stacks) zstd takes
the tables from 1.52 MB to 449 KB, 3.4×: stacks 5.0×, strings 2.2×, locations 2.1×,
functions 1.6×. Larger tables repeat more: a test-stand part's stacks shrink 8.3× under zstd-19.

- **Only the disk boundary compresses.** `SymbolStore.Stored` re-frames a table's body under zstd,
  and the engine calls it (`SideStore.Stored`) on exactly what it writes as sidecars: a flush's
  snapshot once per flush, a merge's union once. Everything else — the batch delta, `Encode`, `Union`
  output, the side RPC's reply — is `AlgorithmNone`: each is decoded straight back, so a zstd encode
  would buy nothing. A delta rides the WAL and replication per ingest batch, where the wire framing
  already compresses.
- **zstd at the default level.** The engine passes the side store no compressor, and its
  `MergeCompression` does not apply at flush, so the table picks its own; the algorithm byte keeps
  it a writer decision. zstd-19 gains 1.5% over the default and `LevelFast` saves 10–20% encode time
  for 3% of the ratio.
- **The cost is flush and merge encode time.** zstd encodes at 110–270 MB/s against 460–1190 MB/s
  raw, and decodes at 460–1035 MB/s against 600–1410. A resolver pays a part's decode once, on a
  symbol-cache miss.
- **Decode is bounded by the recorded length.** The body inflates through `DecompressLimit` to
  exactly the raw length in the header, so a corrupt table allocates no more than it claims, and no
  more than its frame can produce: a frame that inflates past the claim fails before decoding.
- **Version 1 decodes forever and is never written.** It is the same body stored raw, with no
  algorithm or length.

**The resolver reads layers, not a union.** `Storage.ProfileResolver(ctx, tenant, start, end)` builds
a `Resolver` over `Tables` layers: the live accumulator (its maps copied under the engine lock;
absorbed entries are never mutated, so the entries are shared), an in-flight flush's snapshot, then
every part overlapping the window, newest first (`recordengine.Engine.ReadSide`). Symbols carry their
samples' time domain through the parts that hold them, so a window's resolver resolves every stack
the window's samples reference and need not resolve any other.

- **A part decodes once.** Its `Tables` come from a `SymbolCache` keyed by part prefix — immutable,
  so never invalidated — bounded by decoded bytes (`Options.ProfileSymbolCacheBytes`, 128 MiB by
  default) and built on otter like the backend read cache. A decoded entry slices its table's
  decompressed body, so a cached part costs its raw body plus about 48 B per entry for the map. The
  views pin the body's whole backing array, and zstd decompression reserves its bound plus about
  128 KiB of slack, so a small table would pin several times its size outside the budget. Bodies
  therefore decompress into pooled scratch, and only an exact-length copy is retained and charged.
- **Merging layers per query is the cost being avoided.** A union of N cached parts is still N ×
  entries map operations on every call, and on a store whose every part repeats the working set that
  is nearly the whole decode again. A lookup instead probes the layers in order, trying the stack's
  own layer first for its frames: a part holds the closure of every stack it holds, so each frame is
  one probe. Only the stack probe walks the layers — one probe when the newest layer holds it, one
  per newer layer when only an old part does. Content addressing makes any layer's entry for an id
  identical, so the order sets the cost, never the answer.
- **Measured** (`BenchmarkProfileResolver`, parts that each repeat a 2000-stack working set). A
  whole-store build over 32 parts costs 13 µs warm and 6 ms with the cache off, against 38 ms for a
  union decoded, re-encoded and decoded again; a window holding one part costs 0.6 µs warm. The hint
  keeps resolution flat in the layer count: `cpu.pprof`'s 5067 stacks resolve in about 15 ms over one
  layer and over 32 copies of it alike.
- **Cluster.** Each shard contributes its layers. A remote owner's reply is its window's layers
  unioned into one table set (one copy of each symbol on the wire), decoded into one layer and not
  cached, since it includes the owner's head.

## `otlp/pdataconv`

**The only package importing `go.opentelemetry.io/collector/pdata`**, and optional. It lives at the
repository root, not under `signal/`. `AppendMetrics`/`AppendLogs`/`AppendTraces`/`AppendProfiles`
convert each pdata type into its internal batch, each returning what it refuses. `AppendMetrics`
returns a `Dropped{Points, Exemplars}` — split because a dropped point feeds OTLP partial-success
(the producer should retry) while a dropped exemplar only degrades correlation and must not inflate
that count; the others return a plain `dropped` count. For metrics, Gauge/Sum convert directly; **Histogram,
ExponentialHistogram and Summary are stored by classic decomposition** into ordinary float series
(`_count`, `_sum`, cumulative `_bucket{le=…}` per the Prometheus convention; `{quantile=…}` for
summaries). An exponential histogram is first converted to explicit `le` bounds from its scale.
So all three reuse the engine, merge, downsample and fetch paths with **no histogram-specific
storage code**. Number-point exemplars convert; exemplars on the decomposed types are dropped and
counted, since no single decomposed series owns them (see `exemplar` above). Conversion necessarily allocates (pdata holds Go strings), which is why it sits
off the hot path — embedders owning their OTLP decoder build the internal batch directly.

**A log record's event time falls back to its observed time.** `time_unix_nano` is optional in OTLP
— a receiver tailing files or journald with no timestamp parser leaves it unset — and the record's
timestamp is the part sort key, so without the fallback such a record sorts at the unix epoch, where
no query window reaches it and retention drops the part as ancient. A record with neither time is
refused and counted in `dropped`, so the embedder reports it as an OTLP partial success rather than
storing something nothing can retrieve.
