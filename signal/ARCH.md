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

## `otlp/pdataconv`

**The only package importing `go.opentelemetry.io/collector/pdata`**, and optional. Converts
`pmetric.Metrics` → `metric.Metrics`. Gauge/Sum convert directly; **Histogram,
ExponentialHistogram and Summary are stored by classic decomposition** into ordinary float series
(`_count`, `_sum`, cumulative `_bucket{le=…}` per the Prometheus convention; `{quantile=…}` for
summaries). An exponential histogram is first converted to explicit `le` bounds from its scale.
So all three reuse the engine, merge, downsample and fetch paths with **no histogram-specific
storage code**. Conversion necessarily allocates (pdata holds Go strings), which is why it sits
off the hot path — embedders owning their OTLP decoder build the internal batch directly.
