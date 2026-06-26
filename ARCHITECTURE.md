# Architecture (current state)

> **Maintenance directive — read this first.**
> This file describes the architecture **as it exists in the code today**, not the
> roadmap. Any agent or contributor that makes an architectural change — a new package
> or layer, a new public type/interface, a new on-disk/wire format, a changed codec
> framing, a moved boundary between layers, or a new cross-cutting invariant — **must
> update this file in the same change** so it stays an accurate map of reality.
> Forward-looking design and the milestone plan live in `DESIGN.md` and `PROMPT.md`;
> keep speculation and TODOs out of this file.
>
> Last verified against the tree: 2026-06-26 (read seam exposed as `Storage.Fetcher`; the
> library is language-agnostic — query languages live in the embedder, with `query/promql` an
> optional fetch→Prometheus-Queryable adapter; ingest on the internal `metric.Metrics` batch).

`github.com/oteldb/storage` is a low-level, OpenTelemetry-centric columnar storage
**library** (Go 1.26). It has no `main`, server, or CLI: an embedder (primarily
`go-faster/oteldb`) owns the process and calls the small `storage` facade.

What is actually built today is the **encoding foundation** (the bit-level and
column-codec layer), the **part format** (`block`) and **storage backends**
(`backend` memory + file), the **identity + index layer** (typed attributes/SeriesID in
`signal`, plus `index/{symbols,series,postings}`), the **write-ahead log** (`wal`), and —
new in M3 — the **single-node metrics engine** (`engine`): an in-memory head, flush to
immutable flat parts, size-tiered merge with retention, and the **fetch contract**
(`query/fetch`) reading head ∪ parts, all driven through the **public facade**
(`storage`) which now ingests OTLP metrics (`signal/metric` projection) and routes them
per-tenant. The remaining layers — query languages (PromQL/LogQL/…), the query planner,
and cluster — currently exist as package boundaries with documented seams; their behavior
is not yet implemented.

---

## 1. Layered model

The design is a single columnar engine with swappable front-ends and backends
(`DESIGN.md` §3). The layers, top to bottom, and what backs each one **right now**:

| Layer | Concern | Realized today |
|---|---|---|
| L6 Query languages | promql/logql/traceql/genericql | **owned by the embedder, not the library** — the library exposes the fetch seam; `query/promql` is an optional fetch→Prometheus-Queryable *adapter* (no engine) |
| L5 Query engine | plan IR · sharding · streaming exec · cache | **embedder's concern** (it drives its own engine over the fetch contract); our sharded planner/cache is out of scope for the library |
| L4 **Fetch contract** | **callback matchers + window → iterator of batches** | **the library's query surface** (`query/fetch`, exposed via `Storage.Fetcher`); implemented for metrics, column conditions pending |
| L3 **Engine** / **Index / WAL** | **head · flush · merge · retention** / **symbols · series · postings** · **write-ahead log** | **engine implemented (metrics)**; **index + wal implemented** |
| L2 **Part** / **Encoding** | **immutable parts · per-column objects · manifest** / **bitstream · codecs · compress** | **both implemented** (`block`, `encoding`) |
| L1 **Backend** | file · s3 · memory behind one interface | **memory + file + s3 implemented**, with `PutIfAbsent` CAS; `bucketindex` for stateless part enumeration |
| L0 Cluster | etcd ring · HRW sharding · RF=3 · rebalance | — (package `cluster`, seam only) |

The **implemented substance spans L1 (backends) through L4 (the fetch contract)** for
metrics: encoding, parts, index, WAL, the engine head/flush/merge, and the metrics fetch
contract — now **exposed from the facade as `Storage.Fetcher`**, the library's read seam.
The library is **language-agnostic and stops at L4**: query languages (L5/L6) live in the
embedder, which drives its own engines over the fetch contract. `query/promql` is an
**optional adapter** (§3h) bridging the fetch seam to the Prometheus `storage.Queryable` for
embedders that use the Prometheus engine — it is the only package importing prometheus, and
the core never does. L0 (cluster) remains a seam. The rest of this document details what is
built.

---

## 2. Encoding foundation (`encoding/`)

The encoding stack turns typed value slices into compact byte streams and back. It is
the most developed part of the codebase and the foundation everything else builds on.

### 2.1 `encoding/bitstream` — the bit-level substrate

MSB-first bit stream `Writer` and `Reader` over caller-owned `[]byte`. This is the
primitive every codec writes through.

- `Writer`: `WriteBit`/`WriteBits`/`WriteByte`/`WriteBytes`, `WriteUvarint`/`WriteVarint`,
  `AppendBytes(n)` (returns a writable window directly in the output buffer — no copy),
  `AppendString`, `PadToByte`, `Bytes`, `Reset`, `AppendTo`.
- `Reader`: `ReadBit`/`ReadBits`/`ReadByte`/`ReadBytes`, `ReadUvarint`/`ReadVarint`,
  `ReadBytesView(n)` (returns a `[]byte` **aliasing** the source — zero-copy),
  `AlignToByte`, `SkipBits`, `ConsumedBytes` (byte offset where the next byte-aligned
  field begins), `Remaining`, `Reset`.

Design points that hold throughout the codecs: bulk reads/writes stay on a fast
byte-aligned path; full-byte flags (not single bits) are used so subsequent bulk ops
remain aligned; reads can return views into the source instead of copying.

### 2.2 `encoding/chunk` — value-column codecs

Each codec is a pair of append-style functions over caller-owned buffers. Every stream
begins with a shared header: a uvarint **row count**, read back by `readHeader`. The
`Codec` enum (`chunk.go`) names each encoding for column metadata; values are
persisted/wire-stable and must never be reordered. `IsEOF` classifies truncation errors.

| `Codec` | For | Encode / Decode | Technique |
|---|---|---|---|
| `CodecDoD` | timestamps (`int64`) | `EncodeTimestamps` / `DecodeTimestamps` | delta-of-delta, Prometheus-style bucket widths |
| `CodecGorilla` | values (`float64`) | `EncodeFloats` / `DecodeFloats` | Gorilla XOR (leading/trailing-zero reuse) |
| `CodecT64` | low-range `int64` | `EncodeIntsT64` / `DecodeIntsT64` | ClickHouse T64 bit-transpose + crop |
| `CodecDict` | low-cardinality `[][]byte` | `EncodeBytes` / `DecodeBytes` | dictionary; 1 byte/row ≤256 distinct, 2 bytes ≤65536, flat fallback above |
| `CodecDecimal` | `float64` | `EncodeFloatsDecimal` / `DecodeFloatsDecimal` | scaled-decimal + nearest-delta, optionally lossy |
| `CodecID128` | 128-bit ids (`[]U128`) | `EncodeU128` / `DecodeU128` | run-length (distinct id + run length); optimal for a sorted SeriesID sort key |

Dictionary codec specifics (the most built-out):
- `DecodeBytes` materializes one `[]byte` header per row (the gather form).
- `DictColumn` + `DecodeBytesDict` are the **split form**: unique `Entries` plus the raw
  per-row `IDs` (`IDWidth` 1/2, or 0 for the flat fallback), with `Len()`/`At(row)`
  deferring the gather. This lets a caller filter on ids before paying the gather cost
  (~9× faster decode when most rows are filtered out). All returned slices alias the
  source.
- `DictEncoder` (`NewDictEncoder`) is a reusable encoder that keeps its hash map and
  scratch slices warm across batches; `EncodeBytes` is the one-shot equivalent and is
  byte-identical to it.

### 2.3 `encoding/compress` — block compression wrapper

`Compressor` (`NewCompressor(alg, level)`) wraps a column/block in a 1-byte-flagged
frame: `FlagRaw` (stored uncompressed) or `FlagCompressed`. `Compress` automatically
falls back to raw when compression does not shrink the input. Encoders/decoders are
pooled via `sync.Pool`.

- `AlgorithmZSTD` — implemented (klauspost/compress zstd).
- `AlgorithmNone` — identity (always raw).
- `AlgorithmLZ4` — currently a stub that takes the raw path.

---

## 3. Supporting runtime (`pool/`)

`pool.ByteIntMap` is an open-addressing `[]byte → int` hash map using xxh3 hashing and
`bytes.Equal` key comparison, designed for the dictionary-encoding hot path (it beats
`map[string]int` by avoiding string-key hashing and conversion). It is poolable
(`NewByteIntMap`/`PutBack`) and reusable (`Reset` keeps the backing arrays). It backs
both `chunk.EncodeBytes` and `chunk.DictEncoder`.

---

## 3a. Storage backends (`backend/`)

The L1 seam: a common `Backend` interface over whole-object, slash-delimited keys —
`Read(ctx,key)([]byte)`, `Write(ctx,key,[]byte)`, `List(ctx,prefix)([]string)`,
`Delete(ctx,key)`, `PutIfAbsent(ctx,key,[]byte)(bool)`, plus `IsEphemeral()`. Absent keys
return an error satisfying `errors.Is(_, backend.ErrNotExist)`. **`PutIfAbsent`** is the
conditional-write (CAS) primitive on which atomic manifest / block-list commits build
(single-writer-wins with no Raft): a guarded map insert (memory), an exclusive `os.Link`
(file), and S3 `If-None-Match: *` (s3).

- **`backend.Memory()`** (root package) — the ephemeral reference backend: a concurrent
  map that copies on both `Write` and `Read`, so stored objects are immutable and never
  alias a caller's buffer. The default in tests.
- **`backend/file`** — a directory tree. Keys map to paths under a root (with a `..`
  traversal guard); `Write` is atomic via a temp file + `fsync` + `rename`; `PutIfAbsent` is
  an atomic exclusive create via temp + `os.Link` (fails `EEXIST` if the key exists).
- **`backend/s3`** — the object-store-native backend. The store-specific calls sit behind a
  small `ObjectStore` interface, so the Backend's contract logic — root-prefixing, sorted
  listing, `404 → ErrNotExist`, conditional put, and an existence-checked `Delete` (S3's
  `DeleteObject` is idempotent) — is covered by the conformance suite over an in-memory fake.
  `NewAWS` adapts `aws-sdk-go-v2` (GetObject / PutObject±If-None-Match / HeadObject /
  idempotent DeleteObject / paginated ListObjectsV2); a faithful map-based fake `AWSAPI` runs
  the full suite through the adapter (CAS included, via If-None-Match), and an **always-on
  integration test** runs the core suite over a real S3 protocol implementation — the
  embeddable `go-faster/fs` server on an `httptest` listener (no Docker), exercising actual
  HTTP+XML. **This is the only package importing the AWS SDK.**
- **`backend/bucketindex`** — a compact, versioned binary index (block list + per-part time
  bounds) stored as one object, so a stateless reader enumerates and time-prunes a tenant's
  parts (`Overlapping(start,end)`) without a full bucket `List`. Fuzzed for decode safety,
  golden-tested for format stability.
- **`backend/backendtest`** — a shared conformance suite (`Run(t, factory)`) that memory,
  file, and s3 (over both fakes) pass under `-race`, proving they are interchangeable.

The **stateless read path** is wired at the engine level (§3f, `Engine.LoadParts`): a fresh
engine reconstructs its part set (from `bucketindex`) and its identity index (from a durable
series object) from the backend alone — no local state — and serves matcher-based queries
with full labels. Facade-level recovery (`Open` → per-tenant `LoadParts` + WAL replay) is the
remaining wiring.

## 3b. Part format (`block/`)

The L2 on-disk unit: an **immutable, columnar part**. A part is **not** a single blob —
it is a set of backend objects under one key prefix, so a reader fetches only the
columns it references (projection pushdown without ranged reads):

```
{prefix}/manifest   schema + stats, CRC32C-checked, WRITTEN LAST = the commit point
{prefix}/marks      sparse granule index (sort-key min/max per granule)
{prefix}/c/{i}      column i's stream (absent for a constant-collapsed column)
```

- **Columns** (`column.go`) carry one physical `Kind` — `KindInt64` / `KindFloat64` /
  `KindBytes` / `KindInt128`. A codec is selected per kind (`CodecDoD`/`CodecT64` for
  int64, `CodecGorilla`/`CodecDecimal` for float64, `CodecDict` for bytes, `CodecID128`
  for int128; overridable). The encoded chunk stream is wrapped in a `compress` frame. Per
  column the writer records min/max and **collapses a constant column** to a single value
  in the manifest (no data object) — the OTel resource-attribute win. `KindInt128` (the
  metric SeriesID sort key) is the exception: it carries no min/max or constant value, as
  its RLE codec already collapses a single-id run to a few bytes. The lazy `ColumnReader`
  decodes on demand: `Int64`/`Float64`/`ID128` into a reusable slice, `Bytes` into
  `chunk.DictColumn` split form, and synthesizes constants with no I/O.
- **Manifest** (`manifest.go`) is a versioned binary record (magic `OTPM`, version, row
  count, time range, granule size, per-column descriptors) with a trailing CRC32C. Decode
  bounds-checks every field and never panics (fuzzed).
- **Marks** (`marks.go`) is the sparse granule index over the sort-key (timestamp)
  column: per-granule first row + min/max, delta-encoded, CRC-checked. `Overlapping(lo,hi)`
  prunes granules for a time window (used by the future fetcher).
- **PartWriter / PartReader** (`part.go`): `WritePart(ctx, backend, prefix, w)` writes the
  column and marks objects first, then the manifest last (commit). `OpenPart` reads only
  the manifest; `Column`/`Marks` read their objects lazily. An incompletely written part
  (no manifest) is not openable.

## 3c. Identity & index (`index/`)

The L3 indexing layer maps query matchers to the series that satisfy them.

- **Identity** lives in `signal`. The leaf is the typed OTel attribute model: `Value` =
  the AnyValue sum (string/bool/int/double/bytes/array/map, scalars inline + `[]byte` for
  zero-alloc projection), grouped into a sorted `Attributes` set. A **`Series`** is the
  full OTel identity backbone — **`Resource` (schema_url + attrs) + `Scope` (name,
  version, schema_url, attrs) + the data-point `Attributes`** — so series differing only
  in resource or scope are distinct (not collapsed into one attribute bag). Its
  content-addressed **`SeriesID`** is a **128-bit** xxh3 of a canonical, type-tagged,
  length-delimited pre-image (maps hash order-independently, arrays keep order, `int 5`/
  `"5"`/`5.0`/empty are distinct). 128-bit because content addressing has no allocator to
  resolve a collision. `AppendValue`/`DecodeSeries`/`DecodeAttributes` are the reversible
  binary codec (used by the WAL and value interning). Signal-specific identity (metric
  name/unit/temporality) folds into the pre-image at the `signal/metric` layer.
- **`index/symbols`** — a `[]byte → uint32` interning table (via `pool.ByteIntMap`,
  no string conversion) with a CRC32C serialize/decode. Names and typed-value encodings
  intern to small ids.
- **`index/series`** — `SeriesID ↔ Series`. `Add` is idempotent (id is the identity hash)
  and retains a deep copy, so a query reconstructs labels from an id and replay is
  dedup-safe.
- **`index/postings`** — the inverted index, keyed on **interned symbol ids** (`nameID →
  valueID → sorted []SeriesID`), so it is zero-alloc and **type-preserving** (the value id
  comes from the value's typed encoding). Lazy set-op iterators (`Intersect`/`Merge`/
  `Without` with galloping `Seek`, property-tested vs a naive reference) compose lists.
  Matching is **callback-based**: `Select(nameID, func(valueID) bool)` hands the predicate
  a candidate value id, which the caller decodes to a typed `signal.Value` and tests —
  storage imports no query-language operator; negation/equality compose from the
  primitives (`Get`/`Without`/`WithoutName`).

## 3d. Write-ahead log (`wal/`)

CRC-framed records (`[uvarint len][type][payload][CRC32C]`) appended to numbered segment
files (`SegmentWriter`, rotating at a size limit). A series record carries the `SeriesID`
+ the typed attribute encoding. `ReplayDir` stitches segments in order; replay tolerates a
torn final record (crash recovery), surfaces a bad-CRC complete record as corruption, and
skips unknown record types (forward-compat). Replaying the log rebuilds the symbols +
series + postings index — the path that reconstructs the head after a restart.

## 3e. Metrics ingest model + projection (`signal/metric/`)

**The ingest boundary is the internal model, not OTel-Go pdata.** `metric.Metrics`
(`metrics.go`) is a `[]byte`-based, OTLP-shaped batch — `ResourceMetrics → ScopeMetrics →
Metric → NumberPoint` — that mirrors the OTLP hierarchy but holds all identity as `[]byte`,
so an embedder decoding OTLP protobuf can build it by aliasing the decode buffer and
projection copies nothing. It is **resettable and pool-friendly**: the `Add*` builders
reuse the retained capacity of the nested slices (the resettable-arena `grow` trick), so a
`Reset`-then-rebuild cycle (or a `GetMetrics`/`PutMetrics` pool round-trip) allocates
nothing across ingest calls.

`metric.Project(md, emit)` walks the batch for **Gauge and Sum** number points; every point
in a `Metrics` batch is well-formed by construction, so projection rejects nothing
(out-of-order rejection is the engine's concern downstream). Metric identity (`Identity`)
folds the metric-specific fields — name, unit, kind, temporality, monotonicity — into
**reserved labels** (`__name__`, `__unit__`, …) on a `signal.Series`, so the one
identity/index machinery covers metrics and a query matches `__name__` like any other
label. `emit` is called **once per metric** with a `*Batch` — the projected id/timestamp/
value columns for that metric's points, plus the metric context to materialize a full
`signal.Series` lazily. Emitting a whole metric (rather than a call per point) lets the
engine take its lock and resolve the tenant once per metric, not once per point. The `Batch`
is pooled and reused across `Project` calls (its column buffers persist); it and the data it
aliases are valid only for the `emit` call.

**The series id is computed on the hot path without allocating or sorting.** A `projector`
hoists the invariant work: the resource‖scope hash pre-image is built once per scope group
and kept **resident** at the front of a reused buffer (never re-copied per point); the five
folded reserved labels are built once per metric in sorted-key order. Per point, only the
point's (already-sorted) attributes are merged with the reserved labels in **one pass** —
emitting the canonical hash pre-image directly via `signal.AppendKeyValueHashInput`, never
materializing a combined sorted `[]KeyValue` — and hashed. The result is byte-identical to
`Batch.Series(i).Hash()` (a fuzz test pins this, including reserved-key collisions), so
`Identity.ToSeries` stays the reference materialization, used only when the engine reports a
new series. This is what makes ingest ~zero-alloc: the per-point hash, sort, and
merged-slice allocations the naïve path paid (≈7 allocs/point) collapse to none.

**pdata is confined to one optional adapter.** `otlp/pdataconv` converts the collector
`pmetric.Metrics` into `metric.Metrics` (`AppendMetrics`, holding the `pcommon` →
`signal.Value` conversion). It filters and **counts** the points the internal model does not
yet represent — Histogram / ExpHistogram / Summary, and value-less number points — returning
`dropped` so the caller folds them into an OTLP partial-success. It is the only package that
imports `go.opentelemetry.io/collector/pdata`; the conversion necessarily allocates (pdata
stores keys/values as Go strings), which is why it sits off the hot path and embedders that
own their OTLP decoder build `metric.Metrics` directly.

## 3f. Engine (`engine/`) — the single-node metrics vertical

One `Engine` per tenant ties the index, parts, and WAL into a working ingest+query path.
It is safe for concurrent use (one `sync.RWMutex`).

- **Head** (`head.go`) is the in-memory write buffer: the index (`symbols` + `series` +
  `postings`) plus per-series `(ts, value)` append buffers. `append` interns every
  queryable label (resource + scope attributes, scope name/version as reserved labels, and
  the folded point attributes), registers the series on first sight, and rejects samples
  older than `newest − OOOWindow`. The **series index outlives a flush** — only sample
  buffers are drained — so flushed series stay queryable and re-appends don't re-index.
  `AppendBatch` is the hot ingest path: it takes a metric's **precomputed** `SeriesID` plus
  timestamp/value columns and a `materialize func(i) signal.Series` invoked **only on first
  sight**, and ingests the whole run under a **single lock**. Per sample, `appendByID` does a
  **single map probe** — a present sample buffer means the series is known, so the series
  index is never consulted and no `signal.Series` is built or hashed; only an absent buffer
  falls back to the (authoritative) series index. `Append` (full `signal.Series` in, hash
  inside) remains for callers that already hold an identity.
- **Flush** (`flush.go`) drains the head's buffered samples into one **flat 3-column part**
  `[series:int128, ts:int64, value:float64]`, one row per sample, sorted by `(series, ts)`,
  written via `block.PartWriter` under `{tenant}/metrics/{seq}`. After the part is written,
  flush updates the two durable index objects (§3a): the **bucket index**
  (`{prefix}/bucket-index.bin` — part list + per-part time bounds) and the **identity index**
  (`{prefix}/series.bin` — every series' reversible labels). Merge updates both too,
  committing the new part set to the bucket index *before* deleting the source parts.
- **Stateless reconstruction** (`index.go`) — `Engine.LoadParts` rebuilds a fresh engine's
  durable state from the backend alone: the part set from the bucket index, and the
  postings/series index from the identity object (so matchers resolve and batches carry real
  labels — parts store only ids). This is the object-store-native read path: any node serves a
  tenant's flushed data without the (local) WAL. WAL `Replay` is complementary, restoring only
  the *unflushed* head.
- **Part fetch** (`part.go`) — `openPart` rebuilds a `SeriesID → [rowStart,rowEnd)` index
  by scanning the series column once (each series is one contiguous run); `mergeInto`
  decodes a series' `ts`/`value` sub-slice within the window.
- **Merge + retention** (`merge.go`) is the one background-merge engine: `Merge(retainFrom)`
  compacts every part into one, merging samples per series by timestamp (freshest wins on a
  tie) and dropping samples older than the absolute `retainFrom` cutoff before deleting the
  source parts. Retention is a timestamp, not a clock read, so the engine is deterministic.
- **Fetch** (`engine.go`) implements the fetch contract: it resolves matchers to series
  over the index, then merges each series' head buffer ∪ every part by timestamp into one
  batch. `Close` flushes the head. `Reset(ctx)` is the inverse of accumulation: it replaces
  the head with an empty one, drops the part handles, and deletes this engine's part objects
  from the backend (scoped to `{Prefix}/`), returning the engine to its `New` state for
  reuse (tests/benchmarks) without reallocating it.

The metric part column layout and the WAL sample record are **wire-stable** on-disk
formats.

## 3g. Fetch contract (`query/fetch/`)

The dual-shape read seam (metrics shape today). A `Request{Tenant, Start, End, Matchers}`
carries **callback matchers** — `Matcher{Name, Match func(signal.Value) bool}`, never an
operator enum, so equality/regex/negation live in the language layer (§3h) and the storage
layer stays operator-free. `Fetcher.Fetch` returns an `Iterator` of `*Batch{ID, Series,
Timestamps, Values}` (one batch per matching series for M3). `SliceIterator` and `Drain` are
the in-memory helpers. **`Merge(fetchers...)`** is the fan-out combinator: it runs a Request
against several fetchers and merges their batches by series id (timestamp-ordered, later child
wins a duplicate timestamp) — the basis for multi-tenant / cross-tenant reads (via
`Storage.Fetcher`) and, later, cluster fan-out across replicas. A single child is a
pass-through; child batches are cloned, never mutated.

## 3h. PromQL adapter (`query/promql/`) — optional, embedder-facing

The library does **not** implement PromQL (or any query language): that is the embedder's job
(e.g. go-faster/oteldb already has PromQL/LogQL/TraceQL engines). What the library exposes is
the **fetch seam** (`Storage.Fetcher`). `query/promql` is an **optional adapter** that bridges
that seam to the Prometheus `storage.Queryable` interface, so an embedder using the Prometheus
PromQL engine can point it at this store with no glue. It contains **no engine** — the embedder
constructs and drives `promql.Engine` itself. It is the only package that imports
`github.com/prometheus/prometheus`, and importing it is opt-in; the core stays prometheus-free.

What the adapter does (the non-trivial, reusable part):

- **Matcher lowering (condition extraction lives here, never in storage).** A Prometheus
  `*labels.Matcher` becomes a `fetch.Matcher` whose `Match` runs the matcher over the typed
  value's text projection. Only **index-safe** matchers (those that do *not* match the empty
  string) are pushed into the `fetch.Request`: a negated/absent matcher (`!=`, `!~`, `=""`)
  would wrongly drop series lacking the label via the postings index, so every fetched series
  is **re-checked against the full matcher set** (absent label = empty string) for exact
  semantics.
- **Label projection.** A `signal.Series` becomes a `labels.Labels`: resource/scope/point
  attributes flatten to string labels, scope name/version under `otel.scope.*`, the internal
  reserved labels (`__unit__`/`__kind__`/`__temporality__`/`__monotonic__`) hidden, `__name__`
  kept. Each fetched batch becomes a Prometheus `SeriesSet` of float samples.
- **Time units.** Storage is unix **nanoseconds**, Prometheus is **milliseconds**; the
  adapter converts both directions (querier window and sample timestamps).

The embedder owns evaluation and result types: it runs `promql.Engine` over the adapter and
consumes Prometheus' own `Vector`/`Matrix`/`Scalar`, so the library defines no query-result
type and the core leaks nothing prometheus-shaped.

---

## 4. Public surface (`storage` root package)

The embedder-facing API. The construction and **metrics ingest+read path are wired and
working**; logs/traces/profiles ingest and the query-language path still return
`ErrNotImplemented`. The `Write*` methods take the library's internal, `[]byte`-based
ingest batches (`metric.Metrics`, and placeholder `log.Logs`/`trace.Traces`/
`profile.Profiles`), **not pdata** — OTel-Go users convert via `otlp/pdataconv` (§3e).

- **`Storage`** — the facade. `Open(ctx, Options, ...Option)` and `InMemory(...Option)`
  construct it (validation, defaulting, tenant-resolver wiring, and — when `FlushInterval`
  is set — the background maintenance loop start here); `Close(ctx)` stops the loop and
  flushes every tenant engine. `WriteMetrics(ctx, md)` is **fully implemented**: it
  projects the internal `metric.Metrics` batch, derives each point's **tenant from its Resource+Scope**
  via the `Options.Tenant` callback (no tenant argument), and appends to that tenant's
  lazily-created `engine.Engine` (one per tenant, parts under `{tenant}/metrics`) through the
  `AppendBatch` fast path (one locked call per metric), caching the resolved engine across a
  tenant-contiguous run of metrics. It returns `Accepted` (OTLP partial-success: rejected
  counts out-of-order drops; unsupported kinds and value-less points are filtered upstream by
  the producer). `WriteLogs`/`WriteTraces`/`WriteProfiles` are later
  verticals (`ErrNotImplemented`). A single **maintenance loop** periodically flushes +
  merges every engine, applying per-tenant retention from the resolved policy. `Reset(ctx)`
  discards all ingested data (every engine's head + flushed parts), retaining the engines
  for reuse; it is gated to an **ephemeral backend** (`ErrNotEphemeral` otherwise) and is
  meant for tests/benchmarks that reuse one store across runs. `Fetcher(tenants...)` is the
  **read seam**: it returns a `fetch.Fetcher` over the named tenants' data (head ∪ parts) —
  one tenant, several (a **multi-tenant** fan-out), or none ⇒ **all** tenants (a
  **cross-tenant** query). A fan-out merges by series id via `fetch.Merge`, federating a
  series with equal labels across tenants into one. Always usable: an empty fetcher when no
  tenant matches or after `Close`, so callers need not special-case "no data". There is
  deliberately **no `Query` / query-language method**: the store is language-agnostic and the
  embedder drives its own engines over the fetch contract (the optional `query/promql` adapter
  bridges to the Prometheus engine).
- **`Options` / `Option`** (`options.go`) — config struct plus functional options
  (`WithBackend`, `WithCluster`, `WithTenancy`, `WithEncoding`, `WithDurability`,
  `WithWALDir`, `WithFlushThresholdBytes`, `WithFlushInterval`, `WithOOOWindow`).
  `Durability` selects the durability mode; an ephemeral backend with no explicit choice
  defaults to the in-memory engine.
- **`Query` / `Lang` / `Result` / `Accepted`** — the query request (language selected by
  `Lang`), its result, and the ingest acknowledgement type.

### Shared model types

- **`signal`** — signal-neutral model: the `Signal` enum (`Metric`/`Log`/`Trace`/
  `Profile`), `ParseSignal`, `TenantID`, and the typed identity primitives (`Value`,
  `KeyValue`, `Attributes`, the 128-bit `SeriesID`, and the attribute binary codec) — see
  §3c.
- **`tenant`** — policy model: `Limits`, `Retention`, `Downsample`, and the composed
  `Policy`, resolved per tenant id through a `Resolver` (`ResolverFunc` adapter;
  `Default()` returns an empty-policy resolver). Multi-tenancy, retention, and
  downsampling are consumer-supplied callbacks keyed by tenant id.
- **`backend`** — the L1 seam (detailed in §3a): `Read`/`Write`/`List`/`Delete` over
  whole-object keys, with memory and file implementations. s3 + CAS pending.

---

## 5. Cross-cutting invariants (enforced today)

These hold in the implemented code and must be preserved by changes:

- **Zero-alloc hot paths.** Codecs use append-style APIs (`func(dst []byte, …) []byte`);
  callers own and reuse buffers. Parsers, scratch slices, and the dict map are pooled
  and `Reset`. Decoders return views aliasing the source (`ReadBytesView`,
  `DictColumn`) instead of copying, where the lifetime is bounded.
- **One physical engine, many front-ends.** Query languages and signals are meant to be
  thin layers over the shared columnar engine and the fetch contract; storage-layer code
  must not learn a language's or signal's concepts. (The seam exists; the layers above it
  are not built yet.)
- **Immutable, in-memory-first.** The in-memory/ephemeral path is first-class — every
  layer must work with no disk or object store. `backend.Memory()` is the reference
  backend.
- **Stable formats.** The `Codec` enum, the per-stream header, each codec's framing, the
  part formats (manifest `OTPM`, marks `OTMK`, per-column object framing, the
  `{prefix}/manifest|marks|c/{i}` key layout), the **attribute hash/binary encoding** (the
  SeriesID pre-image), the **symbol table** (`OTSY`), the **WAL record framing** (series +
  sample records), and the **metric part column layout** (`[series:int128, ts:int64,
  value:float64]` sorted by `(series, ts)`) are all persisted/wire-stable. Changing any of
  them is an architectural change (golden tests guard formats; bump the version and update
  this file too).

### Testing discipline

Implemented packages ship with ≥90% coverage, table/property/round-trip tests, fuzz
targets for every codec and the bitstream (`encode∘decode == identity`), and benchmarks
on the hot paths. `go test ./...`, `go vet ./...`, and `golangci-lint run ./...` are all
green; the tree is `gofmt`/`goimports` clean.

---

## 6. Package map

```
.                     storage facade: Storage, Open/InMemory, Options, per-tenant engines, maintenance loop [implemented: metrics ingest+read; logs/traces/profiles & query-lang stubbed]
encoding/             umbrella doc for the codec layers
  encoding/bitstream  MSB-first bit Writer/Reader                                      [implemented]
  encoding/chunk      DoD / Gorilla / T64 / dict / decimal / id128 column codecs      [implemented]
  encoding/compress   zstd/none block wrapper (lz4 stub)                              [implemented]
pool/                 ByteIntMap (xxh3) for dict building                              [implemented]
signal/               typed Attributes/Value, Resource/Scope/Series identity, 128-bit SeriesID, Signal, TenantID [implemented]
  signal/metric       []byte-based OTLP-shaped Metrics ingest batch (resettable/pooled) + identity + projection (gauge/sum) [implemented; histogram/summary deferred]
  signal/log,trace,profile  placeholder ingest batch types (keep facade pdata-free)    [stub; verticals deferred]
otlp/pdataconv        optional OTel-Go bridge: pmetric.Metrics → metric.Metrics (only package importing pdata) [implemented for metrics]
tenant/               Limits/Retention/Downsample/Policy, Resolver                     [implemented]
backend/              Backend interface (Read/Write/List/Delete/PutIfAbsent) + memory (root) [implemented]
  backend/file        directory-tree backend; atomic write + exclusive PutIfAbsent (os.Link) [implemented]
  backend/s3          object-store-native backend over ObjectStore + aws-sdk-go-v2 adapter   [implemented; in-process go-faster/fs S3 integration test]
  backend/bucketindex versioned block-list index (time-pruned part enumeration, no full LIST) [implemented]
block/                immutable columnar part format: column/marks/manifest/part        [implemented]
index/                symbols (intern) · series (id↔attrs) · postings (set-ops/matchers) [implemented; bloom seam only]
wal/                  CRC-framed segmented write-ahead log + replay                    [implemented]
engine/               head · flush · background-merge · retention · fetch (metrics)    [implemented]
query/fetch           callback-matcher fetch contract (Request/Matcher/Iterator/Batch) [implemented for metrics; the library's query surface]
query/promql          OPTIONAL adapter: fetch → Prometheus storage.Queryable (no engine) [implemented; only package importing prometheus]
cluster/              etcd ring · HRW sharding · replication · rebalance               [seam only]
```

"Seam only" packages currently contain their `doc.go` (and, where noted, an interface or
config type) that fixes the boundary; they have no behavior yet. As each is implemented,
move its row to "implemented" here and add a section above.
