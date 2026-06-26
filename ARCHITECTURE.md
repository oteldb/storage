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
(`storage`) which ingests OTLP **metrics, logs, traces, and profiles** (`signal/metric` /
`signal/log` / `signal/trace` / `signal/profile` projection) and routes them per-tenant, single-node
or **clustered** (etcd ring, primary-authoritative replication, read fan-out). Logs, traces, and
profiles share one schema-driven **record engine** (`recordengine`). The remaining layers — query
languages (PromQL/LogQL/TraceQL/…) and the query planner — are the embedder's, reached through the
fetch seam.

---

## 1. Layered model

The design is a single columnar engine with swappable front-ends and backends
(`DESIGN.md` §3). The layers, top to bottom, and what backs each one **right now**:

| Layer | Concern | Realized today |
|---|---|---|
| L6 Query languages | promql/logql/traceql/genericql | **owned by the embedder, not the library** — the library exposes the fetch seam; `query/promql` is an optional fetch→Prometheus-Queryable *adapter* (no engine) |
| L5 Query engine | plan IR · sharding · streaming exec · cache | **embedder's concern** (it drives its own engine over the fetch contract); the planner/streaming exec is out of scope — but the part expressible purely over the fetch contract (split-by-interval + results cache) ships as `query/scale` decorators (§3g) |
| L4 **Fetch contract** | **callback matchers + column conditions + window → iterator of batches** | **the library's query surface** (`query/fetch`, exposed via `Storage.Fetcher`/`Storage.LogFetcher`/`Storage.TraceFetcher`/`Storage.ProfileFetcher`); dual-shape implemented for **metrics, logs, traces, and profiles** (matchers + Conditions + Projection + SecondPass), with `query/scale` split + cache decorators |
| L3 **Engine** / **Index / WAL** | **head · flush · merge · retention** / **symbols · series · postings · token blooms** / **write-ahead log** | **metrics engine** (`engine`) **and the shared record engine** (`recordengine`, schema-driven — **logs, traces, profiles**, with an optional content-addressed side store) implemented; **index + wal implemented**, with `index/bloom` per-column token/equality filters |
| L2 **Part** / **Encoding** | **immutable parts · per-column objects · manifest** / **bitstream · codecs · compress** | **both implemented** (`block`, `encoding`) |
| L1 **Backend** | file · s3 · memory behind one interface | **memory + file + s3 implemented**, with `PutIfAbsent` CAS; `bucketindex` for stateless part enumeration |
| L0 Cluster | etcd ring · HRW sharding · RF=3 · rebalance | **full L0 implemented**: ring, membership, quorum replication, rebalance plan + executor, clustered ingest, read fan-out — for **metrics, logs, traces, and profiles** (one signal-discriminated write/read path) |

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
| `CodecBytesRaw` | high-cardinality `[][]byte` (ids) | `EncodeBytesRaw` / `DecodeBytes` | no dictionary: fixed-width block when all values share a length (e.g. a span id, ~unique → a dictionary is pure overhead), else length-prefixed inline |
| `CodecDecimal` | `float64` | `EncodeFloatsDecimal` / `DecodeFloatsDecimal` | scaled-decimal + nearest-delta, optionally lossy |
| `CodecID128` | 128-bit ids (`[]U128`) | `EncodeU128` / `DecodeU128` | run-length (distinct id + run length); optimal for a sorted SeriesID sort key |

The three byte-column forms (`flagDict`/`flagFlat`/`flagFixed`) share one self-describing stream
header, so `DecodeBytes`/`DecodeBytesDict` select the form from the stream's flag byte without
consulting the column's `Codec`. All byte-column decoders bound every length/count read from the
stream before allocating and reject out-of-range dictionary ids, so decode never panics on corrupt
input (fuzzed). Wire layouts are pinned by golden files (`_golden/`, via `go-faster/sdk/gold`).

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

The **stateless read path** is wired end-to-end: at the engine level (§3f,
`Engine.LoadParts`) a fresh engine reconstructs its part set (from `bucketindex`) and its
identity index (from a durable series object) from the backend alone; at the facade level
`Storage.Open` calls `recover`, which — for a **durable** backend — discovers tenants by their
bucket-index objects and `LoadParts` each, so a fresh process serves data a previous one
flushed (it is a no-op for an ephemeral backend).

**The unflushed head is recovered from a per-tenant WAL** when `Options.WALDir` is set (§3d, §4):
each `*EngineFor` attaches a resuming `wal.SegmentWriter` under `{WALDir}/{tenant}/{signal}`, every
engine logs its head writes there, and a flush **checkpoints** the WAL (deletes the now-durable
segments — `SegmentWriter.Checkpoint`) so replay is bounded. `recover` replays each WAL directory
after `LoadParts`, restoring the records — and, for profiles, the symbol store (the `recordSide`
frame) — a crash left unflushed.

**Recovery is exactly-once** (record signals): a flush generation (*epoch*) advances only on a head
flush; WAL segments encode their epoch in the filename, and the watermark of the last-flushed epoch
lives in the **bucket index** (`FlushedEpoch`), so it advances *atomically with part-discoverability*
— the very object `recover` reads. Replay (`wal.ReplayDirFrom`) skips segments at or below the
watermark, so even a crash in the window between a part committing and its WAL being deleted re-applies
nothing already flushed. (Metrics don't track the epoch — their merge dedup makes the same window
self-healing.) The **fsync policy** (`Options.WALSync`) trades durability for throughput: `None`
(default — OS page cache, process-crash safe), `Always` (fsync per record, power-loss safe), or
`Interval` (a background `runWALSync` fsyncs every engine's WAL on a timer).

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
files (`SegmentWriter`, rotating at a size limit). Record types: a **series** record (`SeriesID`
+ typed attribute encoding), a **samples** record (metrics), an opaque **records** payload (the
record signals), and an opaque **side** record (a content-addressed side-store delta — the profiles
symbol store). `ReplayDir` stitches segments in order; replay tolerates a torn final record (crash
recovery), surfaces a bad-CRC complete record as corruption, and skips unknown record types
(forward-compat). Replaying the log rebuilds the symbols + series + postings index and the head —
the path that reconstructs unflushed state after a restart.

Segment files are named `{seq}-{epoch}.wal`: `seq` orders replay, `epoch` is the flush generation of
the records (so a segment self-describes which generation it holds). `Create` **resumes** an existing
directory (the writer opens lazily on first write, beyond the prior run's segments, never truncating
them); `SetEpoch` stamps the generation onto new segments; `Checkpoint` deletes the segments a flush
made durable (truncate-on-flush). `ReplayDirFrom(minEpoch, …)` replays only segments past the
watermark — the basis for exactly-once recovery (§3a). `SetSync` enables a per-write fsync. The facade
attaches one writer per (tenant, signal) engine and replays them on recovery (§3a, §4).

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
`signal.Value` conversion). Gauge and Sum points convert directly; **Histogram,
ExponentialHistogram, and Summary points are stored by classic decomposition** (`histogram.go`):
each point explodes into ordinary float series the columnar engine already handles —
`<name>_count`, `<name>_sum`, and **cumulative** `<name>_bucket{le=…}` for histograms (the
Prometheus convention, so an embedder's `histogram_quantile` works directly), and
`_count`/`_sum`/`<name>{quantile=…}` for summaries. An exponential histogram is first converted to
explicit `le` buckets from its scale (`base = 2^(2^-scale)`, negative/zero/positive buckets folded
into ascending cumulative `le` bounds), then decomposed the same way — so all three types reuse the
engine, merge, downsample, sampling, and fetch paths with **no histogram-specific storage code**.
Only value-less number points are still **counted** in `dropped` (folded into an OTLP
partial-success). It is the only package that imports `go.opentelemetry.io/collector/pdata`; the
conversion necessarily allocates (pdata stores keys/values as Go strings, and decomposition fans
out a point into many series), which is why it sits off the hot path — embedders that own their
OTLP decoder build `metric.Metrics` directly (and, for histograms, decompose themselves).

## 3f. Engine (`engine/`) — the single-node metrics vertical

One `Engine` per tenant ties the index, parts, and WAL into a working ingest+query path.
It is safe for concurrent use (one `sync.RWMutex`). Reads run under the **shared** lock, so
several fetches proceed at once (e.g. `query/scale` split-by-interval issuing one per sub-window).
The label index sorts lazily on the first read after a write, mutating in place — so `Fetch`
performs that one-time sort under the **exclusive** lock first (hold the read lock; while the
index is unsorted, drop to the write lock, `EnsureSorted`, re-check), after which concurrent
readers only read. The `postings.MemPostings` exposes `Sorted()`/`EnsureSorted()` for exactly
this caller-owned-synchronization upgrade.

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
- **Merge — compaction + retention + downsampling + recompression** (`merge.go`, `downsample.go`,
  `recompress.go`) is the one background-merge engine; all four modes are one pass over the immutable
  parts (no separate subsystem). `MergeWith(MergeOptions{RetainFrom, Downsample, Recompress})` (and
  the convenience `Merge(retainFrom)`) compacts every part into one, merging samples per series by
  timestamp (freshest wins on a tie), dropping samples older than the absolute `retainFrom` cutoff,
  **downsampling** the survivors by the supplied tiers, and — when the merged part is **fully cold**
  (its newest sample predates `Recompress.Before`) — rewriting it with a higher-ratio compression
  profile (`RecompressSpec{Before, Algorithm, Level}` → zstd at a chosen level) before deleting the
  source parts. Recompression is **decode-transparent**: the reader keys decompression off the
  per-column algorithm in the manifest (level is decode-irrelevant), so it is a pure ratio/CPU
  trade-off with **no format change**; a lone cold part is recompressed once and then skipped (the
  fixed point checks the part's recorded algorithm, so re-merges don't churn). A `DownsampleTier{Before, Interval, Agg}`
  rolls every sample older than `Before` into one representative per absolute `Interval`-aligned
  bucket, combined by a `signal.Aggregation` (last/first/min/max/sum/avg/count); a sample is assigned
  the coarsest tier it qualifies for, and younger samples stay raw. Both the rollup and the
  compaction are **weight-aware** (the lossy-sampling scale factor, §3k): a sampled series stays
  unbiased through a merge. Both `Before` and `retainFrom`
  are absolute timestamps (not clock reads), so the whole merge is deterministic — the caller
  ([storage.Storage]) resolves the tenant's `tenant.Downsample` policy against one `now` per pass.
  Bucket alignment is to the absolute grid (not ingest time), so the rollup of a range is independent
  of when the merge runs and repeated merges are a **fixed point** for last/first/min/max/sum/avg (a
  one-sample bucket aggregates to itself); count is the documented non-idempotent exception. Records
  (logs/traces/profiles) carry retention only — downsampling and recompression are metrics-specific.
- **Fetch** (`engine.go`) implements the fetch contract: it resolves matchers to series
  over the index, then merges each series' head buffer ∪ every part by timestamp into one
  batch. `Close` flushes the head. `Reset(ctx)` is the inverse of accumulation: it replaces
  the head with an empty one, drops the part handles, and deletes this engine's part objects
  from the backend (scoped to `{Prefix}/`), returning the engine to its `New` state for
  reuse (tests/benchmarks) without reallocating it.
- **Replication apply** (`engine.go`) is the engine's cluster-facing surface (used by §3i):
  `ApplyPrimary(walBytes)` OOO-checks each sample, appends the accepted ones, and returns a
  WAL payload of just the accepted set plus the rejected count — the shard primary's single
  authoritative decision. `ApplyReplicated(walBytes)` applies a payload verbatim (no OOO
  re-check, like WAL replay), the secondary receive path. `RefreshReplica(ctx)` reloads parts
  from the shared store and trims the head to the still-unflushed window, bounding a replica's
  memory after the owner flushes.

The metric part column layout and the WAL sample record are **wire-stable** on-disk
formats.

## 3g. Fetch contract (`query/fetch/`)

The **dual-shape** read seam. A `Request{Tenant, Signal, Start, End, Matchers, Conditions,
AllConditions, Projection, SecondPass}` carries two operator-free predicate families: **callback
matchers** — `Matcher{Name, Match func(signal.Value) bool}` — that resolve **identity** over the
postings index (a metric series, a log stream), and **callback conditions** —
`Condition{Column, Match, Tokens, Equal}` — that filter the **per-record columns** within that identity
(a log record's severity/body/attributes). Neither is an operator enum, so equality/regex/
negation and condition extraction live in the language layer (§3h) and storage stays
operator-free. `Fetcher.Fetch` returns an `Iterator` of `*Batch{ID, Series, Timestamps, Values,
Columns}`: metrics populate `Values`, logs populate the named `Columns` (`Projection` narrows
them, `SecondPass` post-filters). The fields are zero-valued for the other signal, so the metric
path is unchanged by the log additions. `SliceIterator` and `Drain` are
the in-memory helpers. **`Merge(fetchers...)`** is the fan-out combinator: it runs a Request
against several fetchers and merges their batches by series id (timestamp-ordered, later child
wins a duplicate timestamp) — the basis for multi-tenant / cross-tenant reads (via
`Storage.Fetcher`) and, later, cluster fan-out across replicas. A single child is a
pass-through; child batches are cloned, never mutated. **`MergeBatches(groups...)`** is the
batch-level form (same series-id union + timestamp dedup) for callers that already hold drained
results from differing requests — the basis for the split-by-interval combinator below.

### Fetch scale-out decorators (`query/scale/`)

Query scale-out (the L5 "split + cache" a query frontend does) is the embedder's engine concern,
but the part expressible **purely over the fetch contract** ships here as two `Fetcher → Fetcher`
decorators, so any embedder engine composes them without the library owning a query language:

- **`SplitFetcher{Inner, Interval}`** splits a `Request`'s window into sub-windows **aligned to
  multiples of Interval**, fetches them concurrently against `Inner`, and `MergeBatches`-es the
  results. Grid alignment (not request-relative) makes a sub-window's bounds independent of the
  overall range, so overlapping queries share sub-windows — the property the cache below exploits.
  A narrow window (or `Interval ≤ 0`) is a transparent pass-through, so splitting never burdens a
  small query.
- **`CacheFetcher{Inner, Cache, Freshness, Now}`** memoizes results of **fully-pushable** requests
  only — every matcher must carry a serializable equality `Spec`, so the key (tenant ‖ window ‖
  sorted specs) is exact and a hit can never drop a matching series. A request with an opaque
  (non-equality) matcher, or a nil cache, bypasses to `Inner`. The cache does not auto-invalidate,
  so a **`Freshness`** guard keeps the recent window uncached: a request whose `End` is within
  `Freshness` of now (`Now`, injectable for tests; defaults to `time.Now`) bypasses to `Inner`.
  `Cache` is an interface; `MemoryCache` is a bounded-LRU implementation that stores a deep
  snapshot on `Put` (independent of producer buffer reuse) and returns a fresh slice on `Get` (so a
  caller's reslicing never disturbs cached order).

They nest: `SplitFetcher` over `CacheFetcher` caches each aligned sub-window independently, so a
shifted re-query reuses the sub-windows it overlaps — and with `Freshness` set, the settled
sub-windows cache while the most recent one is always re-fetched (the standard query-frontend
behavior). Both sit above the seam — no language, no engine, no query-result type — keeping the
library boundary at L4.

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

## 3i. Cluster ring (`cluster/ring/`) — L0 sharding primitive

The first piece of the (optional) distribution layer: **rendezvous / highest-random-weight
(HRW) hashing**. A node's score for a key is `xxh3.HashSeed(key, seed(nodeID))`; `Lookup(key,
rf)` returns the `rf` owning nodes (primary first, replicas after), ties broken by ID. Three
properties make it the sharding base:

- **Deterministic, coordinator-free placement** — every node computes the same owners from
  just the membership list, so routing needs no lookup table on the hot path.
- **Minimal movement on membership change** — adding a node only ever steals a key's replica
  slot *to itself* (existing pairings never reshuffle); removing one only redistributes *its*
  keys. A property test pins this: per key, at most one replica moves on an add, and the new
  node receives exactly its `~1/(N+1)` fair share of slots.
- **Zone-aware replica spreading** — `Lookup` selection is zone-aware: walking nodes in score
  order it takes the highest-scoring node of each not-yet-used **zone** (`Node.Zone`, the failure
  domain) first, so a key's replicas land in as many distinct zones as possible, and only fills
  the remaining slots in pure score order once distinct zones run out (fewer zones than `rf`).
  The primary is still the single highest-scoring node. When every zone is empty (the default) the
  result is exactly the score-ordered top-`rf` — pure HRW, no behavior change — so zone-awareness
  costs nothing until an operator sets zones (`Config.Self.Zone`, plumbed membership→ring). A
  property test pins the spread and the empty-zone equivalence.

The `Ring` is immutable (`With`/`Without` return a new ring).

**Membership** (`cluster/etcd`) makes the ring *live*. `Join` registers a node under
`{root}/members/{id}` with an etcd **lease**, keeps the lease alive, and **watches** the
member prefix; each change rebuilds the ring and publishes it via an `atomic.Pointer`, so
`Membership.Ring()` is a lock-free read of the current membership. A crashed node's lease
expires and it drops out of every peer's ring within the TTL — no manual deregistration;
`Close` revokes the lease for prompt, clean departure. etcd only distributes membership;
placement stays local and coordinator-free. Tested against an **embedded etcd** (join → watch
propagation → lease-revoke departure).

**Replication** (`cluster/replica`) protects the unflushed head: `Replicate` fans an opaque
write payload out to a key's ring-owners and returns as soon as a **quorum** —
`(len/2)+1` — has applied it (the local owner in process, the rest over the transport),
returning early with an error once quorum is unreachable; non-quorum owners still receive the
write so all replicas converge. `ReplicateQuorum` is the same with an explicit ack count, for a
caller that has already applied locally and needs only `RF/2` more acks from secondaries (the
primary-write path below); a quorum ≤ 0 fans out best-effort and waits for none. **The storage
library owns the node-to-node transport** (a deliberate departure from "the embedder owns
transport"): `cluster/replica` ships an HTTP `Transport` + receiving `Handler` (mounted at
`ReplicatePath`), tested over `httptest`. The replicator is decoupled from the ring — the caller
maps owners→addresses — so the routing and quorum logic test against a fake transport.

**Rebalance** (`cluster/rebalance`) computes the minimal ownership change for a membership
change as a pure function of the old and new rings: `Plan(shards, prev, next, rf)` returns, per
shard whose owner set changed, the IDs added and removed. Because the data is in the shared
object store, a reassignment is an **ownership handoff** (the gainer starts serving the shard's
parts from S3 via the bucket index; the loser stops), not a copy — and HRW guarantees only the
~1/N shards that actually moved appear, each one-in/one-out. The etcd-coordinated handoff that
makes exactly one node compact a shard at a time **is** built (the compaction-claim executor,
§3j), but it reconciles ownership **directly from the ring** — so `Plan` is a pure diff that the
executor does **not** yet consume; the minimal-move diff is currently informational only.

**The cluster write path is primary-authoritative.** A write is framed as `EncodeWrite`
(tenant ‖ WAL-encoded series+samples) and routed to the tenant's **ring-primary** — the single
authority for the shard. The primary applies it via `engine.ApplyPrimary`, which OOO-checks each
sample (the *only* OOO decision for the shard), re-frames the **accepted** set into a fresh WAL
payload, and returns that payload plus the **rejected count**. The primary then replicates the
accepted payload to the secondary owners (it already holds one durable copy, so it needs `RF/2`
more acks via `ReplicateQuorum`); a secondary applies it verbatim through `engine.ApplyReplicated`
(no re-check — the primary already decided, the way WAL replay trusts the log). Because every
replica receives the *same accepted set* from one authority, the replicas converge even under
concurrent writers, and the rejected count is exact.

**Per-series sharding (sharded-tenant)** spreads a single tenant's metric series across the ring so
one large tenant is not pinned to a single owner set. `Config.ShardsPerTenant` (default 1) splits a
tenant into N shards; a series maps to `shard = hash(seriesID) % N` and the **shard** — not the
tenant — is the ring/storage/compaction unit. A shard's routing/storage key is `{tenant}/_s{idx}`,
which **collapses to the bare tenant at N=1**, so the default layout, placement, and on-disk prefixes
are byte-identical to the unsharded path (and the shard key is just a tenant-like string the existing
tenant-keyed machinery — engine map, `{key}/metrics` prefix, ring lookup, compaction claims, and
stateless `recover` — handles transparently). `WriteMetrics` groups each point by its shard key and
routes each group to that shard's `Ring().Primary(shardKey)` (so different series in one batch scatter
to different primaries, each replicating to its shard's owners); the read seam (`clusterFetcherFor`)
**gathers across all N shards** — serving a shard locally when this node owns it, else fanning out to
an owner — and merges, so any node answers a full query. Compaction stays one-owner-per-shard
(`metricMergeOptions` resolves the tenant's retention/downsampling policy from the shard key via
`tenantOfShard`). Sharding applies to **metrics only**; the record signals (logs/traces/profiles)
remain a single shard (`Ring().Primary(tenant)`).

**Facade cluster mode** (`cluster.go`, `Options.Cluster`): when configured, `Storage.Open`
joins the etcd cluster, runs the HTTP server on the node's address (mounting the replicate,
primary-write, and read endpoints), and builds the routed write path. `WriteMetrics` frames each
tenant's projected series+samples and calls `routeToPrimary` — applied in process if this node is
the primary, else forwarded to the primary's `primaryWritePath` over HTTP — and the primary's
reject count flows back into the returned `Accepted{Accepted, Rejected}`, so clustered ingest
reports the same partial-success accounting as the single-node path. `Close` revokes the lease and
stops the server. A **two-node end-to-end test** (shared embedded etcd, two `Storage` instances)
confirms a write to one node is served by both; a single-node test confirms an out-of-order sample
surfaces in `Accepted.Rejected`.

**Read fan-out** (`cluster/read.go`): `Storage.Fetcher` is owner-aware in cluster mode. If the
node owns the tenant it serves locally (the head is replicated there, full matcher pushdown);
otherwise it fans out over HTTP to an owner's read endpoint and **fails over** between owners (a
single owner's copy is complete). Matchers are opaque Go predicates and **not serializable**, so
the RPC carries only the tenant + window — a peer returns its whole window (a superset the fetch
contract permits) and the requesting node **re-applies the matchers** (their closures live
there). A three-node, RF=2 end-to-end test confirms a query on a non-owner returns the data.

**Rebalance executor** (`cluster/etcd/ownership.go`): the handoff is enacted via **exclusive
compaction claims derived directly from the ring** — `Reconcile` does **not** consume
`rebalance.Plan` (that minimal-move diff is built but currently unwired; §3i). `Ownership.Acquire`
is an etcd CAS (create-if-absent) bound to the node's membership lease; `Reconcile(ring, shards)`
acquires every shard the node is the ring-primary of and releases the rest, returning the owned set. In
cluster mode the maintenance loop flushes/merges **only owned tenants**, so a tenant's parts are
written to the shared object store by exactly one node — even during ring-disagreement windows,
the claim arbitrates. A departed node's claims auto-free with its lease and the new primary
takes over (tested via lease revoke). A two-node test confirms only the primary compacts.

**Matcher pushdown.** Most matchers are opaque closures, but **equality** is exact and
serializable, so `fetch.Matcher` carries an optional `Spec *EqualMatcher` (the PromQL adapter
sets it for `=` matchers). The read RPC forwards the equality specs; the peer reconstructs them
(`EqualMatcher.Predicate`) and pushes them into its local fetch — so a non-owner read narrows by
`__name__="metric"` on the owner instead of pulling the whole window. Non-equality matchers are
not forwarded; the requester's re-check still applies them.

Closed parity items: the write path is primary-authoritative, so the OOO decision is made once
by the shard primary (`engine.ApplyPrimary`) and secondaries apply the accepted set verbatim
(`engine.ApplyReplicated`); replica heads are trimmed to the unflushed window after the owner
flushes (`Engine.RefreshReplica`, over a shared store); per-point **partial-success accounting**
now propagates from the primary back to the origin (the reject count flows into `Accepted`); and
equality matcher pushdown (above).

---

## 3j. Record signals — logs, traces & profiles (`recordengine`, `signal/log`, `signal/trace`, `signal/profile`, `index/bloom`)

Logs, traces, and profiles are **record-shaped** signals: a stream (a Resource+Scope identity,
indexed by the postings index exactly like a metric series) of rows that each carry a primary
timestamp plus a fixed set of typed columns. Unlike a metric's `(ts, float)` sample, a record's
per-row fields vary *within* the stream, so they are **columns filtered by predicate**, not identity
— the dual-shape fetch contract (§3g): **Matchers resolve the stream, Conditions filter its
records.** All three share one engine; only the column schema, projection, and (for profiles) an
optional side store differ.

- **Shared engine** (`recordengine`) is the metrics engine's structural twin, generalized over a
  `Schema` of `Column{Name, Kind(Int64|Bytes), Codec, Bloom(None|FullText|Attrs|Equality)}` (the
  timestamp sort key and the int128 stream id are implicit). A signal supplies the schema and
  projects its model into the engine's column vectors (`recordengine.Batch`); the engine treats the
  columns opaquely. It owns the head (per-stream column buffers + the `symbols`+`series`+`postings`
  stream-label index), flush to an immutable columnar part sorted by `(stream, ts)`, the durable
  bucket-index + `streams.bin` stateless-read path, append-only merge with retention, per-column
  blooms, and `Fetch` implementing `fetch.Fetcher`. `Fetch` is heavily tuned: **lazy column decode**
  (materialize only the columns a request's conditions + projection reference — a body search
  projecting body touches just `ts`+`body`), decode each surviving part **once** distributing rows
  to per-stream accumulators, pre-size accumulators from row-range counts, bulk-append in-window
  ranges, filter **in place**, and skip the sort when already ts-ordered. Flush/merge materialize
  the full schema. Conditions over a non-fixed column are per-record **attributes**, resolved by the
  zero-allocation `signal.LookupAttribute` over the serialized `attrs` column.
- **Per-column blooms** (`index/bloom`): a token bloom (bit array, k xxh3-128 double-hash probes,
  versioned+CRC'd, **no false negatives**) per bloom-bearing column, written as `bloom-{col}.bin`.
  `FullText` columns tokenize their value (a `contains` token prunes); `Attrs` columns hold
  **key-scoped** `key‖value` (equality) and `key‖word` (contains) tokens per record attribute;
  `Equality` columns hold each value verbatim (the **trace-by-id** path). `Fetch` skips a part whose
  bloom proves a required `Condition.Tokens`/`Condition.Equal` absent, then re-checks per row.
- **WAL** carries a signal-agnostic records frame (`wal.WriteRecords`/`OnRecords`) of an opaque,
  engine-encoded payload, plus an optional **side frame** (`wal.WriteSide`/`OnSide`) carrying the
  side-store delta; `recordengine` owns the rec codec and `EncodeWAL` (the cluster write form, which
  appends the side frame so the symbol store replicates).
- **Side store** (`recordengine.SideStore`, optional; nil for logs/traces) — a content-addressed
  auxiliary store a signal attaches to each batch (`Batch.Side`) that rides the part lifecycle: the
  engine absorbs each batch's delta into a live accumulator, writes the accumulated tables as part
  sidecars on flush (`{prefix}/sym-{name}.bin`, mirroring the bloom sidecars), and unions the
  compacted parts' sidecars on merge — under the one merge engine, no parallel subsystem.
  Content-addressing (an entry's id is a hash of its content) makes the union a plain dedup with no
  id remap. Profiles is the first user (its symbol store).
- **Logs** (`signal/log`): schema = `observed`/`severity`/`flags`/`dropped`(int) +
  `severity_text`/`body`(FullText)/`trace_id`/`span_id`/`attrs`(Attrs)(bytes). `WriteLogs` /
  `LogFetcher`.
- **Traces** (`signal/trace`): a span is a record. Schema = `duration`/`kind`/`status_code` +
  ingest-computed nested-set ids `parent_id`/`nested_set_left`/`nested_set_right` (int) +
  `trace_id`(Equality)/`span_id`/`parent_span_id`/`name`(FullText)/`status_message`/`attrs`(Attrs) +
  serialized `events`/`links` (bytes). `span_id` is near-unique, so it uses the dictionary-free
  fixed-width `CodecBytesRaw` (≈30% smaller than a dictionary for that column); `trace_id` keeps the
  dictionary codec — it repeats once per span of a trace, which the dictionary exploits.
  `Project` computes nested-set ids per trace within the batch
  (group by trace id across services, build the parent→child tree, preorder-DFS assign left/right/
  parent), so an embedder's TraceQL does ancestor/descendant/sibling as range comparisons on the
  returned columns — **no `SeekTo`**; a cross-batch parent is treated as a root (the raw
  `parent_span_id` is always present to reconcile). `WriteTraces` / `TraceFetcher`, plus
  **`Trace(tenant, id)`** — trace-by-id as an equality condition on `trace_id`, pruned by its
  equality bloom, returning the trace's spans across services. Events/links round-trip via
  `trace.DecodeEvents`/`DecodeLinks`.
- **Profiles** (`signal/profile`): a profile is a pprof-style graph (samples → stacks → locations →
  functions, all index-based) with a large shared symbol dictionary — Pyroscope's **two-table
  split**: a columnar sample table + a deduplicated symbol store. Each `Sample` flattens to one
  record **row** (a sample with `timestamps_unix_nano` explodes to one row per (timestamp, value); an
  aggregated sample is one row at the profile time); schema = `value`/`period`/`duration`(int) +
  `stack_id`/`profile_id`(Equality)/`trace_id`/`span_id`/`attrs`(Attrs)(bytes). The **profile type is
  folded into the stream identity** as reserved `otel.profile.*` labels (sample/period type+unit) —
  like a metric's `__name__` — so a query selects a type with an ordinary label matcher and the
  available types enumerate through the postings index (rather than a per-sample column). `stack_id`
  is a **content-addressed id** into the symbol store, computed Merkle-style bottom-up
  (string→function→location→stack), so the same stack has the same id everywhere. The symbol store
  (strings, mappings, functions, locations, stacks) is built per-stream at projection time, attached
  to `Batch.Side`, and persisted/merged as part sidecars via the side-store hook above.
- **Profiles query surface** (`WriteProfiles` / `ProfileFetcher` plus the read primitives that make
  an embedder's Pyroscope/ProfileQL `Querier` buildable):
  - **Sample search** — matchers resolve streams (incl. the profile type), conditions filter samples;
    returned rows carry `value` + the global `stack_id`.
  - **`ProfileResolver(tenant)`** → a `profile.Resolver` over the tenant's unioned symbol store
    (`recordengine.Engine.SideSnapshot` merges the head accumulator with every part's sidecars);
    `Resolve(stack_id) → []Frame{Function, File, Line}` (leaf-first, bounds-checked). This is what
    turns a sample search into a symbol-resolved **flamegraph** (`SelectMergeProfile`).
  - **`ProfileSeries(tenant, matchers, window)`** → the matching stream identities
    (`recordengine.Engine.Series`), from which an embedder derives **ProfileTypes** (distinct
    `otel.profile.*` tuples), **LabelNames**, and **LabelValues**.

  All three fan out in cluster mode (a non-owner serves them from an owner — see below).
- **Cluster**: one signal-discriminated path serves all four signals. The write envelope
  (`cluster.EncodeWrite`) and read request (`cluster.EncodeFetchRequest`) carry a `signal.Signal`
  byte; the primary-write, replicate, and read handlers dispatch to the metric / log / trace /
  profile engine. Record writes are **primary-authoritative**
  (`recordengine.ApplyPrimary`/`ApplyReplicated`) with accurate `Accepted` accounting; read fan-out
  ships batches via the column-aware codec (`EncodeLogBatches`, shared by the record signals) — a
  fan-out matcher re-filters against the full series label set (resource + scope, not just
  data-point attributes). Compaction ownership is per-tenant, reconciled once across all signals. The
  facade's `WriteLogs`/`WriteTraces`/`WriteProfiles` and `LogFetcher`/`TraceFetcher`/`ProfileFetcher`
  share generic helpers (`writeRecordsLocal`/`writeRecordsClustered`/`recordFetcher`). The profiles
  **symbol store is replicated** with the write: `EncodeWAL` appends a `wal.recordSide` frame and
  `ApplyPrimary`/`ApplyReplicated` absorb it (and the primary forwards it), so every owner holds the
  symbols and flushes them. Profiles' **enumeration and resolution also fan out**: a non-owner serves
  `ProfileSeries` (over `cluster.SeriesPath`, re-applying non-equality matchers to the superset) and
  `ProfileResolver` (over `cluster.SidePath`, fetching the owner's symbol tables) from an owner,
  failing over between owners. Decoders for both peer responses are bounds-checked and fuzzed.

---

## 3k. Admission control & backpressure (`admission.go`, `engine/admission.go`)

Overload protection so a load spike **degrades rather than OOMs**. It is **lossless** (parts are
always exact) and enforces the per-tenant `tenant.Limits`, resolved through the policy callback so
changes hot-reload on the next write. Anything shed is reported back via OTLP partial-success
(`Accepted.Rejected` + `RejectedReason`), never silently dropped or queued unbounded. The admission
stage sits **between tenant resolution and the engine** in `WriteMetrics`, so a shed point costs no
index/head/WAL/flush work; the engine core stays policy-agnostic (it sees only numbers via
`engine.AppendLimits`, never a tenant or policy). Three valves:

- **Ingest rate** (`Limits.IngestBytesPerSecond`) — a per-tenant **token bucket** (`tokenBucket`,
  burst = one second of budget) in the facade; an over-budget batch is shed up front. The clock is
  injected (`Storage.now`) for deterministic tests.
- **Cardinality** (`Limits.MaxSeries`) — enforced **in the head** (`head.appendByID`, race-free under
  the engine lock): a sample that would mint a *new* series past the cap is shed; samples for
  already-known series are never blocked, so a query keeps returning what is already admitted. (No
  hysteresis or `__overflow__`-series routing yet — those are §8a refinements.)
- **In-flight memory** (`Limits.MaxInFlightBytes`) — also head-enforced, against the head's buffered
  **byte measure** (`engine.SampleBytes` = 16 per buffered sample, reset on flush, recomputed on
  replica trim): samples arriving while at the cap are shed until a flush drains the head. This is the
  bounded-memory valve; pair it with `FlushInterval` (or a flush threshold) so the head drains and the
  valve reopens.

`engine.AppendBatch` returns an `AppendResult` breaking accepted/rejected down by reason
(`RejectedOOO`/`RejectedCardinality`/`RejectedBytes`), which the facade folds — together with rate
rejections — into the `Accepted` reply and into per-tenant **meta-metrics** (`AdmissionStats`,
exposed by `Storage.AdmissionStats(tenant)`: accepted plus rejected-by-reason plus
`SampledDropped`), so an operator can see which valve tripped.

**Budgeted (lossy) sampling — the StatsHouse-style unbiased path** (`Limits` is the lossless floor;
this is the lossy ceiling). Under `Sampling.MaxRowsPerSecond`, when a tenant exceeds the budget the
facade **keeps a representative subset rather than rejecting** and tags each kept sample with a
**scale factor** so an embedder's count/sum/rate stays unbiased. The sampler (`tenantAdmission.sample`)
is **deterministic by (series, ts)** — the same point is consistently kept or dropped — and the
scale factor **adapts each 1-second window** from the prior window's observed rate, so the kept rate
tracks the budget. Sampled-out points count as **accepted** (the producer must not retry — a kept
peer's weight represents them) and are tracked as `SampledDropped`. The scale factor rides the whole
metric path: a 4th **`sf` part column** (`engine`, written only when sampling occurred, so an
unsampled part keeps its original three-column layout and a missing column reads as weight 1),
carried through flush, the one merge engine (compaction **and** downsampling are weight-aware — Sum
emits Σ(value·sf), Count emits Σsf, the rest carry the representative's weight), and surfaced on the
read seam as **`fetch.Batch.ScaleFactors`** (nil when unsampled;
`Batch.ScaleFactor(i)` reads it with the default-1). The library only *carries* the weight; honoring
it in aggregation is the embedder's job (a gauge read ignores it).

Scope today: **metrics, single-node** ingest. Not yet built (the rest of §8a): the **cardinality
budget with hysteresis / `__overflow__` routing** (the cap is a hard reject, not yet a soft budget),
the clustered **central→edge budget feedback**, **WAL persistence of the scale factor** (so a crash
recovers unflushed *sampled* data as weight 1 — a narrow window), and admission on the **record
signals** and the **clustered write path** (the primary applies only the OOO check today).
`MaxPartSize` is also not yet enforced.

---

## 4. Public surface (`storage` root package)

The embedder-facing API. **All four signals' ingest+read paths are wired and working** (metrics on
the float-sample engine; logs, traces, profiles on the shared record engine); only the
query-language path stays in the embedder. The `Write*` methods take the library's internal,
`[]byte`-based ingest batches (`metric.Metrics`/`log.Logs`/`trace.Traces`/`profile.Profiles`),
**not pdata** — OTel-Go users convert via `otlp/pdataconv` (§3e).

- **`Storage`** — the facade. `Open(ctx, Options, ...Option)` and `InMemory(...Option)`
  construct it (validation, defaulting, tenant-resolver wiring, and — when `FlushInterval`
  is set — the background maintenance loop start here); `Close(ctx)` stops the loop and
  flushes every tenant engine. `WriteMetrics(ctx, md)` is **fully implemented**: it
  projects the internal `metric.Metrics` batch, derives each point's **tenant from its Resource+Scope**
  via the `Options.Tenant` callback (no tenant argument), and appends to that tenant's
  lazily-created `engine.Engine` (one per tenant, parts under `{tenant}/metrics`) through the
  `AppendBatch` fast path (one locked call per metric), caching the resolved engine across a
  tenant-contiguous run of metrics. Before the engine append it runs the **admission stage** (§3k):
  the per-tenant ingest-rate valve, then the engine's per-sample cardinality / in-flight-memory
  limits. It returns `Accepted` (OTLP partial-success: `Rejected` counts every shed point —
  out-of-order drops plus admission rejections — and `RejectedReason` names the principal valve;
  unsupported kinds and value-less points are filtered upstream by the producer). **`WriteLogs(ctx,
  ld)` is fully implemented** the same way over per-tenant
  `recordengine.Engine`s (parts under `{tenant}/logs`). **`WriteTraces(ctx, td)` is fully
  implemented** the same way over per-tenant `recordengine.Engine`s on the span schema (parts under
  `{tenant}/traces`). **`WriteProfiles(ctx, pd)` is fully implemented** over per-tenant
  `recordengine.Engine`s on the sample schema **with a `profile.SymbolStore` side store** (parts +
  symbol sidecars under `{tenant}/profiles`). A single
  **maintenance loop** periodically flushes +
  merges every metric, log, trace, and profile engine, applying per-tenant retention from the resolved policy. `Reset(ctx)`
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
  bridges to the Prometheus engine). When `WithQuerySplitInterval` / `WithQueryCache` are set,
  `Fetcher` wraps the result with the `query/scale` split / cache decorators (§3g): the cache is
  shared and **scoped by tenant set** (a `scopedFetcher` stamps the sorted tenant ids onto the
  request so cache keys never collide across scopes), so only explicit-tenant queries are cached
  — a no-arg cross-tenant query is never cached (its membership is dynamic). `WithQueryCacheFreshness`
  keeps the recent window uncached so freshly-ingested samples are never served stale.
  **`LogFetcher(tenants...)`** is the logs read seam, the same shape over the log engines
  (multi-tenant reads concatenate rather than timestamp-dedup, since log records are append-only
  and carry columns); a query supplies stream `Matchers` plus record `Conditions`.
  **`TraceFetcher(tenants...)`** is the identical seam over the trace engines, and
  **`Trace(ctx, tenant, traceID)`** is the trace-by-id convenience: it issues a fetch with an
  equality `Condition` on the `trace_id` column (pruned by that column's equality bloom) and returns
  all of the trace's spans across services. **`ProfileFetcher(tenants...)`** is the identical seam
  over the profile engines (matchers resolve streams — incl. the profile type via its reserved
  `otel.profile.*` labels — and conditions filter samples), complemented by
  **`ProfileResolver(ctx, tenant)`** (stack-id → frames, for flamegraphs) and
  **`ProfileSeries(ctx, tenant, matchers, window)`** (stream enumeration, for profile-type/label
  listing) — together enough to back an embedder's Pyroscope/ProfileQL querier.
- **`Options` / `Option`** (`options.go`) — config struct plus functional options
  (`WithBackend`, `WithCluster`, `WithTenancy`, `WithEncoding`, `WithDurability`,
  `WithWALDir`, `WithWALSync`, `WithWALSyncInterval`, `WithFlushThresholdBytes`, `WithFlushInterval`,
  `WithOOOWindow`, `WithQuerySplitInterval`, `WithQueryCache`, `WithQueryCacheFreshness`). `Durability`
  selects the durability mode; an ephemeral backend with no explicit choice defaults to the in-memory
  engine. `WithWALDir` (durable backends only) turns on per-tenant write-ahead logging so a crash
  recovers the unflushed head exactly-once (§3a/§3d); without it, recovery restores only flushed parts.
  `WithWALSync`/`WithWALSyncInterval` pick the fsync policy (page-cache / per-write / background).
- **`Query` / `Lang` / `Result` / `Accepted`** — the query request (language selected by
  `Lang`), its result, and the ingest acknowledgement type.

### Shared model types

- **`signal`** — signal-neutral model: the `Signal` enum (`Metric`/`Log`/`Trace`/
  `Profile`), `ParseSignal`, `TenantID`, the `Aggregation` enum (the shared rollup
  vocabulary for merge-time downsampling, see §3f), and the typed identity primitives
  (`Value`, `KeyValue`, `Attributes`, the 128-bit `SeriesID`, and the attribute binary
  codec) — see §3c.
- **`tenant`** — policy model: `Limits` (admission valves — §3k), `Retention`, `Downsample` (a list
  of `DownsampleTier{After, Interval, Agg}` rollup bands, applied at merge time — §3f), `Sampling`
  (the lossy budget — §3k), `Recompress` (cold-data zstd recompression at merge — §3f), and the
  composed `Policy`, resolved per tenant id through a `Resolver` (`ResolverFunc` adapter;
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
- **One physical engine, many front-ends.** Signals are thin layers over the shared columnar
  engine and the fetch contract: metrics on `engine`, and logs/traces/profiles on the schema-driven
  `recordengine` (each supplying only a column schema, a projection, and — profiles — a side store).
  Storage-layer code must not learn a language's or signal's concepts; query languages stay in the
  embedder above the seam.
- **Immutable, in-memory-first.** The in-memory/ephemeral path is first-class — every
  layer must work with no disk or object store. `backend.Memory()` is the reference
  backend.
- **Stable formats.** The `Codec` enum, the per-stream header, each codec's framing, the
  part formats (manifest `OTPM`, marks `OTMK`, per-column object framing, the
  `{prefix}/manifest|marks|c/{i}` key layout), the **attribute hash/binary encoding** (the
  SeriesID pre-image), the **symbol table** (`OTSY`), the **WAL record framing** (series, sample,
  records, and side frames), the **metric part column layout** (`[series:int128, ts:int64,
  value:float64]` sorted by `(series, ts)`, plus an **optional 4th `sf:float64`** scale-factor column
  present only when lossy sampling occurred — §3k), and the **profile symbol-store table** (`OTSP`, the
  content-addressed side-store sidecars) are all persisted/wire-stable. Changing any of
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
.                     storage facade: Storage, Open/InMemory, Options, per-tenant engines, maintenance loop [implemented: metrics+logs+traces+profiles ingest+read; query-lang in embedder]
  admission.go        per-tenant admission control: ingest-rate token bucket + AdmissionStats meta-metrics (§3k) [implemented: metrics, single-node]
encoding/             umbrella doc for the codec layers
  encoding/bitstream  MSB-first bit Writer/Reader                                      [implemented]
  encoding/chunk      DoD / Gorilla / T64 / dict / bytesraw / decimal / id128 column codecs [implemented]
  encoding/compress   zstd/none block wrapper (lz4 stub)                              [implemented]
pool/                 ByteIntMap (xxh3) for dict building                              [implemented]
signal/               typed Attributes/Value, Resource/Scope/Series identity, 128-bit SeriesID, Signal, TenantID, Aggregation [implemented]
  signal/metric       []byte-based OTLP-shaped Metrics ingest batch (resettable/pooled) + identity + projection (gauge/sum; histogram/exp-histogram/summary via classic decomposition in otlp/pdataconv) [implemented]
  signal/log          []byte-based OTLP-shaped Logs ingest batch (resettable/pooled) + stream identity + projection [implemented]
  signal/trace        []byte-based OTLP-shaped Traces ingest batch (resettable/pooled) + span schema + projection (nested-set, events/links) [implemented]
  signal/profile      []byte-based OTLP-shaped Profiles ingest batch + sample schema (type folded into identity) + projection + content-addressed symbol store (SideStore) + stack Resolver [implemented]
otlp/pdataconv        optional OTel-Go bridge: pmetric.Metrics → metric.Metrics; gauge/sum direct + histogram/exp-histogram/summary classic decomposition (only package importing pdata) [implemented]
tenant/               Limits/Retention/Downsample/Sampling/Recompress/Policy, Resolver             [implemented]
backend/              Backend interface (Read/Write/List/Delete/PutIfAbsent) + memory (root) [implemented]
  backend/file        directory-tree backend; atomic write + exclusive PutIfAbsent (os.Link) [implemented]
  backend/s3          object-store-native backend over ObjectStore + aws-sdk-go-v2 adapter   [implemented; in-process go-faster/fs S3 integration test]
  backend/bucketindex versioned block-list index (time-pruned part enumeration, no full LIST) + WAL flush-epoch watermark [implemented]
block/                immutable columnar part format: column/marks/manifest/part        [implemented]
index/                symbols (intern) · series (id↔attrs) · postings (set-ops/matchers) [implemented]
  index/bloom         token bloom filter (no false negatives) + tokenizer: full-text + attr/equality pruning [implemented]
wal/                  CRC-framed segmented WAL: samples + opaque records + side delta, resume + checkpoint (truncate-on-flush), facade-wired durability [implemented]
engine/               head · flush · background-merge · retention · downsampling · recompression · admission limits · fetch (metrics) [implemented]
recordengine/         shared schema-driven record engine (logs+traces+profiles): head · flush · merge · fetch · conditions · per-column blooms · optional content-addressed side store [implemented]
query/fetch           dual-shape fetch contract (Matchers + Conditions/Projection/SecondPass) [implemented for metrics + logs + traces + profiles; the library's query surface]
query/scale           fetch-seam scale-out decorators: split-by-interval + results cache  [implemented]
query/promql          OPTIONAL adapter: fetch → Prometheus storage.Queryable (no engine) [implemented; only package importing prometheus]
cluster/              L0 distribution: ring + membership + (later) replication · rebalance [partly implemented]
  cluster/ring        rendezvous (HRW) hashing: deterministic placement, ~1/N movement, zone-aware replica spread [implemented]
  cluster/etcd        etcd-backed live membership (lease + watch → atomic ring)          [implemented; embedded-etcd tested]
  cluster/replica     quorum write-replication + node-to-node HTTP transport             [implemented]
  cluster/rebalance   minimal ownership-handoff plan from a ring diff (pure)              [implemented]
  cluster/etcd (ownership.go) exclusive compaction claims (CAS+lease) — the rebalance executor [implemented]
  cluster/            cluster write path: EncodeWrite codec + Writer + Config             [implemented]
  cluster/ (read.go)  cluster read RPC: window-fetch codec + ReadHandler + RemoteFetcher    [implemented]
  cluster/ (enum.go)  cluster enumeration/resolution RPC: series-list + side-store fan-out [implemented]
.                     (cluster.go) facade cluster mode: routed WriteMetrics + owner-aware fan-out reads + per-series sharding (sharded-tenant, metrics) [implemented]
```

"Seam only" packages currently contain their `doc.go` (and, where noted, an interface or
config type) that fixes the boundary; they have no behavior yet. As each is implemented,
move its row to "implemented" here and add a section above.
