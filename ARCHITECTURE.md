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

**Adaptive float codec (lossless and lossy).** The value column is not pinned to one codec: the part
writer (`block.Column.AutoCodec`) trial-encodes both float codecs and keeps the **smaller**, recording
the winner in the column descriptor so the reader dispatches correctly. In the **lossless** regime
(`FloatPrecisionBits == 0`) the scaled-decimal codec is taken only when it is smaller *and* a
verification decode reproduces the values (numeric equality, so NaN/±Inf and any precision loss keep
Gorilla) — this is what makes an integer-valued counter or a clean low-precision gauge land near a byte
per point while a high-entropy gauge stays on Gorilla, the lossless floor. The scaled-decimal codec
itself rounds (not truncates) to its base-10 scale and decodes by dividing by an exact power of ten, so
clean decimals round-trip bit-for-bit (a spurious `-0` may surface as `+0`, numerically identical for a
metric). In the **lossy** regime (`FloatPrecisionBits` in 1..63, set per age tier by the merge engine —
§3f) the decimal codec retains only that many significant mantissa bits but still competes against
lossless Gorilla, so a lossy tier is never worse than lossless. The budget is persisted in the manifest
(below) as the merge fixed point. Note the lossy error is in the **delta** domain, so it accumulates
mildly along a long series — a property of nearest-delta compression.

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
- **`backend.Cached(inner, maxBytes)`** (root package) — the object-store **read cache**: a
  byte-bounded LRU over read objects that wraps any backend, targeting the cold tier (file/S3) where
  a part column is otherwise re-read over the network on every query. Correct by construction —
  part objects are write-once immutable, so a cached value is never stale; the only invalidation is
  eviction, and a `Write`/`Delete` of the same key (manifest/index objects) updates/drops the entry.
  It preserves the copy semantics (stored/returned slices are private), passes `List`/`PutIfAbsent`
  through, and exposes hit/miss `Stats`. For hot read paths the copy is optional: **`backend.Viewer`**
  is an opt-in capability (`ReadView(ctx, key)`) returning the value as a **read-only view** — a hit
  hands out the resident entry's slice itself (a stored value is never mutated in place, so a view
  stays valid across overwrite/eviction), removing the clone-per-hit that dominated the query-path
  allocation profile. The memory backend and the metering wrapper implement/forward it, the
  `backend.ReadView` helper falls back to a plain `Read` over any other backend, and `block.PartReader`
  reads manifest/column/marks objects through it. Enabled via `Options.ReadCacheBytes` / `WithReadCache`,
  wrapped **outermost** (a hit skips both metering and the backend) and skipped for an ephemeral
  backend. A hit removes the per-fetch backend-read latency entirely, so the win scales with object-
  store latency × objects-per-query.
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
- **Block-framed columns** (`blockcolumn.go`, opt-in via `Column.Block`) split a per-row sequential
  column (DoD/T64 int64, Gorilla/decimal float64 — the metric ts/value/sf columns) into granule-sized
  row blocks, each an **independently decodable** codec stream (the codecs reset their running state at
  every block's row 0). The object becomes `[uvarint nBlocks][uvarint blockRows][per-block uvarint
  len][block streams…]`, each stream block-compressed on its own; a blocked column is flagged by
  `flagBlocked` in its descriptor (additive, no version bump). `ColumnReader.Int64`/`Float64` decode
  every block in place (no whole-column re-read), and `RangeInt64`/`RangeFloat64` decode **only the
  blocks spanning a row range** — the sub-part seek primitive (decode a fraction of a column for a
  selector touching a fraction of its rows). `DecodeBlocksInt64`/`DecodeBlocksFloat64` decode a chosen
  *set* of blocks into their row spans of a full-length slice — the engine's series-skip primitive.
  Block boundaries align with the marks granules, so the marks index already carries each block's
  `[minTime,maxTime]`. The streaming merge cursors (`TsCursor`/`FloatCursor`) decode block-by-block and
  span boundaries transparently (each block's row 0 is absolute), so the merge reads blocked parts
  unchanged. Unblocked columns keep the prior single-stream layout byte-for-byte. **Metric parts are
  blocked by default** (`engine.Config.MetricBlockRows`, default 1024 rows): the ts/value/sf columns
  are blocked and the block size drives the part's marks granules.
- **Manifest** (`manifest.go`) is a versioned binary record (magic `OTPM`, version, row
  count, time range, granule size, per-column descriptors) with a trailing CRC32C. Each column
  descriptor is `[name][kind][codec][compress][flags]` then, **only when `flagLossy` is set**, one
  `FloatPrecisionBits` byte (the lossy precision budget — §2.2/§3f), then the per-kind stats/const.
  The precision byte is flag-gated, so lossless columns and pre-existing parts keep their exact
  byte-for-byte layout (no version bump, no golden churn). `flagBlocked` (additive, same way) marks a
  block-framed column object (above). Decode bounds-checks every field and never panics (fuzzed).
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
- **`index/series`** — `SeriesID ↔ Series`. `Add` is idempotent (id is the identity hash) and
  **interns** the identity's key/value bytes through a per-index `index/symbols` table (via
  `signal.Series.Intern`), so each distinct label/attribute string is owned once and referenced by
  every series that shares it. It also **deduplicates whole `Resource` and `Scope` sets** by content
  (content-keyed caches), so series sharing a resource/scope — node_exporter has a handful of
  resources and one scope across ~all series — point at one owned `[]KeyValue` rather than a private
  per-series clone of the *structure* that byte interning alone leaves behind (≈23 % less resident
  identity storage on the metrics shape). Point attributes are near-unique per series, so they are
  byte-interned only (a per-set cache there would store keys it could not dedup). A query reconstructs
  labels from an id and replay is dedup-safe.
- **`index/postings`** — the inverted index, keyed on **interned symbol ids** (`nameID →
  valueID → sorted []SeriesID`), so it is zero-alloc and **type-preserving** (the value id
  comes from the value's typed encoding). Lazy set-op iterators (`Intersect`/`Merge`/
  `Without` with galloping `Seek`, property-tested vs a naive reference) compose lists; `Merge` is a
  binary-min-heap k-way union (O(N·log k), not O(N·k)), so a high-cardinality matcher resolving across
  thousands of value buckets stays fast.
  Matching is **callback-based**: `Select(nameID, func(valueID) bool)` hands the predicate
  a candidate value id, which the caller decodes to a typed `signal.Value` and tests —
  storage imports no query-language operator; negation/equality compose from the
  primitives (`Get`/`Without`/`WithoutName`).

## 3d. Write-ahead log (`wal/`)

CRC-framed records (`[uvarint len][type][payload][CRC32C]`) appended to numbered segment
files (`SegmentWriter`, rotating at a size limit). Record types: a **series** record (`SeriesID`
+ typed attribute encoding), a **samples** record (metrics), a **scale-factor samples** record
(metrics that carry per-sample lossy-sampling weights, written only when sampling occurred — see
§3k), an opaque **records** payload (the record signals), and an opaque **side** record (a
content-addressed side-store delta — the profiles symbol store). `ReplayDir` stitches segments in
order; replay tolerates a torn final record (crash
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

**The engine lock is never held across object-store I/O.** Flush, merge, and fetch are phased so
their large reads/writes run lock-free, exploiting that parts are immutable. Both engines (`engine`,
`recordengine`) share this discipline:
- The **`parts` slice is copy-on-write** (`appendPart`/`replaceParts`), so a reader that snapshots the
  header under the lock keeps a stable backing array after releasing it.
- **Fetch** *plans* under the read lock — resolve matchers, snapshot+`acquire()` the in-window parts,
  seed accumulators from the head — then releases the lock and reads the parts' columns lock-free
  (`planFetch`/`readParts`), `release()`-ing the parts after.
- **Flush/merge** *plan* under the lock (drain/snapshot the head, reserve the part sequence), *build*
  the part off the lock (compaction reads, part write, read-back, sidecar union), then *publish* under
  the lock (swap the parts slice, persist the **small** bucket/stream index + WAL checkpoint — kept
  under the lock so the swap and the durable-watermark commit stay atomic, preserving exactly-once
  crash-consistency). Only the background maintenance task (or `Close`) mutates `parts`, so the swap is
  single-writer.
- **Parts retired** by flush/merge are not deleted inline: each `part` has an `atomic.Int32` refcount,
  and `reclaimRetired` deletes a retired part's objects only once its in-flight readers have drained —
  so a lock-free fetch never races a delete (deferred reclamation).
- A flush **detaches** the head's buffers into an engine-level `flushing` set that fetches still read,
  swapped for the new part atomically at publish — so a fetch sees the records in exactly one of
  `flushing`/the part, never neither (no visibility gap) nor both (no double count).

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
  inside) remains for callers that already hold an identity. When durable, a batch's WAL frames
  are **grouped by series** (a reusable `walBatch` scratch — zero steady-state allocation): one
  `WriteSamples` frame per series, one `WriteSeries` per new series, written once after the run —
  not a write+fsync syscall per sample.
- **Flush** (`flush.go`) drains the head's buffered samples into one **flat 3-column part**
  `[series:int128, ts:int64, value:float64]`, one row per sample, sorted by `(series, ts)`,
  written via `block.PartWriter` under `{tenant}/metrics/{seq}`. After the part is written,
  flush updates the two durable index objects (§3a): the **bucket index**
  (`{prefix}/bucket-index.bin` — part list + per-part time bounds) and the **identity index**
  (`{prefix}/series.bin` — every series' reversible labels). Merge updates both too,
  committing the new part set to the bucket index *before* deleting the source parts.
  Flush and merge are driven by the facade's single **background maintenance loop**
  (`storage.runMaintenance`), on the `Options.FlushInterval` cadence. A **durable** store always runs
  it: a zero `FlushInterval` resolves to `defaultFlushInterval` (10 s), because without a running loop
  a non-ephemeral head and WAL grow unbounded in RAM until OOM — only a *negative* interval (an
  explicit opt-out) or the ephemeral in-memory engine disables it.
- **Stateless reconstruction** (`index.go`) — `Engine.LoadParts` rebuilds a fresh engine's
  durable state from the backend alone: the part set from the bucket index, and the
  postings/series index from the identity object (so matchers resolve and batches carry real
  labels — parts store only ids). This is the object-store-native read path: any node serves a
  tenant's flushed data without the (local) WAL. WAL `Replay` is complementary, restoring only
  the *unflushed* head.
- **Part fetch** (`part.go`, `sidx.go`) — every flushed/merged part carries a **series-index
  sidecar** (`{prefix}/sidx`): the sorted distinct `SeriesID`s with their run-start rows as
  fixed-width 20-byte entries (magic/version header, CRC32C tail), so a lookup **binary-searches
  the raw sidecar bytes in place**. `openPart` validates it once (CRC + ids-ascending +
  starts-increasing invariants) and attaches the **paged index**: nothing per-series is pinned in
  the heap — the entries view is held only while at least one fetch is reading the part (dropped
  when the part's refcount reaches zero) and re-fetched through `backend.ReadView` (a zero-copy
  cache hit) on the next use, so resident index memory is governed by the read cache's byte budget
  rather than series count, and opening a part reads no series column at all. Lookup cost is
  within ~4% of the resident form (same binary search, big-endian entry decode per probe). On a
  backend without the `Viewer` capability (bare cold tier, read cache off) the view is loaded once
  and kept — the old footprint, no re-read regression. A part with a missing or invalid sidecar (a
  part written before the sidecar existed, or a corrupt object) falls back to the **resident
  index** built by scanning the sorted series column once: a sorted `ids` slice plus an `int32`
  row-start offsets slice (runs partition `[0, rows)`, so series k's range is
  `[starts[k], starts[k+1])`) — 20 bytes/series in heap. The sidecar is derived (rebuildable from
  the column), so it carries no format-migration burden; it costs 20 B/series/part on disk
  (re-based in the density budgets). `mergeInto` decodes a series' `ts`/`value` sub-slice within
  the window.
- **Merge — compaction + retention + downsampling + recompression + precision** (`merge.go`,
  `downsample.go`, `recompress.go`, `precision.go`) is the one background-merge engine; all five modes
  are one pass over the immutable parts (no separate subsystem). `MergeWith(MergeOptions{RetainFrom,
  Downsample, Recompress, Precision})` (and
  the convenience `Merge(retainFrom)`) compacts a **bounded, size-tiered group** of the parts (see
  *size-tiered compaction* below — not the whole set), merging samples per series by
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
  one-sample bucket aggregates to itself); count is the documented non-idempotent exception.
- **MaxPartBytes — part splitting** (`Config.MaxPartBytes`, from `tenant.Limits.MaxPartSize`):
  **flush splits** its column output into row-bounded chunks (`chunkRanges`/`slice`, a ~32 B/row
  estimate) so no freshly-flushed part exceeds the cap; 0 ⇒ unlimited (one part, byte-identical to
  before). A **merge promotes**: its output splits at the taller merge cap (`mergeHeight ×
  MaxPartBytes`), so same-tier siblings combine into a larger part rather than re-splitting at the
  flush size (see size-tiered compaction below). Splitting at row boundaries is safe — parts are
  independent and a series spanning two parts is merged back by the read seam — and a merge decides
  each split part's cold recompression/precision from its own newest sample. `replaceParts` takes the
  resulting set.
  **Precision** is age-tiered *lossy* float compression: a `PrecisionTier{Before, Bits}` re-encodes a
  **fully-cold** part's value column with the scaled-decimal codec retaining only `Bits` significant
  mantissa bits (fewer ⇒ denser, less accurate), so recent data stays lossless and only old data
  trades accuracy for size. It is per-part (decided from the merged part's newest sample, like
  recompression), never worse than lossless (the adaptive encoder keeps whichever of Gorilla/decimal
  is smaller — §2.2), and reaches a **fixed point** via the budget recorded in the manifest (a part
  already at or below the target budget is not rewritten). Records
  (logs/traces/profiles) carry retention only — downsampling, recompression and precision are
  metrics-specific.
- **Size-tiered compaction — bounded working set** (`compact.go`): a merge does **not** re-read the
  whole part set each cycle (that re-decoded, re-materialized and re-encoded the entire growing
  dataset per maintenance tick, with O(dataset) memory and write amplification — what pinned multi-GB
  of churned RSS on the object-store backend).   `selectMergeParts` instead picks only the parts worth
  merging this cycle: (a) any part a **forced rewrite** must touch — retention/downsample/recompress/
  precision eligibility, regardless of size, so age-driven work is never starved; plus (b) the largest
  group of same-size-**tier** *unsealed* parts (`sizeTier` buckets by power-of-two row count above a
  floor; ties drain the smaller tier first), so small freshly-flushed parts merge up into larger ones.
  A part that has reached the merge cap (`mergeHeight × MaxPartBytes`) is **sealed** — re-merging it
  only re-splits it into equally-full parts (pure churn), so it is never compacted again. Parts below
  the cap roll up through progressively taller size tiers (each merge of same-tier siblings produces
  a larger part), so part count is bounded at ≈ dataset / (mergeHeight × MaxPartBytes) instead of
  growing with every flush. The chosen group is capped — by cumulative rows at the merge cap (so one
  merge's selected input is at most one sealed-tier part's worth) when a part cap is set, else by
  part count. The multi-part path **streams both its input and its output** (`compactStream`): each
  source part is read through a forward [partStream] (`block.ColumnReader.TsCursor`/`FloatCursor` →
  `chunk` forward decoders) that decodes one series range at a time, advancing strictly forward
  through the part's (series, ts)-sorted rows, so the decoded input resident at any moment is
  O(parts × one-series-range), not O(parts × whole-column) — the streaming k-way merge, which keeps
  the background merge's working set bounded regardless of how large the merged parts grow; merged
  rows accumulate in one reused buffer flushed to a part each time it reaches the cap, so output
  memory is one part's worth. (A lone forced part keeps the in-memory path with its fixed-point
  skip.) The facade defaults `MaxPartSize` to `defaultMaxPartBytes` (64 MiB) when a tenant leaves it
  unset, so the sealing bound — and thus the bounded merge — applies by default.
- **Fetch** (`engine.go`) implements the fetch contract: it resolves matchers to series
  over the index, then merges each series' head buffer ∪ every part by timestamp into one
  batch. With the block cache on and block-framed parts, the merge is **block-sliced**
  (`seriesblocks.go`): for each matched series it slices the spanning column blocks **straight from
  the cache** (decoding+caching a miss) and adds them to the per-series merge as *views* into the
  immutable cached blocks — it **never materializes a whole-part `decodedPart`**, so the per-fetch
  transient is the result, not the decoded columns (the concurrency RSS cliff `growLen` showed). The
  cached block stays reachable through the merge run until `collect` copies the samples out; a one
  block-per-column memo keeps consecutive same-block series from re-locking the cache. A cache-off
  engine, or a constant/legacy-unblocked column, falls back to decoding the part once per fetch (a
  per-fetch memo), **series-skipped**: it decodes only the blocks the matched series' row ranges
  touch (`neededBlocks` → `DecodeBlocksInt64/Float64`), so a sparse selector decodes a fraction of the
  columns (≈3–4× less for a single-series selector over a many-series part). When a per-tenant **decode cache** is configured (`Config.DecodeCacheBytes`, `blockcache.go`)
  those decoded blocks are also memoized *across* fetches: a byte-bounded LRU keyed by **`(part prefix,
  column, block index)`**, so the resident set is the *useful* blocks across live parts rather than
  every whole part touched, and an overlapping query reuses already-decoded blocks (columns cache
  independently — a ts-only count and a value-reading fetch over the same part share the ts blocks
  without the value column ever being decoded). A cached block is an immutable decoded slice; a fetch
  either **copies** the blocks it needs into a pooled per-fetch `decodedPart` or (on the block-slice
  path) holds them as merge *views*, so cache entries are never mutated and stay valid across
  concurrent fetches. Blocks are dropped when the part is reclaimed (`evictPrefix`) or the budget
  evicts the coldest. An evicted block's decoded slice is **recycled** into a bounded, GC-stable
  freelist that the next cache-miss decode draws its destination from (`DecodeInt64Into` /
  `DecodeFloat64Into`, decompressing through a scratch buffer the per-column `Decoder` reuses across
  its blocks) — this cuts the miss-path allocation *rate* (the dominant query-path allocation once the
  live heap is bounded) without enlarging the resident set. The freelist's bound scales with the
  number of in-flight fetches (baseline at rest), and a draw scans for a size-fitting buffer instead
  of discarding a too-small one. Because a fetch may still be viewing a
  block when the byte budget evicts it, each entry is **reference-counted**: `get`/`insert` pin it, the
  fetch releases each series' pins as soon as `collect` has copied that series' samples out
  (`releaseSeriesPins`, keeping only the memoized blocks pinned) and sweeps the rest at teardown
  (`releaseParts` → `releasePins`), and a buffer returns to the
  freelist only once its entry is both evicted and unpinned — so a reader never sees a block it holds
  recycled underneath it, and a block evicted mid-fetch recirculates while the fetch is still running
  instead of being held hostage until teardown. With the cache on, a fetch also **prefetches** the
  parts it will touch — warming each part's matched blocks concurrently (bounded fan-out) so backend
  reads + decodes overlap instead of running one part at a time.
  A **decode-memory budget** (`Config.DecodeMemoryBytes`, `budget.go`) caps the total in-flight
  decoded column bytes across concurrent queries: each query estimates its decode footprint (8 bytes ×
  rows × needed columns, summed over the parts it touches) and reserves it from a shared byte
  semaphore before reading any part, releasing on `releaseParts`. It bounds the query-concurrency RSS
  cliff — N heavy queries serialize through the cap instead of each materializing whole columns at
  once. The reservation is taken once per query off the engine lock (not incrementally per part, so
  two queries cannot each hold a partial reservation and deadlock), and a query larger than the whole
  budget is admitted alone rather than waiting forever. 0 ⇒ unlimited. The budget object is
  shareable (`engine.NewDecodeBudget`, `Config.DecodeBudget`): the storage facade builds **one**
  budget from `WithDecodeMemory` / `Options.DecodeMemoryBytes` and hands it to every tenant engine,
  so the cap bounds the process-wide in-flight decoded bytes rather than multiplying per tenant —
  fit it to the process memory budget (e.g. `GOMEMLIMIT` minus caches and baseline).
  A **recent tier** (`Config.RecentWindow`, `recent.go`) opt-in mirrors the most recent flush window
  in RAM across flushes (the head is drained on every flush, but the tier persists), so a query whose
  `[Start, End]` falls inside the tier's window is served from the tier ∪ the mid-flush buffers ∪ the
  head and **acquires no part at all** — first-touch recent-range queries skip the decode path the
  cache only helps on repeats. The tier is populated at flush publish from the detached buffers and
  trimmed to the window; the same samples live in both the tier and a part, and the freshest-wins
  timestamp merge dedups the overlap.
  during the merge. `Close` flushes the head.
- **Aggregate pushdown** (`aggregate.go`, `seriesstats.go`) — `AggregateRange` returns a per-series
  `SeriesAgg` (count/sum/min/max → avg) over a window. With `Config.AggregateStats` (opt-in,
  exposed as `WithAggregateStats` / `Storage.AggregateMetrics`), each part writes a small **stats
  sidecar** (`{prefix}/stats`: per-series count/sum/min/max, CRC-guarded, deleted with the part) at
  flush/merge. A range that **fully covers** a part folds its sidecar **without decoding the value
  column** — one number per series instead of every sample. It is taken only when provably exact:
  in-window parts must be fully covered *and* pairwise time-disjoint (plus head newer), so a
  timestamp can't be double-counted; otherwise (partial range, overlapping parts, a sampled/
  sidecar-less part) it falls back to decode + merge, which dedups. The sidecar is a derived
  optimization — absent/corrupt ⇒ decode — so it carries no golden/format-stability burden. Off by
  default (it costs a little per-series storage); a full-range aggregate is **~30× faster and ~19×
  lighter** than fetch-and-fold when on. `AggregateStep` (facade `Storage.AggregateMetricsStep`) is
  the step-bucketed form — per series, the aggregate of each step-aligned bucket on the absolute grid
  (the range-vector shape an embedder's `*_over_time` needs); it reuses the same pushdown, folding a
  part's sidecar whenever the part falls wholly inside one bucket and decoding only the parts that
  straddle a bucket boundary (an unsafe plan merges first to dedup by timestamp). In **cluster** mode
  the pushdown is preserved across nodes: a compact aggregate RPC (`cluster.AggregatePath`,
  `AggregateHandler`/`RemoteAggregator`) has each shard owner run `AggregateStepNamed` locally and
  ship per-series identity + buckets, which the coordinator (`clusterAggregateFor`) re-checks against
  the full matcher set and unions — so only aggregates cross the wire, not raw samples (§3i). The
  facade exposes both forms: `Storage.AggregateMetrics` (unlabeled, `map[SeriesID]SeriesAgg`) and
  `Storage.AggregateMetricsNamed` (`[]SeriesAggregate`, each a `signal.Series` + `SeriesAgg`) — the
  labeled form lets an embedder render the result as a PromQL vector from the same sidecar pass,
  without a second value-decoding fetch (cluster mode unions via `clusterAggregateNamedFor`).
  `Reset(ctx)`
  is the inverse of accumulation: it replaces
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
AllConditions, Projection, SecondPass, Limit, Reverse}` carries two operator-free predicate families: **callback
matchers** — `Matcher{Name, Match func(signal.Value) bool}` — that resolve **identity** over the
postings index (a metric series, a log stream), and **callback conditions** —
`Condition{Column, Match, Tokens, Equal}` — that filter the **per-record columns** within that identity
(a log record's severity/body/attributes). Neither is an operator enum, so equality/regex/
negation and condition extraction live in the language layer (§3h) and storage stays
operator-free. `Fetcher.Fetch` returns an `Iterator` of `*Batch{ID, Series, Timestamps, Values,
Columns}`: metrics populate `Values`, logs populate the named `Columns` (`Projection` narrows
them, `SecondPass` post-filters). The fields are zero-valued for the other signal, so the metric
path is unchanged by the log additions. `SliceIterator` and `Drain` are
the in-memory helpers.

**Ordered top-N pushdown (`Request.Limit` + `Reverse`).** A limited log query (`{…} | … | limit N`)
otherwise fetches every matching record and discards all but `N` after the caller materializes them —
the dominant cost when the selector matches a large window. `Limit > 0` bounds the returned records to
the most recent (`Reverse`) or oldest by timestamp across all matched streams, so the caller
materializes ~`N` rows instead of the whole window. It composes with `Matchers`/`Conditions`/
`SecondPass` (filtering runs first, the limit selects over survivors), so a per-row condition — e.g. a
`| json | status>=400` filter lowered to a `Condition` over the body column — drops rows *before* the
limit counts them. The result is a deliberate **superset**: the record engine keeps the `N` rows
beyond the boundary timestamp plus any rows tying at that boundary, so the caller's own exact
ordering+limit (which breaks ts ties by label) never loses a boundary row. Honored by the **record
engine** (`recordengine.Fetch`: per-stream runs are ts-sorted, then a pooled global selection trims
to the boundary via `recordCols.keep`); the **metric engine ignores it** (PromQL needs every sample).
`Limit == 0` is unlimited (unchanged behavior).

**Count pushdown (`fetch.Counter` / `fetch.GroupCounter`).** Two optional `Fetcher` capabilities
answer count-shaped PromQL without materializing samples or labels. `Counter.Count` (engine:
`count.go`) backs `count(<selector>)`: matched ids resolve from postings; a count-shaped read
plans through a **lightweight existence plan** (`planExistence`) that computes one in-memory
existence flag per matched id under the lock by scanning the live head/flush/recent buffers
directly — no per-series sorted batch copies, no batch maps, no identity slab (snapshotted only
for `CountBy`'s grouping), no block readers — so a broad concurrent count holds none of the
fetch-shaped per-plan state that used to top the live heap; per-series in-window
existence for flushed data comes from the part index — a part fully covered by the window contributes its matched
ids by a sorted intersection with **zero column decode**; only window-edge parts decode, and only
their timestamp column. `GroupCounter.CountBy` is the grouped variant backing
`count by (label)(<selector>)` (and, via the result's length, `count(count by (label)(...))` =
distinct label values): the same existence flags, grouped by the label's canonical text read from
the snapshotted series identities over the same flattened key space the postings index sees (point
attrs → `otel.scope.name`/`version` → scope attrs → resource attrs; absent ⇒ the `""` group).
`CounterOf`/`GroupCounterOf` walk the `Unwraper` decorator chain to find the capability;
multi-child fan-outs opt out (their counts are not a simple delegation). The PromQL queryable
exposes both as `CountSeries`/`CountSeriesBy` hooks (interface-asserted by the embedder's engine),
each falling back to a Fetch-and-recheck path when the selector's matchers are not all index-safe.

**Opt-in buffer reuse (`Request.Recycle` + `Batch.Release`).** A batch's `Timestamps`/`Values`
slices (and, for record signals, its `Columns`) are the engine's buffers. When a caller sets
`Request.Recycle` and calls `Batch.Release()` once done with each batch, the engine hands out (and
recycles) those buffers from a pool via a single shared release hook (no per-batch closure). The
metric engine's pool is a GC-stable, doubly-bounded freelist (entry count + total retained
capacity), not a `sync.Pool` — a `sync.Pool` is emptied at every GC, so under allocation-driven
collections the result buffers lost their capacity and every fetch re-minted its columns. It is
**opt-in and default-off**: with `Recycle` unset the engine allocates fresh buffers and the caller
need not release, so the non-recycling path takes no pool overhead and is byte-for-byte as before.
After `Release` the batch and its slices must not be read. Pass-through decorators (single-child
`Merge`, split) forward the hook; a decorator that retains/clones a batch (the results cache, a
multi-child `Merge`) produces hookless copies and releases its inputs. The cluster read handler sets
`Recycle` and releases after serializing, so a fan-out read recycles the serving node's buffers.

The metric engine recovers its pooled buffers directly from the batch's own slices. The record
engine's pool entry is the per-stream accumulator (`*recordCols`, whose columns back `Columns`), not
recoverable from the batch alone, so the batch carries it via `Batch.SetReleaseState`/`ReleaseState`
(a pointer handle, no allocation) for the engine's shared `recycle` hook to recover. Independently of
`Recycle`, the record engine **always** pools its part-decode int columns (`i64Pool`): a part's
decoded timestamp/int columns are copied by value into the accumulators, so they are dead once a part
is distributed and reuse carries no aliasing risk (the byte columns are concatenated into each
accumulator's own offsets+blob column, so the part's decoded byte data is likewise dead and left to
the GC / a future decode cache). Measured: metric recycling read `B/op` ~59% / time ~24%;
record plain read `B/op` −25% (int pooling), record recycling read `B/op` −63% / time −44%.

**`Merge(fetchers...)`** is the fan-out combinator: it runs a Request
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
  kept. The projection is a pure function of the content-addressed `SeriesID`, so a `Queryable`
  reused across queries **memoizes** it per id (bounded by cardinality — the same label set
  Prometheus keeps resident) and a Select's series share one scratch builder + text buffer.
- **Zero-copy samples + buffer recycling.** Each fetched batch becomes a Prometheus series whose
  iterator reads the batch's `Timestamps`/`Values` slices **directly** (ns→ms on the fly) — no
  per-sample copy or `chunks.Sample` interface boxing. The aliased buffers stay valid until the
  querier is closed: Select sets `fetch.Request.Recycle`, holds the matched batches, and
  `querier.Close` (called by the engine after evaluation) releases them, recycling the engine's
  result buffers (§3g). A batch that fails the matcher re-check is released immediately.
- **Time units.** Storage is unix **nanoseconds**, Prometheus is **milliseconds**; the
  adapter converts both directions (querier window and sample timestamps).

The matcher-lowering and label-projection helpers are exported — `PushableMatchers`,
`MatchesAll`, `PromLabels` — so an embedder building a pushdown path over the fetch/aggregate
seam (e.g. oteldb's `*_over_time` aggregate pushdown, which renders a `Storage.AggregateMetrics`
result as a PromQL vector) reuses the adapter's translation rather than duplicating it. They are
the single source of truth for the Prom↔storage projection.

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
makes exactly one node compact a shard at a time is the compaction-claim executor (§3j). That
executor is now **event-driven and minimal-move**: it tracks the claims it holds and on each
pass issues an etcd write only for a shard whose ring-primary actually changed (steady state is
zero round-trips), and it **records the `rebalance.Plan` it enacted** at each ring change
(`Ownership.LastPlan`) so an operator can preview what moved. `Plan` is therefore no longer
informational-only — it is the published handoff record (and the same pure diff a preview API
can compute against two rings).

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
`tenantOfShard`). **Sharding applies to all signals.** The record signals (logs/traces/profiles)
shard the same way: `writeRecordsClustered` groups each stream by `shardKeyOf(tenant,
hash(streamID)%N)` and routes per shard; `clusterRecordFetcherFor` gathers across all N shards
(concatenating, since records are not ts-deduped). The cross-shard reassembly that a record query
needs is handled explicitly: **trace-by-id** runs the fetch across every shard (a trace's spans
belong to different service streams that scatter), **`LogSeries`/`TraceSeries`/`ProfileSeries`**
concatenate per-shard series (disjoint sets) and **`LogKeys`** unions per-shard keys (OR-ing scope
bits), and the **profile symbol store** is unioned across shards (`clusterProfileSymbols` merges each
shard's tables — content-addressed, so a plain dedup) so a flamegraph over samples from several
shards resolves every stack. `retainFrom` resolves a record shard key's policy via `tenantOfShard`,
like `metricMergeOptions`. One knob (`ShardsPerTenant`) governs every signal; N=1 stays
byte-identical to the unsharded layout.

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
otherwise it fans out over HTTP to an owner's read endpoint, **hedging** across owners (a single
owner's copy is complete) — the first owner is tried immediately and a second is raced once it is
slow or errors, first success wins (`hedgedFetcher`, §3m). Matchers are opaque Go predicates and **not serializable**, so
the RPC carries only the tenant + window — a peer returns its whole window (a superset the fetch
contract permits) and the requesting node **re-applies the matchers** (their closures live
there). A three-node, RF=2 end-to-end test confirms a query on a non-owner returns the data.

**Rebalance executor** (`cluster/etcd/ownership.go`): the handoff is enacted via **exclusive
compaction claims**. `Ownership.Acquire` is an etcd CAS (create-if-absent) bound to the node's
membership lease; `Reconcile(ring, shards)` is **stateful and minimal-move** — it tracks the
shards it currently holds, computes the wanted set by in-memory ring-primary lookups (no etcd),
and issues an etcd write only to acquire a wanted-but-unheld shard or release a held-but-unwanted
one. Steady state (unchanged ring, no new tenants) is therefore **zero etcd round-trips** instead
of one acquire/release per shard per tick; retrying the wanted-but-unheld acquires every pass is
what converges a handoff once the prior owner releases. It returns the full owned set and, on a
ring change, records the enacted `rebalance.Plan` (`Ownership.LastPlan`, rf=1 primary handoffs)
for operator preview. In cluster mode the maintenance loop flushes/merges **only owned tenants**,
so a tenant's parts are written to the shared object store by exactly one node — even during
ring-disagreement windows, the claim arbitrates. A departed node's claims auto-free with its
lease and the new primary takes over (tested via lease revoke). A two-node test confirms only the
primary compacts; another confirms steady-state reconcile is a no-op and a node removal produces
the expected one-in/one-out handoff plan.

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
  blooms, and `Fetch` implementing `fetch.Fetcher`. **Byte columns** (head buffer, fetch
  accumulators, and the part read path) use a contiguous **offsets+blob** layout (`byteCol`: one
  `[]byte` data blob + `[]int32` row end-offsets, cell `i` = `data[offsets[i]:offsets[i+1]]`) rather
  than a `[][]byte` of per-cell slices — the GC scans two headers per column instead of one per row,
  and a per-record scan walks one allocation with locality. Cell views alias the blob under the
  read-only-until-next-append rule (an append that grows the blob may move it, so a value retained
  past an append is copied); the `fetch.NamedColumn` boundary materializes `[][]byte` views into the
  blob, pooled across recycled fetches. Flush is a **pass-through**: `block.Column` accepts the
  blob+offsets form directly (`BytesBlob`/`BytesOffsets`, encoded byte-identically to the `Bytes`
  form via `chunk.EncodeBytesBlob`/`EncodeBytesRawBlob`), so writing a part walks the head buffer's
  blobs without materializing a view per row. `Fetch` is heavily tuned: **lazy column decode**
  (materialize only the columns a request's conditions + projection reference — a body search
  projecting body touches just `ts`+`body`), decode each surviving part **once** distributing rows
  to per-stream accumulators, pre-size accumulators from row-range counts, bulk-append in-window
  ranges, filter **in place**, and skip the sort when already ts-ordered. When the request sets
  `Limit` (§3g), a final pooled global selection trims the ts-sorted survivors to the newest/oldest
  `Limit` records by timestamp (boundary ties kept) so a limited log query returns ~`Limit` rows, not
  the whole window. Flush/merge materialize the full schema. Conditions over a non-fixed column are
  per-record **attributes**, resolved by the zero-allocation `signal.LookupAttribute` over the
  serialized `attrs` column.
- **Per-column blooms** (`index/bloom`): a token bloom (bit array, k xxh3-128 double-hash probes,
  versioned+CRC'd, **no false negatives**) per bloom-bearing column, written as `bloom-{col}.bin`.
  `FullText` columns tokenize their value (a `contains` token prunes); `Attrs` columns hold
  **key-scoped** `key‖value` (equality) and `key‖word` (contains) tokens per record attribute;
  `Equality` columns hold each value verbatim (the **trace-by-id** path). `Fetch` skips a part whose
  bloom proves a required `Condition.Tokens`/`Condition.Equal` absent, then re-checks per row.
- **Record-key footer** (`{prefix}/keys.bin`, magic+version+CRC32C): each part persists its distinct
  per-record **attribute keys** (not values — bounded by the stream schema, so tiny) next to its
  blooms, written by `writePart` (so flush and merge produce it) and loaded by `openPart`.
  `Engine.Keys(start, end)` enumerates the distinct attribute keys across head ∪ in-window parts,
  each tagged with a `KeyScope` bitset (resource / scope / record): stream-identity keys come from
  the authoritative series index, record keys from the head buffers and the part footers. It is the
  enumeration twin of `Engine.Series` (identities) and backs the facade's `LogKeys` — letting an
  embedder list/push-down **record-attribute** labels that `Series`-based resolution cannot see, and
  authoritatively distinguish a stream label from a record attribute (or both) via the bitset.
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
  `severity_text`/`body`(FullText)/`trace_id`(Equality)/`span_id`/`attrs`(Attrs)(bytes). `WriteLogs` /
  `LogFetcher`, plus **`LogsForTrace(tenant, id)`** — logs-by-trace-id as an equality condition on
  `trace_id`, pruned by its equality bloom (mirrors traces' `Trace`, for "logs for this trace").
- **Traces** (`signal/trace`): a span is a record. Schema = `duration`/`kind`/`status_code` +
  ingest-computed nested-set ids `parent_id`/`nested_set_left`/`nested_set_right` (int) +
  `trace_id`(Equality)/`span_id`/`parent_span_id`/`name`(FullText)/`status_message`/`attrs`(Attrs) +
  serialized `events`/`links` (bytes). `span_id` and `trace_id` are high-cardinality id columns, so
  both use the dictionary-free fixed-width `CodecBytesRaw`: at production cardinality (hundreds of
  thousands of distinct ids per part, far above the dictionary's 65536 cap) the dictionary codec
  degrades to its flat length-prefixed fallback (17 B/row for a 16-byte id), whereas the fixed-width
  form stores 16 B/row and decodes far faster (no dictionary reconstruction). `trace_id` still carries
  the equality bloom for trace-by-id pruning.
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
  **hedging** across owners (§3m) so a slow/down owner is raced or failed over. Decoders for both
  peer responses are bounds-checked and fuzzed.

---

## 3k. Admission control & backpressure (`admission.go`, `engine/admission.go`)

Overload protection so a load spike **degrades rather than OOMs**. It is **lossless** (parts are
always exact) and enforces the per-tenant `tenant.Limits`, resolved through the policy callback so
changes hot-reload on the next write. Anything shed is reported back via OTLP partial-success
(`Accepted.Rejected` + `RejectedReason`), never silently dropped or queued unbounded. The admission
stage sits **between tenant resolution and the engine** in the write path (`WriteMetrics` and, for
the record signals, `writeRecordsLocal`), so a shed point costs no index/head/WAL/flush work; the
engine core stays policy-agnostic (it sees only numbers via `engine.AppendLimits` /
`recordengine.AppendLimits`, never a tenant or policy). It covers **all four signals** — metrics on
the float engine, logs/traces/profiles on the record engine (cardinality + in-flight enforced per
record in `recordengine`'s head; no sampling, since dropping a log/span would break a stream/trace).
Three valves:

- **Ingest rate** (`Limits.IngestBytesPerSecond`) — a per-tenant **token bucket** (`tokenBucket`,
  burst = one second of budget) in the facade; an over-budget batch is shed up front. The clock is
  injected (`Storage.now`) for deterministic tests.
- **Cardinality** (`Limits.MaxSeries` hard ceiling, optional `Limits.MaxSeriesSoft` soft budget) —
  enforced **in the head** (`head.appendByID`, race-free under the engine lock): a sample that would
  mint a *new* series past the hard cap is shed; already-known series are never blocked. With a soft
  budget set (`0 < MaxSeriesSoft <= MaxSeries`) plus a caller-supplied `AppendLimits.Overflow`
  remapper, a new metric series in the `[soft, hard)` band is instead **routed to a synthetic
  overflow series** — the facade's `metricOverflow` collapses it to `{__name__, __overflow__="true"}`,
  one bucket per metric name — keeping the tenant queryable and its aggregates approximately right
  under a cardinality spike rather than hard-dropping new series. The overflow series is exempt from
  the cap; the redirected sample is logged to the WAL under the overflow id (replay-consistent) and
  counted as **accepted + overflowed** (`AppendResult.Overflowed`, `AdmissionStats.Overflowed`,
  `storage.ingest.overflowed`). The remapper keeps the head signal-agnostic (it never sees
  `__name__`). No hysteresis: the head's series index is monotonic within an engine's life, so the
  budget does not breathe back down (see `docs/design/cardinality-overflow.md`). Metrics only; the
  record signals keep the hard reject.
- **In-flight memory** (`Limits.MaxInFlightBytes`) — also head-enforced, against the head's buffered
  **byte measure** (`engine.SampleBytes` = 16 per buffered sample, reset on flush, recomputed on
  replica trim): samples arriving while at the cap are shed until a flush drains the head. This is the
  bounded-memory valve; pair it with `FlushInterval` (or a flush threshold) so the head drains and the
  valve reopens.

`engine.AppendBatch` returns an `AppendResult` breaking accepted/rejected down by reason
(`RejectedOOO`/`RejectedCardinality`/`RejectedBytes`), which the facade folds — together with rate
rejections — into the `Accepted` reply, into per-tenant **meta-metrics** (`AdmissionStats`, exposed
by `Storage.AdmissionStats(tenant)`: accepted plus rejected-by-reason plus `SampledDropped`), and —
when a meter is configured — into **OTel counters** via the injected observability handle
(`internal/obs`, §4a): `storage.ingest.accepted` / `.rejected` (by `reason`+`signal`) /
`.sampled_dropped`, emitted **once per write** (bulk, never per point). So an operator sees which
valve tripped both in-process and in their metrics backend.

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

Scope today: **all four signals**, single-node *and* clustered. The **clustered write path applies
the lossless valves**: the ingest-rate valve runs at the origin (per real tenant, so each node
rate-limits its own ingest, like the single-node path), and the cardinality + in-flight-memory valves
run on the **shard primary** — the single authority for the shard — inside
`engine`/`recordengine.ApplyPrimary`, which now takes the tenant's `AppendLimits` and returns a
per-reason `AppendResult`. That breakdown rides the primary-write RPC back to the origin
(`primaryReject` = ooo ‖ cardinality ‖ inflight), so clustered ingest reports the same per-reason
`Accepted{Rejected, RejectedReason}` as single-node. Budgeted (lossy) sampling stays **metrics-only,
single-node** — but its scale factor is now **WAL-durable**: a sampled batch logs its per-sample
weights via a dedicated `recordSamplesSF` WAL frame, and replay restores them, so a crash recovers
unflushed *sampled* data at its representative weight rather than weight 1 (the unsampled path keeps
the original no-sf frame, byte-for-byte). The **soft cardinality budget with `__overflow__` routing**
is built (metrics, single-node — see the Cardinality valve above; no hysteresis, as the head's series
index is monotonic). Not yet built (the rest of §8a): the clustered **central→edge budget feedback**
(each node's rate valve and soft budget see only their own node's traffic) and **overflow on the
clustered write path** (the primary applies the hard cap only). `MaxPartSize` **is** now enforced —
flush and merge split their output so no part exceeds it (§3f, `engine.Config.MaxPartBytes`).

---

## 3l. Observability (`internal/obs`, `query/profile`)

The library is **observed through embedder-injected handles** — it never owns a global logger,
tracer, or meter. `Options.Logger` (a `*zap.Logger`), `Options.TracerProvider`, and
`Options.MeterProvider` (OTel **API** types — the embedder brings the SDK, exporters, and
sampling) feed `obs.New`, which builds the single `*obs.Obs` handle threaded into every layer
(`engine`, `recordengine`, `wal`, the backend decorator, the cluster transport). **Every pillar
is no-op by default** — an unset logger becomes `zap.Nop`, an unset provider becomes the OTel
noop provider — so an unconfigured store spans, logs, and counts nothing and pays no overhead.

- **Tracing.** Each coarse operation opens a span: `engine.flush` / `engine.merge` /
  `engine.fetch` (and the `recordengine.*` analogs), backend object ops, and the cluster RPCs.
  W3C trace-context propagates across the node-to-node HTTP transport via the **global**
  `otel.GetTextMapPropagator()` (default no-op) — `obs.InjectHTTP`/`obs.ExtractHTTP` on the
  replica-write and read paths — so a distributed query is one trace.
- **Metrics.** `internal/obs` owns the instrument set: ingest admission meta-metrics
  (`storage.ingest.{accepted,rejected,sampled_dropped}`, the rejected counter tagged by reason),
  plus flush/merge/fetch/backend/WAL latency+throughput. Instruments fire at **operation
  granularity only** (flush, merge, fetch, RPC, WAL append/fsync/rotate) — **never per-sample or
  per-row**, preserving the zero-alloc hot path.
- **Logging.** zap is plumbed through the **context** with `github.com/go-faster/sdk/zctx`: each
  operation seeds the injected logger as the zctx base (`obs.Obs.Base`) before starting its span,
  and every layer below retrieves a logger with `zctx.From(ctx)` / `obs.Obs.Logger(ctx)` — so log
  lines automatically carry the active span's `trace_id`/`span_id` and a layer needs no logger
  handle of its own, only the ctx. Debug fires at each layer boundary (facade `write start`/`done`,
  `query fetch`, engine/recordengine `fetch start`/`done` + `flush`/`merge`, `backend read/write/
  cas/list/delete`, WAL `segment opened`/`rotate`/`checkpoint`, cluster `primary-write send`/
  `received`); Info on lifecycle (`storage opened`/`closed`, member join/leave + ring rebuild,
  cluster join); Warn on `admission shed writes` (only when overload control rejects); Error on
  operation failure. All at operation granularity — **no per-sample logging**. The two background
  components without a request ctx (the etcd membership watch, the WAL writer) take the logger
  directly via `SetLogger`.
- **EXPLAIN ANALYZE** (`query/profile`). Opt-in per query via `profile.WithCollector(ctx)`:
  operators call `profile.Begin(ctx, name)` to push a timed node onto a concurrency-safe tree
  (nil collector ⇒ no-op, the default). A fetch yields `query → engine.fetch →
  {resolve-matchers, scan}`, a cross-tenant fetch a `fan-out` node per engine. **Distributed:**
  the read RPC client sets an `X-Oteldb-Profile` header when a collector is active; the peer runs
  the read under its own collector and prepends a `[uvarint len][profile-tree]` frame to the
  response (`Node.Encode`/`Decode`, bounds-checked + fuzzed); the requester strips the frame and
  grafts the peer's subtree (labeled by peer address) under the current node, so a query on a
  non-owner shows the owner's timing as a nested `remote {addr}` subtree.

## 3m. Transport reliability (`internal/retry`, `reliability`)

The two transports that leave a process — the node-to-node cluster RPCs and the S3 backend — must
survive a lossy, noisy network where a request can fail instantly, hang and fail after a long
timeout, or simply run slow. `internal/retry` is the shared mechanism; `reliability.RetryConfig`
(public, with `Default` and `LossyEnvironment` presets) is the knob, wired through `Options.Retry`
(cluster) and `s3.WithRetry` (backend). It is **default-on** with the mild `Default` profile and
lives entirely off the data plane's hot path.

- **`internal/retry`** provides three composable primitives: `Do` (bounded sequential retry with a
  per-attempt timeout and exponential, equal-jittered backoff), `Hedge` (opportunistic concurrent
  retries — launch the first attempt, stage the next once the in-flight one passes `HedgeDelay` or
  fails, race them, return the first success and cancel the losers), and HTTP error classifiers:
  `Transient` (retry an idempotent call on any transport error except a parent cancellation),
  `ConnFailure` (the *safe* write predicate — retry only when the request provably never reached the
  server), and `RetryableStatus` (5xx/429).
- **Idempotent reads hedge.** The cluster read fan-out (`hedgedFetcher`, replacing the old
  sequential failover) races a request across a shard's replica owners — first owner immediately, a
  second once it is slow or errors — so a single slow/stuck/down owner no longer dictates latency or
  fails the read. The record-signal enumeration RPCs — series listing (`LogSeries`/`TraceSeries`/
  profile series), attribute-key listing (`LogKeys`), and the profile symbol-store fetch — hedge
  across owners the same way. The S3 backend hedges `GetObject` (a slow GET is re-issued on a fresh
  connection) and never retries a genuine not-found.
- **Per-attempt timeouts everywhere.** Every attempt is bounded by `PerTryTimeout` via context (not
  `http.Client.Timeout`, which would abort a request the hedge layer still wants to race); the
  cluster HTTP client also sets dial/TLS/response-header timeouts (it was `http.DefaultClient` with
  none). A 30-second hang is abandoned in a per-try budget instead of stalling the whole deadline.
- **Writes stay at-most-once.** Primary writes and the S3 conditional put (CAS) retry only on
  `ConnFailure` and never hedge, so an ambiguous failure (a timeout after the body was sent) is not
  re-applied; idempotent S3 overwrites/deletes retry on any transient error.
- **Observable.** Retries and hedges increment `storage.rpc.{attempts,retries,hedges}` (tagged by
  op) and emit trace-correlated Debug logs, so a degrading link shows up as a rising hedge/retry rate.

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
  merges every metric, log, trace, and profile engine, applying per-tenant retention from the resolved policy.
  Engines are independent per-tenant/per-signal shards, so the loop **fans the flush/merge (and the
  background WAL fsyncs) out concurrently** under a bound (`WithMaintenanceConcurrency`, default
  CPU-derived) via `internal/parallel` rather than walking them sequentially. The work list is
  **ordered by flush pressure (head bytes) descending** and `parallel.ForEach` dispatches in index
  order, so the fullest heads flush first — fair scheduling that keeps one noisy tenant from delaying
  others' relief and drains the most in-flight memory soonest (every engine is still serviced each
  cycle; ordering is a within-cycle priority, not a cap). `Reset(ctx)`
  discards all ingested data (every engine's head + flushed parts), retaining the engines
  for reuse; it is gated to an **ephemeral backend** (`ErrNotEphemeral` otherwise) and is
  meant for tests/benchmarks that reuse one store across runs. `Fetcher(tenants...)` is the
  **read seam**: it returns a `fetch.Fetcher` over the named tenants' data (head ∪ parts) —
  one tenant, several (a **multi-tenant** fan-out), or none ⇒ **all** tenants (a
  **cross-tenant** query). A fan-out merges by series id via `fetch.Merge`, federating a
  series with equal labels across tenants into one; `fetch.Merge` (and the clustered shard/tenant
  write-routing) **fetch their children concurrently** under a bound, collecting into per-index slots
  so the merge's duplicate-timestamp winner stays order-deterministic. Always usable: an empty fetcher when no
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
  and carry columns); a query supplies stream `Matchers` plus record `Conditions`. Two
  enumeration primitives sit alongside it: **`LogSeries(ctx, tenant, matchers, window)`** (matching
  stream identities) and **`LogKeys(ctx, tenant, window)`** (distinct attribute keys with their
  `KeyScope` bitset — the only way to see and push down per-record attribute label names). Both
  **fan out in cluster mode** (a non-owner serves them from an owner, hedged), over the shared
  signal-dispatched enumeration RPCs (`cluster.SeriesPath` for `LogSeries`/`TraceSeries`, the new
  `cluster.KeysPath` for `LogKeys`); `LogSeries` re-applies non-equality matchers to the owner's
  superset, and `LogKeys` takes the first owner's reply (each owner is a complete replica).
  **`TraceFetcher(tenants...)`** is the identical seam over the trace engines, and
  **`Trace(ctx, tenant, traceID)`** is the trace-by-id convenience: it issues a fetch with an
  equality `Condition` on the `trace_id` column (pruned by that column's equality bloom) and returns
  all of the trace's spans across services. **`ProfileFetcher(tenants...)`** is the identical seam
  over the profile engines (matchers resolve streams — incl. the profile type via its reserved
  `otel.profile.*` labels — and conditions filter samples), complemented by
  **`ProfileResolver(ctx, tenant)`** (stack-id → frames, for flamegraphs) and
  **`ProfileSeries(ctx, tenant, matchers, window)`** (stream enumeration, for profile-type/label
  listing) — together enough to back an embedder's Pyroscope/ProfileQL querier.
- **`Inspect() StoreStats`** (`inspect.go`) — a pull-based, **in-memory** snapshot of store state for
  an embedder's CLI/UI dashboard (and debugging): per tenant, a per-signal breakdown (series/stream
  count, head items + bytes, flushed part count, data time span) plus the tenant's cumulative
  admission tally; the read-path decode-cache totals; and, in cluster mode, this node's address, the
  live membership, the shards it holds a compaction claim on, and the last enacted rebalance plan. It
  does **no backend I/O and decodes nothing** (it takes only a brief per-engine read lock to copy
  counters), so it never touches the ingest/query hot path — poll it at dashboard cadence. Each
  per-signal entry also carries merge liveness (`MergeRunning`, `MergeBacklog`) and WAL state
  (`WAL`, `WALSegments`, `WALBytes`, `WALEpoch`). On-disk part *byte* sizes remain omitted here (they
  would need backend stat calls) — use `PartsDetailed` for those; `Inspect` stays a counts +
  time-span + state view. Backed by `engine.Stats` / `recordengine.Stats` (per-engine snapshots) and
  the cluster `Ownership.Owned`/`LastPlan` accessors.
- **`Admin()` → `Admin`** (`admin.go`) — the imperative operator-control surface complementing the
  background maintenance loop: `Flush(ctx, key, signal)` drains a head to a part, `Compact(ctx, key,
  signal)` merges its parts (reusing the loop's resolved policy — the one merge engine, no parallel
  path), `Retention(ctx, key)` compacts every signal for a tenant, `Rebalance(ctx)` reconciles
  ownership immediately, and `MaintainNow(ctx)` runs a full cycle. In cluster mode flush/compact are
  gated to the shard's **ring-primary** (else `ErrNotOwner`), preserving the single-writer-per-shard
  invariant; single-node owns everything. The `key` is the engine key (tenant id, or a metric shard
  key under `ShardsPerTenant > 1`).
- **Drill-down introspection** (`introspect.go`) — three pull-based dashboard accessors that scope to a
  `(tenant, signal)`, complementing the store-wide `Inspect()`:
  - **`Parts(tenant, signal) []PartInfo`** — one entry per flushed part (id, time bounds, series and
    row counts) from the engine's in-memory row-range index; **no backend I/O, no decode**.
  - **`PartsDetailed(ctx, tenant, signal) ([]PartDetail, error)`** — augments each part with its
    on-backend byte size and column/codec layout + chunk (granule) count from the cached manifest. It
    sums object sizes via the backend, so it is a drill-down call, not a high-frequency poll; each part
    is ref-held for the read so a concurrent merge cannot reclaim its objects.
  - **`Cardinality(tenant, signal, topN) CardinalityStats`** — total series, distinct label names,
    interned-symbol count, and the top-N highest-cardinality label names (series count + distinct
    values), computed from the head's inverted index (which spans head ∪ flushed series); no backend
    I/O. The operator's first stop for a cardinality-explosion incident.
  Backed by `engine`/`recordengine` `Parts`/`PartsDetailed`/`Cardinality`, the `postings.MemPostings.ForEachName`
  iterator, and the `wal.SegmentWriter` `Seq`/`Size`/`Epoch` accessors (the latter feed the WAL/merge
  fields `Inspect()` now carries per signal: `MergeRunning`, `MergeBacklog`, and `WAL{,Segments,Bytes,Epoch}`).
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
  (the lossy budget — §3k), `Recompress` (cold-data zstd recompression at merge — §3f), `Precision`
  (a list of `PrecisionTier{After, Bits}` age-banded *lossy* float-precision budgets, applied at merge
  so only old data trades accuracy for size — §3f), and the
  composed `Policy`, resolved per tenant id through a `Resolver` (`ResolverFunc` adapter;
  `Default()` returns an empty-policy resolver). Multi-tenancy, retention, and
  downsampling are consumer-supplied callbacks keyed by tenant id.
- **`backend`** — the L1 seam (detailed in §3a): `Read`/`Write`/`List`/`Delete`/`PutIfAbsent` over
  whole-object keys, with memory, file, and s3 implementations. The optional **`Sizer`** capability
  (`Size(ctx, key)`) reports an object's byte size without reading it; `backend.SizeOf` uses it when
  present and falls back to a full Read otherwise (memory/file implement it cheaply; the wrappers
  delegate; s3 uses the fallback). Used by `PartsDetailed` for part byte accounting.

### 4a. Observability handle (`internal/obs`)

Observability is **injected, never owned** (DESIGN §16). `internal/obs.Obs` bundles the three
pillars — a `*zap.Logger`, an OTel `trace.Tracer`, and the metric instruments — built once by
`obs.New` from `Options.{Logger, TracerProvider, MeterProvider}` and held on the facade (`s.obs`,
never nil after `Open`). Each unset pillar defaults to its **no-op** implementation (`zap.NewNop`,
the OTel noop tracer/meter), so an unconfigured store logs, spans, and counts nothing at zero
overhead, and the library imports only the OTel **API** — the embedder owns the SDK and exporters.
**Built today:** the handle + `Options` wiring + the **admission meta-metrics** (`obs.Admission`),
emitted once per write from the facade for every signal (§3k). The rest of §16 (per-layer spans +
metrics, and the `query/profile` EXPLAIN ANALYZE tree) is forward-looking.

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
- **Injected, no-op-default observability.** The library never owns a global logger, tracer, or
  meter — they arrive through `Options` and are no-op unless the embedder configures them, so the
  default path pays no overhead. Logging is plumbed through the **context** (`go-faster/sdk/zctx`):
  seed the injected logger as the base at an operation entry, then `zctx.From(ctx)` below returns a
  trace-correlated logger — never store a logger on a long-lived struct except the two ctx-less
  background components (membership watch, WAL writer). Telemetry fires at **operation granularity**
  (flush/merge/fetch/RPC/WAL), never per-sample or per-row; this is what keeps the zero-alloc
  invariant intact. Only the OTel **API** is imported (the embedder owns the SDK). EXPLAIN ANALYZE
  is ctx-threaded and no-op without a collector (§3l).
- **Stable formats.** The `Codec` enum, the per-stream header, each codec's framing, the
  part formats (manifest `OTPM`, marks `OTMK`, per-column object framing, the
  `{prefix}/manifest|marks|c/{i}` key layout), the **attribute hash/binary encoding** (the
  SeriesID pre-image), the **symbol table** (`OTSY`), the **WAL record framing** (series, sample,
  scale-factor sample, records, and side frames — record types are additive, so an old reader skips
  an unknown type), the **record-key footer** (`OTKY`, the per-part `keys.bin` distinct
  record-attribute keys), the **metric part column layout** (`[series:int128, ts:int64,
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

**Golden benchmarks & per-PR deltas.** `golden_bench_test.go` defines the definitive, deterministic
read+write performance set under `BenchmarkGolden/…` (`write/{head,flush,concurrent}`,
`read/{fetch_all,fetch_recent}`, `density`). It is deliberately self-contained so the
`.github/workflows/bench.yml` workflow can run the **same** benchmark file against both the PR head
and the base commit, diff them with `golang.org/x/perf/cmd/benchstat`, and post a sticky PR comment
with overall values plus the `vs base` delta and significance. Efficiency regression *gates* (hard
ceilings on bytes/point and hot-path allocations) live separately in `efficiency_test.go`.

**Distributed fault-injection.** `cluster/chaos_test.go` wires N real engines behind a
fault-injecting in-process transport, sharing the real HRW ring and quorum replicator, and asserts
the L0 safety properties under node death, partition, and stragglers: a quorum write succeeds iff a
quorum of owners is reachable (no false acks), every acked write survives a later minority failure
(no data loss — verified by a randomized soak), and reads converge by gathering+merging across the
reachable replicas. It drives the production `replica`/`ring`/`fetch.Merge` code, so a regression in
the quorum or convergence logic trips it (race-clean under `-race`). `cluster/rebalance_chaos_test.go`
covers the other half — **ownership handoff under membership change**: across single and rolling ring
changes (with ongoing ingest), a shard's gained owner opens a fresh engine over the shared backend and
`LoadParts` reconstructs every flushed sample, proving the object-store-native handoff is lossless and
stateless (no bytes move; the move stays minimal). `cluster/etcd/ownership_chaos_test.go` adds the
coordination half against a real embedded etcd: a contended claim admits exactly one winner,
concurrent reconciliation converges to one-owner-per-shard across a membership change, and a node
death (lease revoke) hands all of its shards to the survivors with none left orphaned.

---

## 6. Package map

```
.                     storage facade: Storage, Open/InMemory, Options, per-tenant engines, maintenance loop [implemented: metrics+logs+traces+profiles ingest+read; query-lang in embedder]
  admission.go        per-tenant admission control: ingest-rate token bucket + AdmissionStats meta-metrics (§3k) [implemented: all signals; rate at origin + cardinality/in-flight on the shard primary in cluster mode]
  inspect.go          Inspect() StoreStats: in-memory pull snapshot (per-tenant/signal counts+time-span, merge/WAL state, admission, decode cache, cluster membership/ownership) for a dashboard/CLI [implemented]
  introspect.go       Parts/PartsDetailed/Cardinality: per-(tenant,signal) drill-down for a dashboard (part list, part byte/codec/chunk detail, label cardinality) [implemented]
  admin.go            Admin(): on-demand Flush/Compact/Retention/Rebalance/MaintainNow; cluster flush/compact gated to the shard's ring-primary (ErrNotOwner) [implemented]
encoding/             umbrella doc for the codec layers
  encoding/bitstream  MSB-first bit Writer/Reader                                      [implemented]
  encoding/chunk      DoD / Gorilla / T64 / dict / bytesraw / decimal / id128 column codecs [implemented]
  encoding/compress   zstd / lz4 (pierrec/lz4) / none block wrapper                   [implemented]
pool/                 ByteIntMap (xxh3) for dict building                              [implemented]
internal/simd         vectorized columnar kernels (AVX2) + pure-Go fallback + runtime CPU dispatch [implemented: int64 + float64 min/max]
internal/cmd/gensimd  avo generator for internal/simd's committed *_amd64.s (//go:generate)   [implemented]
internal/obs          injected observability handle: zap logger (context-plumbed via go-faster/sdk/zctx, trace-correlated) + OTel tracer + per-layer metric instruments (admission/flush/merge/fetch/backend/WAL/rpc) + W3C trace propagation; no-op default [implemented]
internal/retry        transport reliability primitives: Do (retry+per-try timeout+backoff), Hedge (opportunistic concurrent retries), HTTP error classifiers [implemented]
reliability           public RetryConfig + Default/LossyEnvironment presets (the embedder-facing reliability knob) [implemented]
signal/               typed Attributes/Value, Resource/Scope/Series identity, 128-bit SeriesID, Signal, TenantID, Aggregation [implemented]
  signal/metric       []byte-based OTLP-shaped Metrics ingest batch (resettable/pooled) + identity + projection (gauge/sum; histogram/exp-histogram/summary via classic decomposition in otlp/pdataconv) [implemented]
  signal/log          []byte-based OTLP-shaped Logs ingest batch (resettable/pooled) + stream identity + projection [implemented]
  signal/trace        []byte-based OTLP-shaped Traces ingest batch (resettable/pooled) + span schema + projection (nested-set, events/links) [implemented]
  signal/profile      []byte-based OTLP-shaped Profiles ingest batch + sample schema (type folded into identity) + projection + content-addressed symbol store (SideStore) + stack Resolver [implemented]
otlp/pdataconv        optional OTel-Go bridge: pmetric.Metrics → metric.Metrics; gauge/sum direct + histogram/exp-histogram/summary classic decomposition (only package importing pdata) [implemented]
tenant/               Limits/Retention/Downsample/Sampling/Recompress/Precision/Policy, Resolver     [implemented]
backend/              Backend interface (Read/Write/List/Delete/PutIfAbsent) + memory + read cache (root) [implemented]
  backend/file        directory-tree backend; atomic write + exclusive PutIfAbsent (os.Link) [implemented]
  backend/s3          object-store-native backend over ObjectStore + aws-sdk-go-v2 adapter   [implemented; in-process go-faster/fs S3 integration test]
  backend/bucketindex versioned block-list index (time-pruned part enumeration, no full LIST) + WAL flush-epoch watermark [implemented]
block/                immutable columnar part format: column/marks/manifest/part        [implemented]
index/                symbols (intern) · series (id↔attrs) · postings (set-ops/matchers) [implemented]
  index/bloom         token bloom filter (no false negatives) + tokenizer: full-text + attr/equality pruning [implemented]
wal/                  CRC-framed segmented WAL: samples (+ scale-factor samples) + opaque records + side delta, resume + checkpoint (truncate-on-flush), facade-wired durability [implemented]
engine/               head · flush · background-merge · retention · downsampling · recompression · admission limits · fetch (metrics) [implemented]
recordengine/         shared schema-driven record engine (logs+traces+profiles): head · flush · merge · fetch · conditions · per-column blooms · optional content-addressed side store [implemented]
query/fetch           dual-shape fetch contract (Matchers + Conditions/Projection/SecondPass + Limit/Reverse ordered top-N) [implemented for metrics + logs + traces + profiles; the library's query surface]
query/scale           fetch-seam scale-out decorators: split-by-interval + results cache  [implemented]
query/profile         EXPLAIN ANALYZE: concurrency-safe per-query timing tree (ctx-threaded, no-op default) + binary encode/decode for distributed grafting [implemented]
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
