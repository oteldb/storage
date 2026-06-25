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
> Last verified against the tree: 2026-06-25 (M2: index + WAL).

`github.com/oteldb/storage` is a low-level, OpenTelemetry-centric columnar storage
**library** (Go 1.26). It has no `main`, server, or CLI: an embedder (primarily
`go-faster/oteldb`) owns the process and calls the small `storage` facade.

What is actually built today is the **encoding foundation** (the bit-level and
column-codec layer), the **part format** (`block`) and **storage backends**
(`backend` memory + file), the **identity + index layer** (typed attributes/SeriesID in
`signal`, plus `index/{symbols,series,postings}`) and the **write-ahead log** (`wal`),
its supporting **pool**, and the **public facade skeleton** (construction, options,
shared types). The remaining layers — engine, fetch contract, query languages, cluster —
currently exist as package boundaries with documented seams; their behavior is not yet
implemented.

---

## 1. Layered model

The design is a single columnar engine with swappable front-ends and backends
(`DESIGN.md` §3). The layers, top to bottom, and what backs each one **right now**:

| Layer | Concern | Realized today |
|---|---|---|
| L6 Query languages | promql/logql/traceql/genericql | — (package `query`, seam only) |
| L5 Query engine | plan IR · sharding · streaming exec · cache | — (package `query`, seam only) |
| L4 Fetch contract | matchers + column conditions + window → iterator | — (package `query`, seam only) |
| L3 Engine / **Index / WAL** | head · flush · merge / **symbols · series · postings** · **write-ahead log** | engine seam only; **index (symbols/series/postings) + wal implemented** |
| L2 **Part** / **Encoding** | **immutable parts · per-column objects · manifest** / **bitstream · codecs · compress** | **both implemented** (`block`, `encoding`) |
| L1 **Backend** | file · s3 · memory behind one interface | **memory + file implemented**; s3 + CAS pending |
| L0 Cluster | etcd ring · HRW sharding · RF=3 · rebalance | — (package `cluster`, seam only) |

The **implemented substance is the L1 backend seam, the L2 encoding column + part
format**, plus the supporting pool and the public facade. The rest of this document
details those.

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
`Delete(ctx,key)`, plus `IsEphemeral()`. Absent keys return an error satisfying
`errors.Is(_, backend.ErrNotExist)`. Ranged/streaming reads and compare-and-swap are
deferred to the s3 backend (later milestone).

- **`backend.Memory()`** (root package) — the ephemeral reference backend: a concurrent
  map that copies on both `Write` and `Read`, so stored objects are immutable and never
  alias a caller's buffer. The default in tests.
- **`backend/file`** — a directory tree. Keys map to paths under a root (with a `..`
  traversal guard); `Write` is atomic via a temp file + `fsync` + `rename`, which is the
  per-object atomicity the "manifest written last" part commit relies on.
- **`backend/backendtest`** — a shared conformance suite (`Run(t, factory)`) that both
  backends pass under `-race`, proving they are interchangeable.

Backends are interchangeable behind the interface; s3 and `bucketindex` remain seam-only.

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
  `KindBytes`. A codec is selected per kind (`CodecDoD`/`CodecT64` for int64,
  `CodecGorilla`/`CodecDecimal` for float64, `CodecDict` for bytes; overridable). The
  encoded chunk stream is wrapped in a `compress` frame. Per column the writer records
  min/max and **collapses a constant column** to a single value in the manifest (no data
  object) — the OTel resource-attribute win. The lazy `ColumnReader` decodes on demand:
  `Int64`/`Float64` into a reusable slice, `Bytes` into `chunk.DictColumn` split form, and
  synthesizes constants with no I/O.
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

- **Identity** lives in `signal`: an `Attributes` set is the typed OTel attribute model
  (`Value` = the AnyValue sum: string/bool/int/double/bytes/array/map, all held as
  `[]byte` for scalars-inline + zero-alloc projection). `Attributes.Hash` is the
  content-addressed **`SeriesID`** — a **128-bit** xxh3 of a canonical, type-tagged
  pre-image (maps hash order-independently, arrays keep order, `int 5`/`"5"`/`5.0`/empty
  are distinct). 128-bit because content addressing has no allocator to resolve a
  collision. `AppendValue`/`DecodeAttributes` are the reversible binary codec (used by the
  WAL and value interning).
- **`index/symbols`** — a `[]byte → uint32` interning table (via `pool.ByteIntMap`,
  no string conversion) with a CRC32C serialize/decode. Names and typed-value encodings
  intern to small ids.
- **`index/series`** — `SeriesID ↔ Attributes`. `Add` is idempotent (id is the hash) and
  retains a deep copy, so a query reconstructs labels from an id and replay is dedup-safe.
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

---

## 4. Public surface (`storage` root package)

The embedder-facing API. The construction and configuration path is wired and working;
the ingest and query paths are scaffolded and return `ErrNotImplemented`.

- **`Storage`** — the facade. `Open(ctx, Options, ...Option)` and `InMemory(...Option)`
  construct it (validation, defaulting, and tenant-resolver wiring run here);
  `Close(ctx)` tears it down. Ingest methods `WriteMetrics`/`WriteLogs`/`WriteTraces`/
  `WriteProfiles` take pdata and return `Accepted` (OTLP partial-success counts).
  `Query(ctx, tenant, Query)` returns a `Result`. The unexported `write` helper resolves
  the tenant policy and checks the closed flag — the seam ingest dispatches through.
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
  SeriesID pre-image), the **symbol table** (`OTSY`), and the **WAL record framing** are
  all persisted/wire-stable. Changing any of them is an architectural change (golden tests
  guard formats; bump the version and update this file too).

### Testing discipline

Implemented packages ship with ≥90% coverage, table/property/round-trip tests, fuzz
targets for every codec and the bitstream (`encode∘decode == identity`), and benchmarks
on the hot paths. `go test ./...`, `go vet ./...`, and `golangci-lint run ./...` are all
green; the tree is `gofmt`/`goimports` clean.

---

## 6. Package map

```
.                     storage facade: Storage, Open/InMemory, Options, Query/Result   [implemented: construction; ingest/query stubbed]
encoding/             umbrella doc for the codec layers
  encoding/bitstream  MSB-first bit Writer/Reader                                      [implemented]
  encoding/chunk      DoD / Gorilla / T64 / dict / decimal column codecs              [implemented]
  encoding/compress   zstd/none block wrapper (lz4 stub)                              [implemented]
pool/                 ByteIntMap (xxh3) for dict building                              [implemented]
signal/               typed Attributes/Value, 128-bit SeriesID, codec, Signal, TenantID [implemented]
  signal/metric       metrics point types & OTLP→columnar projection                  [seam only]
tenant/               Limits/Retention/Downsample/Policy, Resolver                     [implemented]
backend/              Backend interface + memory (root) + file/                         [implemented; s3/bucketindex seam only]
block/                immutable columnar part format: column/marks/manifest/part        [implemented]
index/                symbols (intern) · series (id↔attrs) · postings (set-ops/matchers) [implemented; bloom seam only]
wal/                  CRC-framed segmented write-ahead log + replay                    [implemented]
engine/               head · flush · background-merge                                  [seam only]
query/                fetch contract · plan · exec · language front-ends               [seam only]
cluster/              etcd ring · HRW sharding · replication · rebalance               [seam only]
```

"Seam only" packages currently contain their `doc.go` (and, where noted, an interface or
config type) that fixes the boundary; they have no behavior yet. As each is implemented,
move its row to "implemented" here and add a section above.
