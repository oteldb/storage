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
> Last verified against the tree: 2026-06-25.

`github.com/oteldb/storage` is a low-level, OpenTelemetry-centric columnar storage
**library** (Go 1.26). It has no `main`, server, or CLI: an embedder (primarily
`go-faster/oteldb`) owns the process and calls the small `storage` facade.

What is actually built today is the **encoding foundation** (the bit-level and
column-codec layer), its supporting **pool**, and the **public facade skeleton**
(construction, options, shared types). The higher layers — part format, index, engine,
fetch contract, query languages, cluster — currently exist as package boundaries with
documented seams; their behavior is not yet implemented.

---

## 1. Layered model

The design is a single columnar engine with swappable front-ends and backends
(`DESIGN.md` §3). The layers, top to bottom, and what backs each one **right now**:

| Layer | Concern | Realized today |
|---|---|---|
| L6 Query languages | promql/logql/traceql/genericql | — (package `query`, seam only) |
| L5 Query engine | plan IR · sharding · streaming exec · cache | — (package `query`, seam only) |
| L4 Fetch contract | matchers + column conditions + window → iterator | — (package `query`, seam only) |
| L3 Engine / Index | WAL · head · flush · merge / postings · marks · blooms | — (packages `engine`, `index`, `wal`, seam only) |
| L2 Part / **Encoding** | immutable parts · per-column streams / **bitstream · codecs · compress** | **Encoding implemented**; part format (`block`) seam only |
| L1 Backend | file · s3 · memory behind one interface | Interface + ephemeral memory stub |
| L0 Cluster | etcd ring · HRW sharding · RF=3 · rebalance | — (package `cluster`, seam only) |

The **implemented substance is the L2 encoding column** plus the supporting pool and
the public facade. The rest of this document details those.

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
  `Profile`), `ParseSignal`, and `TenantID`.
- **`tenant`** — policy model: `Limits`, `Retention`, `Downsample`, and the composed
  `Policy`, resolved per tenant id through a `Resolver` (`ResolverFunc` adapter;
  `Default()` returns an empty-policy resolver). Multi-tenancy, retention, and
  downsampling are consumer-supplied callbacks keyed by tenant id.
- **`backend`** — the L1 seam. The `Backend` interface currently exposes only
  `IsEphemeral()`; `Memory()` returns the ephemeral reference backend used as the test
  default. The full Read/Write/List/Delete/CAS surface and the file/s3 implementations
  are not yet present.

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
- **Stable formats.** The `Codec` enum, the per-stream header, and each codec's framing
  are persisted/wire-stable. Changing a framing is an architectural change (golden tests
  guard formats; update this file too).

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
signal/               Signal enum, TenantID, ParseSignal                              [implemented]
  signal/metric       metrics point types & OTLP→columnar projection                  [seam only]
tenant/               Limits/Retention/Downsample/Policy, Resolver                     [implemented]
backend/              Backend interface + ephemeral Memory() stub                      [interface + stub]
block/                immutable columnar part format + manifest                        [seam only]
index/                postings / symbols / bloom / series identity                     [seam only]
wal/                  write-ahead log framing                                          [seam only]
engine/               head · flush · background-merge                                  [seam only]
query/                fetch contract · plan · exec · language front-ends               [seam only]
cluster/              etcd ring · HRW sharding · replication · rebalance               [seam only]
```

"Seam only" packages currently contain their `doc.go` (and, where noted, an interface or
config type) that fixes the boundary; they have no behavior yet. As each is implemented,
move its row to "implemented" here and add a section above.
