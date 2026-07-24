# Architecture (current state)

> **Maintenance directive.** This file is the **high-level map** of the code as it exists
> today — layers, boundaries, invariants, package map. Per-package detail lives in each
> package's own `ARCH.md` (linked below). Keep both current: an architectural change (new
> package/layer, new public type, new or changed on-disk/wire format, a moved layer
> boundary, a new cross-cutting invariant) updates this file *or* the relevant `ARCH.md` in
> the same change. Keep it brief — anything derivable from the source belongs in the source,
> not here. Roadmap and speculation live in `DESIGN.md`/`PROMPT.md`, not here.

`github.com/oteldb/storage` is a low-level, OpenTelemetry-centric columnar storage
**library** (Go 1.26). No `main`, server, or CLI: an embedder (primarily `go-faster/oteldb`)
owns the process and calls the `storage` facade.

All four signals have a working ingest+read path: **metrics** on the float-sample engine
(`engine`), **logs/traces/profiles** on the shared schema-driven record engine
(`recordengine`). Query languages are **not** in the library — the embedder drives its own
engines over the fetch seam (`query/fetch`).

---

## 1. Layered model

| Layer | Concern | Today |
|---|---|---|
| L6 Query languages | promql/logql/traceql | **embedder's** — `query/promql` is an optional fetch→Prometheus-Queryable adapter |
| L5 Query engine | plan · exec · cache | **embedder's** — except split-by-interval + results cache, expressible over the seam (`query/scale`) |
| L4 **Fetch contract** | matchers + column conditions + window → batch iterator | `query/fetch`, exposed as `Storage.{,Log,Trace,Profile}Fetcher` |
| L3 Engine / Index / WAL | head · flush · merge · retention / symbols · series · postings · blooms / WAL | `engine`, `recordengine`, `index`, `wal` |
| L2 Part / Encoding | immutable parts · per-column objects · manifest / bitstream · codecs · compress | `block`, `encoding` |
| L1 Backend | memory · file · s3 behind one interface, CAS via `PutIfAbsent` | `backend` |
| L0 Cluster | etcd ring · HRW · replication · rebalance · EC | `cluster` |

---

## 2. Package docs

| Doc | Covers |
|---|---|
| [`encoding/ARCH.md`](encoding/ARCH.md) | bitstream, column codecs, compression, `pool` |
| [`backend/ARCH.md`](backend/ARCH.md) | the L1 seam, memory/file/s3, read cache, bucket index, stateless read path |
| [`block/ARCH.md`](block/ARCH.md) | the immutable part format: columns, marks, manifest |
| [`index/ARCH.md`](index/ARCH.md) | identity (`signal.Series`/`SeriesID`), symbols, series, postings, blooms |
| [`wal/ARCH.md`](wal/ARCH.md) | segmented CRC-framed WAL, epochs, exactly-once recovery |
| [`signal/ARCH.md`](signal/ARCH.md) | per-signal ingest models + projection, `otlp/pdataconv` |
| [`engine/ARCH.md`](engine/ARCH.md) | the metrics vertical: head, flush, merge/retention/downsample, fetch, caches |
| [`recordengine/ARCH.md`](recordengine/ARCH.md) | the shared record engine for logs/traces/profiles |
| [`query/ARCH.md`](query/ARCH.md) | fetch contract, scale decorators, PromQL adapter, EXPLAIN ANALYZE |
| [`cluster/ARCH.md`](cluster/ARCH.md) | ring, membership, replication, rebalance, sharding, partsync, erasure coding |
| [`ADMIN.md`](ADMIN.md) | operator surface: `Inspect`, `Admin`, drill-downs, metrics catalog |

---

## 3. Public surface (root package)

`Storage` is the whole API. `Open(ctx, Options, ...Option)` / `InMemory(...Option)` build it;
`Close` stops maintenance and flushes.

- **Write** — `WriteMetrics`/`WriteLogs`/`WriteTraces`/`WriteProfiles` take the library's
  internal `[]byte`-based batches (`signal/{metric,log,trace,profile}`), **not pdata**.
  Tenant is **derived** from Resource+Scope by the `Options.Tenant` callback, never passed.
  Returns `Accepted{Accepted, Rejected, RejectedReason}` (OTLP partial success).
- **Read** — `Fetcher(tenants...)` and the per-signal variants return a `fetch.Fetcher` over
  head ∪ parts; no tenants ⇒ all (cross-tenant fan-out, merged by series id). Plus the
  convenience/enumeration primitives: `Trace`, `LogsForTrace`, `LogSeries`, `LogKeys`,
  `ProfileSeries`, `ProfileResolver`, `AggregateMetrics{,Named,Step}`.
- **Maintenance** — one background loop flushes+merges every (tenant, signal) engine on
  `FlushInterval`, concurrently under a bound, ordered by head pressure. A head-bytes
  threshold pokes it early. A durable store always runs it (an unbounded head OOMs).
- **Operator surface** — `Inspect`, `Admin`, `Parts`/`PartsDetailed`/`Cardinality`,
  `AdmissionStats`. See `ADMIN.md`.
- **Policy** — `tenant.Policy` (limits, retention, downsample, sampling, recompress,
  precision, durability/EC) resolved per tenant id through a consumer-supplied `Resolver`.

### Admission control (`admission.go`, `engine/admission.go`)

Overload degrades instead of OOMing; anything shed is reported, never silently dropped.
Sits **between tenant resolution and the engine**, so a shed point costs no engine work; the
engines see only numbers (`AppendLimits`), never a tenant. Three lossless valves — ingest-rate
token bucket (facade), cardinality cap, in-flight head bytes (both head-enforced) — plus, for
metrics only, a soft cardinality budget that remaps new series into a synthetic
`__overflow__` bucket, and a budgeted **lossy sampler** that keeps a representative subset and
tags it with a per-sample scale factor (`sf` column) so counts/sums stay unbiased.
In cluster mode the rate valve runs at the origin, cardinality/in-flight on the shard primary.

### Observability (`internal/obs`, `query/profile`)

**Injected, never owned.** `Options.{Logger, TracerProvider, MeterProvider}` (OTel **API**
only) build one `*obs.Obs` threaded into every layer; each unset pillar is no-op, so an
unconfigured store costs nothing. Telemetry fires at **operation granularity** (flush, merge,
fetch, RPC, WAL) — never per sample or row. Logging is context-plumbed via `go-faster/sdk/zctx`,
so lines carry the active span. `query/profile` is opt-in EXPLAIN ANALYZE: a per-query timing
tree that grafts a peer's subtree across the cluster read RPC.

### Transport reliability (`internal/retry`, `reliability`)

The two transports leaving the process (cluster RPC, S3) retry with per-attempt timeouts and
jittered backoff; **idempotent reads hedge** across replicas, **writes stay at-most-once**
(retried only when the request provably never reached the server, never hedged).
`reliability.RetryConfig` is the public knob (`Default`, `LossyEnvironment`).

---

## 4. Cross-cutting invariants

- **Zero-alloc hot paths.** Append-style codec APIs (`func(dst []byte, …) []byte`), caller-owned
  buffers, pooled+`Reset` scratch, decoders returning views that alias the source where the
  lifetime is bounded. `[]byte` (not `string`) for keys/values/identity.
- **One physical engine, many front-ends.** A signal supplies a column schema, a projection and
  (profiles) a side store — nothing more. Storage never learns a language's or signal's concepts;
  matchers and conditions are **callbacks**, never operator enums.
- **Immutable parts + one merge engine.** Compaction, retention, downsampling, recompression,
  precision and EC conversion are all one background merge pass. No parallel subsystem.
- **Backends are interchangeable**; the in-memory/ephemeral path is first-class and every layer
  must work with no disk or object store.
- **Engine locks are never held across object-store I/O.** Plan under lock → read/write off lock →
  publish under lock; parts are copy-on-write and refcounted with deferred reclamation.
- **Coordination is external/minimal.** etcd for membership/claims, backend CAS for commits. No
  homegrown Raft; single-node works with the cluster layer absent.
- **Injected, no-op-default observability** (above).
- **Stable formats.** Golden-tested and version-guarded: the `Codec` enum and per-codec framing,
  part manifest (`OTPM`) / marks (`OTMK`) / column object framing and key layout, the attribute
  hash+binary encoding (the SeriesID pre-image), symbol table (`OTSY`), WAL record framing
  (additive record types), record-key footer (`OTKY`), the metric part column layout
  (`[series:int128, ts:int64, value:float64]` + optional `sf:float64`), and the profile
  symbol-store sidecar (`OTSP`). Changing one is an architectural change.

### Testing discipline

≥90% coverage, fuzz targets for every codec/parser/format, property tests for invariants, golden
files for on-disk formats, benchmarks on hot paths. `go test ./...`, `go vet ./...`,
`golangci-lint run ./...` stay green. `golden_bench_test.go` is the deterministic perf set the
per-PR benchstat workflow diffs against base; `efficiency_test.go` holds hard regression gates.
Distributed safety is covered by fault-injection chaos tests (`cluster/chaos_test.go`,
`cluster/rebalance_chaos_test.go`, `cluster/etcd/ownership_chaos_test.go`, `cluster_ec_chaos_test.go`).

---

## 5. Package map

```
.                     facade: Storage, Options, per-tenant engines, maintenance loop, admission,
                      cluster mode, Inspect/Admin/introspection
encoding/{bitstream,chunk,compress}   bit stream · column codecs · block compression
pool/                 ByteIntMap (xxh3) for dict building
signal/               identity model (Value/KeyValue/Attributes/SeriesID) + Signal/TenantID/Aggregation
  signal/{metric,log,trace,profile}   per-signal ingest batch + projection
otlp/pdataconv        optional OTel-Go bridge (only package importing pdata)
tenant/               policy model + Resolver
backend/{,file,s3,bucketindex,backendtest}   L1 seam, implementations, part index, conformance suite
block/                immutable columnar part format
index/{symbols,series,postings,bloom}        identity index + inverted index + token blooms
wal/                  segmented CRC-framed WAL
engine/               metrics vertical
recordengine/         shared record engine (logs/traces/profiles)
query/{fetch,scale,profile,promql}           read seam · scale-out decorators · EXPLAIN ANALYZE · Prom adapter
cluster/{,ring,etcd,replica,rebalance,partsync,ec}   L0 distribution
internal/{obs,retry,simd,parallel,cmd/gensimd}       injected observability · reliability · AVX2 kernels · fan-out
reliability/          public RetryConfig presets
```
