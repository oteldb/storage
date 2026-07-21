# Admin & Observability

This is the operator-facing surface of the storage library: how to **observe** a running store
(stats, cardinality, part layout, metrics, traces) and how to **act** on it (flush, compact,
retention, rebalance). It documents what exists today; keep it current with any change to that
surface (see the rule in `CLAUDE.md`).

The library is **embedded, not a server.** It exposes data and control through Go methods on the
`Storage` facade; the embedder (e.g. `go-faster/oteldb`) owns any HTTP/CLI/dashboard UI built on top.
The one exception is the cluster transport (`cluster/replica`), which is node-to-node, not operator-facing.

Everything here keyed by tenant takes the **engine key**: the tenant id in the default layout, or a
metric shard key (`{tenant}/_s{idx}`) when `Options.Cluster` sets `ShardsPerTenant > 1`. An empty
tenant normalizes to `"default"`.

## Observe

### `Storage.Inspect() StoreStats` — store-wide snapshot (`inspect.go`)

A pull-based, **in-memory** snapshot for a dashboard: it does **no backend I/O and decodes nothing**,
taking only a brief per-engine read lock to copy counters — safe to poll at dashboard cadence
(seconds), never on a per-request path.

- `StoreStats.Tenants` — per tenant: cumulative `Admission` tally, and per-signal `SignalStats`.
- `StoreStats.Cluster` — cluster mode only (nil single-node): this node's address, live membership,
  owned shards, and the last enacted rebalance plan (`LastRebalance`: each changed shard's full
  owner-set diff at its per-tenant replication factor — the replicas that must backfill, not just
  the compaction-primary move). With a private (per-node) backend
  (`cluster.Config.PrivateBackend`), `Cluster.PartSync` additionally reports the shared-nothing
  part-mirroring activity (nil otherwise): cumulative `Passes` (every sync attempt — the
  "is the sync loop running?" probe), `Mirrored` (passes that installed a newer peer copy),
  `Copied`/`CopiedBytes` (objects fetched from peers), `Pruned` (stale local objects deleted after
  the quarantine delay), `Errors` (failed passes, retried next tick), and `LastSyncUnixNano` (when
  the last mirroring pass completed — the replication-staleness probe).
- `StoreStats.Caches` — read-path decode-cache totals (hits/misses/bytes and `Items` = cached
  decoded **blocks**, the cache being keyed by `(part, column, block)`).

`SignalStats` (one per `(tenant, signal)`):

| Field | Meaning |
|------|---------|
| `Series` | distinct series/streams ever seen (head ∪ flushed) |
| `HeadItems` / `HeadBytes` | unflushed samples/records and their in-flight bytes |
| `Parts` | flushed immutable part count |
| `MinTimeUnixNano` / `MaxTimeUnixNano` | data time span (min over parts; max includes the head) |
| `MergeRunning` | a compaction is executing on this engine right now |
| `MergeBacklog` | parts pending compaction (the backlog proxy; currently equals `Parts`) |
| `WAL` | the engine has a write-ahead log (false for the ephemeral in-memory engine) |
| `WALSegments` / `WALBytes` | WAL segment sequence number and open-segment byte size |
| `WALEpoch` | WAL active flush generation (not the recovery watermark) |

Part *byte* sizes are intentionally omitted from `Inspect` (they would need backend stat calls) — use
`PartsDetailed` for those.

### Drill-down per `(tenant, signal)` (`introspect.go`)

- **`Parts(tenant, signal) []PartInfo`** — one entry per flushed part: `ID` (key prefix), time
  bounds, `Series`, `Rows`. In-memory, no backend I/O — safe to poll.
- **`PartsDetailed(ctx, tenant, signal) ([]PartDetail, error)`** — augments each part with `Bytes`
  (summed backend object sizes), `Chunks` (sparse-index granules), and `Columns` (`Name`, `Kind`,
  `Codec`, `Compress`). Reads object sizes from the backend, so call it for a drill-down view, not a
  high-frequency poll; each part is ref-held for the read so a concurrent merge cannot reclaim it.
  Returns `nil` (no error) when the tenant has no engine for the signal.
- **`Cardinality(tenant, signal, topN) CardinalityStats`** — the first stop for a
  cardinality-explosion incident. `TotalSeries`, `DistinctLabelNames`, `SymbolCount` (interned
  symbols), and `TopLabelNames` (the top-N label names by series count, each with `Series` and
  `DistinctValues`). `topN ≤ 0` returns every label name. Computed from the head's inverted index
  (which spans head ∪ flushed series); no backend I/O.

### `Storage.AdmissionStats(tenant) AdmissionStats` (`admission.go`)

Per-tenant cumulative admission tally (shared across signals — the valves are keyed by tenant):
`Accepted`, `RejectedOOO`, `RejectedRate`, `RejectedCardinality`, `RejectedInFlight`,
`SampledDropped`, `Overflowed`, plus the `Rejected()` total. Drives "why is this tenant being shed?".

### Injected metrics / traces / logs (`internal/obs`)

Observability is **injected, never owned**: pass `Logger`, `TracerProvider`, `MeterProvider` via
`Options`; each defaults to a no-op, so an unconfigured store emits nothing at zero overhead. The
library imports only the OTel **API** — the embedder owns the SDK and exporters.

Metric instruments (all prefixed `storage.`):

| Instrument | Tags | Notes |
|-----------|------|-------|
| `ingest.accepted` / `ingest.rejected` | `signal`(, `reason`) | reasons: `out_of_order`, `rate_limit`, `max_series`, `max_in_flight_bytes` |
| `ingest.sampled_dropped` / `ingest.overflowed` | `signal` | budgeted sampling / overflow routing |
| `flush.total` / `flush.duration` / `flush.rows` | `signal` | head flushes |
| `merge.total` / `merge.duration` / `merge.parts_in` | `signal` | background merges |
| `fetch.total` / `fetch.duration` / `fetch.series_matched` / `fetch.rows_returned` / `fetch.parts_scanned` | `signal` | reads |
| `backend.ops` / `backend.bytes` / `backend.latency` | `op`(, `result`) | ops: read/write/list/delete/cas/size; results: ok/not_found/error |
| `rpc.attempts` / `rpc.retries` / `rpc.hedges` | `op` | cluster RPCs |
| `wal.appends` / `wal.fsyncs` / `wal.rotations` | — | WAL activity |

Tracing emits coarse spans (`engine.flush`, `engine.merge`, `engine.fetch`, backend ops, cluster
RPCs) with W3C trace-context propagation across the cluster transport. Logs are context-plumbed via
`go-faster/sdk/zctx` (trace-correlated); admission shed events log at Warn only when rejections occur.

**EXPLAIN ANALYZE** (`query/profile`): `profile.WithCollector(ctx)` opts a single query into a
per-operator timing tree; distributed reads graft each peer's subtree under a `remote {addr}` node.

## Act

### `Storage.Admin() Admin` (`admin.go`)

Imperative operator control, complementing the background maintenance loop (it holds no state):

- `Flush(ctx, key, signal)` — drain a head to an immutable part now (no-op if nothing ingested).
- `Compact(ctx, key, signal)` — merge a signal's parts now, applying the tenant's resolved policy
  (retention cutoff, plus downsampling/recompression/precision for metrics). The same merge engine
  the loop runs — no parallel path.
- `Retention(ctx, key)` — compact every signal for a tenant (drops parts past the policy cutoff).
- `Rebalance(ctx)` — reconcile cluster ownership immediately (no-op single-node).
- `MaintainNow(ctx)` — run one full maintenance cycle (flush + merge + retention across owned engines).

In **cluster mode**, `Flush`/`Compact` act only on shards this node is the ring-primary of, returning
`ErrNotOwner` otherwise — so a shard's parts are still written by exactly one node, the invariant the
maintenance loop preserves. Single-node owns everything.

## Backend object sizing (`backend.Sizer`)

`PartsDetailed` needs per-object byte sizes. The `backend.Backend` seam exposes none directly; the
optional `Sizer` capability (`Size(ctx, key) (int64, error)`) does. Use `backend.SizeOf(ctx, b, key)`:
it takes the `Sizer` fast path when available and falls back to a full `Read` otherwise. Memory and
file backends implement `Size` cheaply (in-RAM length / `os.Stat`); the cache and instrumentation
wrappers delegate; s3 currently uses the Read fallback (a future optimization can add a `HeadObject`
size path).
