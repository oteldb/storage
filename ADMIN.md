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
  the last mirroring pass completed — the replication-staleness probe). `Cluster.EC` (same
  gating) reports the erasure-coding activity, cumulative: `Converted`/`ConvertErrors` (cold parts
  coded by this node as compaction owner), `RepairedSlots`/`RepairErrors` (shard slots rebuilt
  after a loss), `PrunedStagedParts` (staged shards dropped after distribution — each one converges
  a part to one shard per node), and `Reconstructs`/`ReconstructErrors` (read-path object
  reconstructions; a high reconstruct rate on cold reads is expected, growing errors are not).
- `StoreStats.Caches` — read-path decode-cache totals (hits/misses/bytes and `Items` = cached
  decoded **blocks**, the cache being keyed by `(part, column, block)`).
- `StoreStats.Maintenance` — the background maintenance loop: cumulative `Cycles` (the loop-
  liveness probe), `LastCycleStartUnixNano` / `LastCycleDurationNano` (a growing duration means
  compaction is falling behind ingest), `LastCycleTasks` (engine tasks dispatched in the most
  recent cycle), and `PressureFlushes` (engines flushed by the head-size trigger
  `Options.FlushThresholdBytes` rather than by the interval — a rising count means ingestion is
  filling heads faster than the flush cadence drains them).

`SignalStats` (one per `(tenant, signal)`):

| Field | Meaning |
|------|---------|
| `Series` | distinct series/streams ever seen (head ∪ flushed) |
| `HeadItems` / `HeadBytes` | unflushed samples/records and their in-flight bytes. For a record engine `HeadBytes` also counts the buffers an in-flight flush has detached but not yet published — they stay resident, and they are what `MaxInFlightBytes` and `FlushThresholdBytes` meter |
| `IdentityBytes` | resident identity state: the symbol table, the series/stream index, the postings lists and the per-series out-of-order watermarks. A flush does **not** drain it — identities outlive the data they named and are reclaimed only by `Reset` — so it is reported beside `HeadBytes` rather than folded into it (`MaxInFlightBytes` / `FlushThresholdBytes` would otherwise chase a number a flush cannot lower). It falls when the background maintenance prunes the identities retention left without data — on every node holding metric parts, owner or replica, since a part carries its own identities — so an unbounded rise means churn is outpacing retention |
| `Parts` | flushed immutable part count |
| `MinTimeUnixNano` / `MaxTimeUnixNano` | data time span (min over parts; max includes the head) |
| `MergeRunning` | a compaction is executing on this engine right now |
| `SealedParts` | parts already at the merge cap. A merge never reconsiders them, so this is the share of `Parts` no compaction will reduce |
| `MergeBacklog` | parts a merge may still take (`Parts − SealedParts`) — the backlog in the literal sense, not the part count |
| `MergeCandidates` | parts the **next** merge would select. `0` with a non-zero `MergeBacklog` is the stuck state a maintenance cycle cannot fix by itself; `Admin.CompactNow` is the override |
| `MergeCapBytes` | the seal threshold in effect. Derived per merge for metrics (from free space and the merge memory allowance), so it reads `0` until that engine's first merge; a static function of configuration for the record signals |
| `WAL` | the engine has a write-ahead log (false for the ephemeral in-memory engine) |
| `WALSegments` / `WALBytes` | WAL segment sequence number and open-segment byte size |
| `WALEpoch` | WAL active flush generation (not the recovery watermark) |
| `HasReadGap` / `ReadGapAfterUnixNano` | this node holds the shard but came back without its unflushed head (a restart, or a rebalance handing it over), so it disclaims reads at or past that timestamp and lets another owner answer. Clears when a flush by the shard's compaction owner puts newer data in this node's parts. `math.MinInt64` means the shard had no parts at all — nothing here is known to be complete |

Part *byte* sizes are intentionally omitted from `Inspect` (they would need backend stat calls) — use
`PartsDetailed` for those.

**Diagnosing a part count that never falls.** `Parts` alone cannot tell a healthy idle engine from
one wedged at a fixed point — a deployment sat at 59 parts for thousands of cycles looking exactly
like an idle one. The three numbers beside it answer it directly:

- `SealedParts == Parts` — everything is at the cap; the count is the floor and nothing is wrong.
- `MergeBacklog > 0`, `MergeCandidates > 0` — a merge is coming on the next cycle.
- `MergeBacklog > 0`, `MergeCandidates == 0` — **stuck**: parts remain mergeable but none qualify.
  The metric engine waives its write-amplification guard after a few idle cycles and unwedges
  itself; the record engines do not. `Admin.CompactNow` breaks it either way.

Each engine's own `MergeShape()` (`engine`, `recordengine`) carries the rest of the selector's
inputs behind these fields — the metric engine's `BestMultiplier`/`MinMultiplier`/`IdleRounds`/
`WaiveAfter`, the record engines' `Tiers`/`LargestTierParts`/`MinTierParts`. They are per-engine
because the two selectors reason differently: the metric engine orders parts by size and scores runs
(it has had no size tiers since the tiering that stranded parts was removed), while the record
engines still bucket by tier and wait for `MinTierParts` of them.

### Drill-down per `(tenant, signal)` (`introspect.go`)

- **`Parts(tenant, signal) []PartInfo`** — one entry per flushed part: `ID` (key prefix), time
  bounds, `Series`, `Rows`. In-memory, no backend I/O — safe to poll. (Each engine's own `PartStat`
  additionally carries `SizeBytes`, the figure that engine's merge cap compares against, and so the
  one that explains why a part is or is not sealed. It is deliberately **not** on the cross-signal
  `PartInfo`: the two are different quantities under one name — `engine.PartStat.SizeBytes` is the
  part's size *on disk*, since the metric merge is bounded by what it writes, while
  `recordengine.PartStat.SizeBytes` is its *decoded* footprint, since the record merge is bounded by
  what it holds.)
- **`PartsDetailed(ctx, tenant, signal) ([]PartDetail, error)`** — augments each part with `Bytes`
  (summed backend object sizes), `Chunks` (sparse-index granules), and `Columns` (`Name`, `Kind`,
  `Codec`, `Compress`, `Level` — the compression level, which for merged metric parts climbs a
  size-graduated ladder). Reads object sizes from the backend, so call it for a drill-down view, not a
  high-frequency poll; each part is ref-held for the read so a concurrent merge cannot reclaim it.
  Returns `nil` (no error) when the tenant has no engine for the signal.
- **`Cardinality(tenant, signal, topN) CardinalityStats`** — the first stop for a
  cardinality-explosion incident. `TotalSeries`, `DistinctLabelNames`, `SymbolCount` (interned
  symbols), and `TopLabelNames` (the top-N label names by series count, each with `Series` and
  `DistinctValues`). `topN ≤ 0` returns every label name. Computed from the head's inverted index
  (which spans head ∪ flushed series); no backend I/O.
- **`StreamCosts(ctx, tenant, signal, StreamCostOptions) ([]StreamCost, error)`** (`introspect.go`,
  record signals only) — the "**which service is costing me, and why**" drill-down: the flushed
  parts attributed to streams, or (with `GroupBy`, e.g. `service.name`) to a label's values.
  `Cardinality` reports *label* cardinality and `PartsDetailed` reports *per-part* layout; neither
  attributes bytes to a stream, nor says anything about the cardinality of the values inside
  `body`/`attrs`, which is what drives the cost. Per group: `Streams`, `Rows`, `RawBytes`,
  `DiskBytes`, and per column `RawBytes` / `DiskBytes` / `Distinct` / `DistinctNormalized`. Sorted
  by `RawBytes` descending; `TopN` truncates. Errors for `signal.Metric` (no per-record columns);
  returns nil for a tenant with no engine.

  - **`DistinctNormalized` is the field to read first.** It is `Distinct` over values with every run
    of ASCII digits collapsed to `#`. A group whose `Distinct` is large and whose
    `DistinctNormalized` is tiny is *mis-parsed at the source*, not expensive by nature — one
    templated line with an embedded timestamp or id, never turned into fields. On a real corpus one
    service read as 4,564,470 distinct bodies and 184 normalized templates: nothing to fix in
    storage, everything to fix in the pipeline. Both are HyperLogLog estimates from the same sketch
    the bloom builder uses to size its filters; measured against exact counts on that service's
    4,513,535 distinct bodies the estimate was **−2.4%**, and on its 184 templates **−0.5%**.
    `RawBytes` is not an estimate — it matched the exact byte count to the byte.
  - **`DiskBytes` is APPROXIMATE, by construction.** Compression is per column per frame and a frame
    spans whatever streams its rows fall in, so a stream's compressed footprint is not directly
    measurable — an exact number would mean compressing each stream separately, which would cost
    most of the ratio. Each frame's compressed size is apportioned across the groups holding its
    rows by their raw-byte share. Rows are `(stream, ts)`-ordered, so most frames hold one stream
    and the estimate is close; a group narrower than one frame is where it is not. `RawBytes` (the
    decoded footprint) is exact.
  - **Cost.** This is the heaviest introspection call in the library: every accounted byte column of
    every live part is read and decoded once (int columns and the timestamp are accounted
    arithmetically — a row is a fixed width there — so they cost nothing). It runs on operator
    demand, never on a schedule, and `Columns` narrows the decode to the columns in question.
    Nothing is accumulated on the ingest or merge path, deliberately: measured on real log bodies
    the per-row sketch work costs 350 ns against a 2288 ns/row record merge — **+15% on the merge,
    for one column**, on top of the ~21% bloom construction already takes. The trade is that the report costs a
    scan, and that it covers **flushed parts only** — the head holds no compressed bytes to
    attribute. Measured on a real 303M-row / 135 GiB-decoded log store (665 parts, 2256 services):
    **2m58s single-threaded**, ~8 GiB resident.
  - **Distinct estimates are budgeted.** `MaxSketchGroups` (default 4096, 8 KiB of sketch per group)
    bounds how many groups carry them; the budget goes to the groups with the most rows, which is
    free to rank because row counts come from the parts' in-memory row-range index. A group outside
    it reports `DistinctEstimated == false` and full byte attribution — the counts are absent, not
    zero. The cap is there for the pathological case (grouping a million-stream store by raw stream
    id), not to ration memory: a real corpus grouped by a pod-suffixed `service.name` yielded 2256
    groups, 18 MiB of sketch, against the ~8 GiB the decode itself holds.
  - Grouping by label rather than by stream id is the intended use: what an operator can act on is a
    service, and stream identity is a field policy that can change under them. An empty `GroupBy`
    groups by stream id (32 hex digits); a stream lacking the label lands in the empty key.
- **`EfficiencyStats(ctx) ([]TenantEfficiency, error)`** (`efficiency.go`) — the capacity/
  efficiency view: per `(tenant, signal)`, `Series`, `Parts`, `Points` (samples/records),
  `StoredBytes` (this node's on-disk footprint — under erasure coding with slot filtering that is
  the local shard, not the cluster-wide total), `BytesPerPoint` (the per-sample storage cost), and
  for metrics `LogicalBytes` (`Points × 16`) with `CompressionRatio` (logical/stored; 0 for the
  record signals, whose per-record logical size is not recorded). Reads object sizes from the
  backend like `PartsDetailed` — dashboard cadence, not per request.

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
| `fetch.total` / `fetch.duration` / `fetch.series_matched` / `fetch.rows_returned` / `fetch.parts_scanned` | `signal` | reads; the metric engine's reads are streaming, so these are recorded when the iterator is **closed** — `duration` covers the whole iteration (the consumer's own per-batch work included) and `rows_returned` counts what was actually consumed |
| `fetch.decode_budget_forced_admissions` | `signal` | queries admitted **over** the decode-memory ceiling after their wait stalled (see below); a non-zero rate means the ceiling is not holding |
| `backend.ops` / `backend.bytes` / `backend.latency` | `op`(, `result`) | ops: read/write/list/delete/cas/size; results: ok/not_found/error |
| `rpc.attempts` / `rpc.retries` / `rpc.hedges` | `op` | cluster RPCs |
| `rpc.shard_absent` | `op` | shard reads that failed over because an owner holds no data for the shard (a rebalance backfill that has not caught up, or a lagging membership view); a sustained rate means the ring and the data disagree |
| `rpc.shard_incomplete` | `op` | shard reads that failed over because this node holds the shard but is missing the head it held unflushed (see `HasReadGap` above). Expected briefly after a restart or a rebalance; sustained means no owner is flushing the shard |
| `wal.appends` / `wal.fsyncs` / `wal.rotations` | — | WAL activity |
| `parts.total` / `parts.sealed` / `parts.merge_backlog` / `parts.merge_candidates` / `merge.cap_bytes` | `signal` | gauges: the merge selector's view of the parts, published once per maintenance cycle, summed over this node's tenants (`cap_bytes` is the largest threshold in effect, not a sum — it is a threshold, not a quantity). `merge_backlog` flat with `merge_candidates` pinned at 0 is the stuck engine above. Per-tenant detail is `Inspect`, which needs no meter |

Tracing emits coarse spans (`engine.flush`, `engine.merge`, `engine.fetch`, backend ops, cluster
RPCs) with W3C trace-context propagation across the cluster transport. Logs are context-plumbed via
`go-faster/sdk/zctx` (trace-correlated); admission shed events log at Warn only when rejections occur.

**Decode-budget forced admissions.** `Options.DecodeMemoryBytes` caps in-flight decoded bytes; a
query reserves its estimate before reading parts and blocks while it does not fit. That wait is
bounded on purpose: it ends on the query's context, and a waiter that is the queue head for
`engine.DefaultDecodeBudgetForceAfter` without the budget draining is admitted anyway — logged at
Warn (with the estimate, the in-flight bytes and whether the read carried a `fetch.Scope`) and
counted here. A steady rate means either the budget is undersized for the query mix, or an embedder
holds several iterators open per query without passing a `fetch.Scope` — the latter shows up as
`scoped=false` in the log line.

#### Aggregate read spans

`engine.aggregateRange` / `engine.aggregateStep` / `engine.aggregateStepNamed` /
`engine.aggregateWindow` carry the attributes that say **why** an aggregate read was slow, not only
how long it took:

| Attribute | Meaning |
|-----------|---------|
| `storage.series_matched` / `storage.parts_scanned` | plan size |
| `storage.stats_pushdown_reason` | `ok`, `grid_unusable`, `partial_coverage`, `overlapping_parts` |
| `storage.stats_pushdown_parts` | how many sources tripped the reason (0 for `ok`/`grid_unusable`) |
| `storage.samples_decoded` | samples decoded **and** folded — the input quantity that explains the duration |
| `storage.step` / `storage.window` | the requested grid (window form only) |
| `storage.window_grid` | whether the fine-bucket grid was usable (window form only) |
| `storage.series_emitted` / `storage.windows_emitted` / `storage.stopped_early` | outputs, recorded when the iteration ends (window form only) |
| `storage.decode_duration_ms` / `storage.fold_duration_ms` | coarse phase split of the per-series work (window form only) |

`engine.aggregateWindow` also emits one child span, `engine.aggregateWindow.plan`, around matcher
resolution and plan construction. Decode and fold get accumulated durations instead of spans: they
interleave once per series in a streaming, series-major iteration, so neither is a contiguous
interval, and a span per series or per part would put thousands of spans in one query's trace. Both
the clock reads and the child span are skipped entirely when the parent span is not recording.

**Reading `stats_pushdown_reason`.** Anything but `ok` means every matched series decoded its value
column rather than folding the parts' precomputed stats:

- `grid_unusable` — the requested window is not a whole multiple of the step, so a window edge can
  fall inside a fine bucket and no bucket-level shortcut is exact. A query-shape problem: round the
  range to a multiple of the step.
- `partial_coverage` — `storage.stats_pushdown_parts` parts reach outside the query range, so their
  whole-part stats would count out-of-range samples. **This is the one compaction makes worse** — see
  the trade-off note in `engine/ARCH.md` ("Aggregate pushdown"): fewer, larger parts are less likely
  to be contained in a dashboard's window, so a store compacted from 71 parts to 3 can lose pushdown
  eligibility entirely for a 6h query.
- `overlapping_parts` — `storage.stats_pushdown_parts` sources overlap a predecessor in time (the
  head/mid-flush samples count as one such source), so a timestamp could be double-counted. A layout
  property, not a query one; backfill and out-of-order ingest produce it.

**EXPLAIN ANALYZE** (`query/profile`): `profile.WithCollector(ctx)` opts a single query into a
per-operator timing tree; distributed reads graft each peer's subtree under a `remote {addr}` node.

## Act

### `Storage.Admin() Admin` (`admin.go`)

Imperative operator control, complementing the background maintenance loop (it holds no state):

- `Flush(ctx, key, signal)` — drain a head to an immutable part now (no-op if nothing ingested).
- `Compact(ctx, key, signal)` — merge a signal's parts now, applying the tenant's resolved policy
  (retention cutoff, plus downsampling/recompression/precision for metrics). The same merge engine
  the loop runs — no parallel path.
- `CompactNow(ctx, key, signal)` — compact even when the selector would decline: the escape from the
  fixed point above (`MergeBacklog > 0`, `MergeCandidates == 0`), which `Compact`/`MaintainNow`
  cannot break because a cycle that selects nothing is a no-op. It overrides the **selection
  heuristic only** — the seal threshold, the run's cumulative-bytes cap and the merge memory bound
  still apply, so a forced merge reads, writes and holds no more than a background one, and never
  takes a sealed part. One call compacts one group; call it again to make further progress.
- `Retention(ctx, key)` — compact every signal for a tenant (drops parts past the policy cutoff).
- `PruneIdentities(ctx, key) (int, error)` — drop the identities retention left without data across
  **every** of a tenant/shard's signals and report how many went in total. The background loop does
  this after anything that changed the part set, but only past its thresholds (a minimum index size,
  and a quarter of it dead), so this forces a sweep now — after a cardinality incident, say — and
  returns the count rather than a silent no-op. Watch `SignalStats.IdentityBytes` for the effect;
  signals with no engine on this node contribute `0`.
- `Rebalance(ctx)` — reconcile cluster ownership immediately (no-op single-node).
- `MaintainNow(ctx)` — run one full maintenance cycle (flush + merge + retention across owned engines).

The retention cutoff both paths pass to the merge is `max(age cutoff, size cutoff)`. The size cutoff
comes from `tenant.Retention.MaxBytes`: the tenant's parts across all signals/shards on this node are
summed and dropped oldest-first until the total fits the budget (`retention.go`). Resolving it reads
per-part object sizes from the backend, like `PartsDetailed` — so a cycle only pays that I/O for
tenants that actually set `MaxBytes`, and the cutoff is resolved once per tenant per cycle. It is
also **memoized against the tenant's part set**: parts are immutable, so the cutoff can only change
when a flush, merge, or drop changes the part set (or the budget itself moves), and a cycle that
follows an idle one does no part enumeration at all. Erasure-coding a part rewrites its stored bytes
under the same identity, so the converter drops the tenant's memo.

In **cluster mode**, `Flush`/`Compact`/`CompactNow` act only on shards this node is the ring-primary of, returning
`ErrNotOwner` otherwise — so a shard's parts are still written by exactly one node, the invariant the
maintenance loop preserves. Single-node owns everything.

## Ranged column reads (`backend.ReaderAt`)

A part stores one object per column. Without ranged reads, touching any block of a column transfers
the whole column, so **read cost was independent of selectivity** and part size bounded process
memory rather than disk: a selector matching 16 of 210k series still paid for all 210k.

`file` and `s3` read ranges natively (`pread`, the `Range` header); `Memory` copies the range. The
block-sliced query path opens a column with `PartReader.ColumnBlocks`, which reads the column's
directory and then only the compression frames holding the matched rows.

Two things an operator should know:

- **The compression frame is the read floor** (`WithCompressBlockBytes`, 64 KiB uncompressed by
  default). A single-series fetch pays one frame per column however few rows it wants, so on a part
  small enough to fit in a frame there is nothing to save.
- **A backend wrapper that hides `backend.Sizer` or `backend.ReaderAt` silently reverts this.**
  `backend.ReadAt` falls back to a whole-object read, and `backend.SizeOf` to reading the object to
  measure it. Both are correct and both are the cost this avoids. The in-tree wrappers (read cache,
  metering) forward them.

## Backend object sizing (`backend.Sizer`)

`PartsDetailed` needs per-object byte sizes. The `backend.Backend` seam exposes none directly; the
optional `Sizer` capability (`Size(ctx, key) (int64, error)`) does. Use `backend.SizeOf(ctx, b, key)`:
it takes the `Sizer` fast path when available and falls back to a full `Read` otherwise. Memory and
file backends implement `Size` cheaply (in-RAM length / `os.Stat`); the cache and instrumentation
wrappers delegate; s3 currently uses the Read fallback (a future optimization can add a `HeadObject`
size path).
