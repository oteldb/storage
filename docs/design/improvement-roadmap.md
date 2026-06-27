# Improvement roadmap — introspection, control, multi-tenancy, wire-ups

**Status:** partially implemented. Grounded in `ARCHITECTURE.md` (as-built) and the code surface.

Implementation status (recommended order 4 → 1 → 3a → 2 → 3b/3c):
- **Track 4a–4d — DONE** (rebalance.Plan wired; clustered admission; Log/Trace enumeration fan-out;
  WAL-durable scale factor). Folded into `ARCHITECTURE.md`.
- **Track 4e — DEFERRED** (MaxPartSize needs part-splitting; lz4 needs a new dependency — discuss per
  CLAUDE.md; SIMD expansion is large codegen). Follow-ups, not done.
- **Track 1 (`Storage.Inspect`) — DONE.** Folded into `ARCHITECTURE.md`.
- **Track 2 (`Storage.Admin`) — DONE.** Folded into `ARCHITECTURE.md`.
- **Track 3a (cardinality budget + overflow) — DESIGNED** (`docs/design/cardinality-overflow.md`);
  awaiting go-ahead. Hysteresis dropped (the head's series index is monotonic).
- **Track 3b (per-signal record sharding) — DESIGNED** (`docs/design/record-sharding.md`); awaiting
  go-ahead.
- **Track 3c (fair maintenance scheduling) — DONE.** Folded into `ARCHITECTURE.md`.

The sections below are the original proposal, kept for context. When a track is implemented, fold its
outcome into `ARCHITECTURE.md`.

---

## Context: where we actually are

The library is well past greenfield. M0–M7 (encoding, parts, index/WAL, single-node engine,
PromQL adapter, S3/object-store-native, cluster, query scale-out), all four signals
(metrics + logs/traces/profiles), observability (logs/traces/metrics + EXPLAIN ANALYZE),
admission control, budgeted sampling, hedged cluster reads, and per-series sharding are
**built and tested**. So the high-value work is no longer "build a layer" — it is:

1. **Introspection** — surface the state the engines already hold as a pull-based snapshot
   API (the dashboard/CLI story). *Biggest genuine gap.*
2. **Control** — an imperative admin facade for on-demand flush/compact/rebalance/retention.
3. **Multi-tenancy** — cardinality budget + overflow, per-signal sharding, fair maintenance.
4. **Wire-ups** — close built-but-unwired edges (rebalance plan, clustered admission, log
   enumeration fan-out, sampled-WAL).

Recommended sequencing: **4 → 1 → 3-isolation → 2 → 3-rest**. The wire-ups close correctness
holes cheaply and unblock #2; introspection is the foundation a CLI/UI *and* our own
debugging both need; it is also low-risk aggregation over existing methods.

Cross-cutting invariants every track must preserve (from `ARCHITECTURE.md` §5): zero-alloc
hot paths (telemetry/introspection at operation granularity only, never per-sample);
in-memory-first (every feature works with no disk/S3); injected no-op observability; library
not binary (we expose Go structs/methods, never a UI or process); language-agnostic storage
seam.

---

## Track 1 — Introspection / exportable statistics (`Storage.Inspect`)

### Goal
A single pull-based API returning a structured snapshot of store state, suitable for an
embedder's CLI/UI dashboard and for our own debugging. No new mechanism — this aggregates and
serializes state the engines already hold.

### Current state
Introspection is scattered and thin: `Storage.AdmissionStats(tenant)` (per-tenant admission
tally), `engine.DecodeCacheStats()` (engine-internal, not surfaced on the facade), and the
OTel instruments in `internal/obs` (only visible if the embedder wired a meter). There is **no
"describe the whole store" call** and no way to enumerate tenants/engines/parts.

The raw material already exists per engine:
- `engine.Engine`: `HeadBytes()`, `HeadSampleCount()`, `SeriesCount()`, `PartCount()`,
  `DecodeCacheStats()`.
- `recordengine.Engine`: `HeadBytes()`, `HeadRecordCount()`, `StreamCount()`, `PartCount()`.
- Part time-bounds & sizes: `backend/bucketindex.Index.Overlapping(start,end)` enumerates
  parts with their windows; part objects know their byte size.
- Cluster: `cluster.Membership.Ring()`, `.Members()`, `.AddrOf(id)`, `.LeaseID()`;
  `ring.Ring.Nodes()`, `.Lookup(key, rf)`; `Ownership` knows the claimed shard set.
- Caches: `engine.DecodeCacheStats()`, and the `query/scale` `MemoryCache` (hit/miss).

### Proposed API
A new `inspect.go` in the root package. All types are plain serializable Go structs
(JSON-tag them so an embedder can expose them directly).

```go
// Inspect returns a point-in-time snapshot of store state. Pull-based, computed off the
// hot path (one locked read per engine to copy counters — never per-sample). Safe to call
// concurrently with ingest/query.
func (s *Storage) Inspect(ctx context.Context) StoreStats

type StoreStats struct {
    Tenants []TenantStats
    Cluster *ClusterStats // nil in single-node mode
    Caches  CacheStats
    Uptime  time.Duration
}

type TenantStats struct {
    Tenant  signal.TenantID
    Signals []SignalStats // one per signal present for this tenant
}

type SignalStats struct {
    Signal       signal.Signal
    Series       int64 // SeriesCount / StreamCount
    HeadItems    int64 // HeadSampleCount / HeadRecordCount
    HeadBytes    int64
    PartCount    int
    PartBytes    int64
    OldestTSUnixNano, NewestTSUnixNano int64
    BytesPerPoint float64 // PartBytes / total points — the density KPI
    MergeBacklog  int     // parts above the next compaction tier's fan-in (see note)
    Admission     AdmissionStats // reuse existing type, for metrics; zero for others
}

type ClusterStats struct {
    Self      string   // node id
    Members   []MemberStats
    Shards    []ShardStats // per shard: owners, isLocalPrimary, isClaimed
    RebalancePending []ShardMove // from rebalance.Plan once wired (Track 4)
}
type MemberStats struct { ID, Addr, Zone string; Alive bool }
type ShardStats struct { Key string; Owners []string; LocalPrimary, Claimed bool }

type CacheStats struct {
    Decode engine.DecodeCacheStats // aggregated across engines
    Query  QueryCacheStats         // from query/scale MemoryCache
}
```

### Implementation notes
- Add small read-only accessors where missing: part byte total + min/max ts per engine
  (derive from `bucketindex` + in-head min/max). Most counters already exist.
- `Inspect` walks `engineSnapshot()` / `*EngineSnapshot()` (these helpers already exist) and
  copies counters under each engine's lock — **no fetch, no decode**. This is the same
  discipline as the OTel instruments: operation/object granularity, never per-row.
- `MergeBacklog`: expose the count of parts the merge policy would still want to compact (the
  merge engine already computes tiering in `compactParts`; surface the pre-merge candidate
  count). This is the single most useful "is compaction keeping up?" signal.
- Cluster section only populated when `Options.Cluster` is set; uses `Membership`/`Ownership`
  accessors already present.
- **Optionally** also register these as OTel gauges via `internal/obs` (observable callbacks),
  so an embedder with a meter gets them as metrics for free — but `Inspect()` is the
  primary, meter-independent surface (works with no observability configured).

### Testing
- In-memory store: ingest known data, assert counts/bytes/ts-bounds match.
- Property: `sum(SignalStats.Series)` equals distinct series ingested; `PartCount` matches
  after forced flush/merge.
- Cluster: 2–3 node embedded-etcd test asserts `ClusterStats.Shards` owners == ring lookup.
- Zero-alloc/perf: assert `Inspect` does no part decode (no I/O counters move).

### Risks / notes
- Don't let `Inspect` become a hot-path drag if an embedder polls it tightly — it takes
  per-engine locks. Document "snapshot, poll at dashboard cadence (seconds), not per request."

---

## Track 2 — On-demand control APIs (`Storage.Admin`)

### Goal
Imperative operator control to complement the time-driven maintenance loop and ring-driven
reconciliation. This is exactly what an operator CLI needs.

### Current state
Everything is automatic: `runMaintenance(interval)` → `maintain()` flushes+merges+retention
on `FlushInterval`; `Ownership.Reconcile` runs from the ring. The engine methods to do these
on demand **exist** (`Engine.Flush`, `Engine.Merge`/`MergeWith`, `recordengine` equivalents,
`Ownership.Reconcile`) — there is just no public, per-tenant trigger.

### Proposed API
Keep the core facade clean; put control behind one accessor:

```go
func (s *Storage) Admin() Admin
type Admin struct { s *Storage }

// Force a flush of the named tenant+signal head to a part now.
func (a Admin) Flush(ctx context.Context, t signal.TenantID, sig signal.Signal) error
// Force a compaction/merge (applies retention+downsampling policy) now.
func (a Admin) Compact(ctx context.Context, t signal.TenantID, sig signal.Signal) error
// Force a retention sweep for a tenant (drop parts older than policy).
func (a Admin) Retention(ctx context.Context, t signal.TenantID) error
// Trigger ownership reconciliation immediately (cluster mode); no-op single-node.
func (a Admin) Rebalance(ctx context.Context) error
// Flush+compact every tenant/signal now (the maintain() body, on demand).
func (a Admin) MaintainNow(ctx context.Context) error
```

### Implementation notes
- Each method resolves the per-tenant engine via the existing `*EngineFor`/`lookup*Engine`
  helpers and calls the existing `Flush`/`MergeWith`/`Reconcile`. `Compact` must resolve the
  same `metricMergeOptions(tid)` the loop uses, so on-demand compaction applies identical
  retention/downsampling/recompression policy (no divergent code path — preserves the
  "one merge engine" invariant).
- Must be safe to call concurrently with the background loop: both go through the engine lock;
  a redundant flush of an empty head is a no-op. No new coordination needed single-node.
- In cluster mode, `Compact`/`Flush` must respect compaction-claim ownership (only act on
  owned shards, or return `ErrNotOwner`) so two nodes don't both compact a shard — reuse the
  `ownedTenants` gate already used by `maintain()`.
- `Rebalance` calls `Ownership.Reconcile(ring, shards)` immediately (and, once Track 4 lands,
  drives it from `rebalance.Plan`).

### Testing
- In-memory: write → `Admin().Flush` → assert `PartCount` increments, head drains.
- `Admin().Compact` reduces part count and applies downsampling tier (reuse merge tests).
- Cluster: `Admin().Compact` on a non-owner returns `ErrNotOwner`; on owner succeeds.
- Concurrency: hammer `Admin().MaintainNow` alongside the background loop under `-race`.

### Risks
- Surface creep: keep `Admin` minimal and operational; resist turning it into a second
  write/query path.

---

## Track 3 — Multi-tenancy

Three sub-items, independently shippable. (a) is the most valuable (a real stability gap
under cardinality spikes); (b) and (c) improve isolation/scaling.

### 3a. Cardinality budget with hysteresis + `__overflow__` routing
**Current:** `Limits.MaxSeries` is a *hard reject* in `head.appendByID` — a sample minting a
new series past the cap is shed (ARCHITECTURE §3k). A cardinality spike makes a tenant lose
*new* data abruptly with no soft landing.

**Proposed:** turn the cap into a soft budget (Mimir/StatsHouse-style):
- Add `Limits.MaxSeriesSoft` / hysteresis band: above the soft line, route *new* series'
  samples into a synthetic `__overflow__` series (per metric name) instead of rejecting, so
  aggregates stay roughly correct and the tenant stays queryable.
- Hysteresis: stop overflowing only once cardinality drops a margin below the soft line, to
  avoid flapping.
- Enforcement stays **in the head** (race-free under the engine lock), where the existing cap
  check lives — extend `engine.AppendLimits` with the soft band; the engine stays
  policy-agnostic (numbers only). The facade resolves the band from `tenant.Limits`.
- Report overflowed points distinctly in `AdmissionStats` (new `Overflowed` counter) and the
  OTel meta-metrics.

**Testing:** property test — under a series flood, total admitted points (real + overflow)
stays > hard-reject baseline; overflow stops after cardinality recovers (hysteresis); merge
treats `__overflow__` like any series.

### 3b. Per-signal sharding (extend `ShardsPerTenant` to records)
**Current:** per-series sharding (`Config.ShardsPerTenant`) is **metrics-only**; logs/traces/
profiles pin a big tenant to one owner set (`Ring().Primary(tenant)`), ARCHITECTURE §3i.

**Proposed:** generalize the shard-key machinery (`{tenant}/_s{idx}`, collapses to bare tenant
at N=1) to the record engines. The write path already groups+routes per shard for metrics
(`writeMetricsClustered`); replicate that grouping in `writeRecordsClustered`, and gather
across shards in `clusterRecordFetcherFor` (mirror `clusterFetcherFor`). Stream→shard map is
`hash(streamID) % N`, same as metrics. Compaction stays one-owner-per-shard.

**Risk:** trace-by-id and profile symbol-store fan-out must gather across all shards (a
trace's spans / a stack's symbols may land on different shards) — verify `Trace()` and
`ProfileResolver` fan across shards, not just tenants.

**Testing:** N>1 cluster test — a single tenant's logs scatter across shards and a query on
any node returns the full set; trace-by-id reassembles across shards.

### 3c. Fair maintenance scheduling
**Current:** `maintain()` fans flush/merge out concurrently under a bound
(`WithMaintenanceConcurrency`) but with **no per-tenant priority** — a noisy tenant's
compaction backlog can monopolize the worker pool and starve others.

**Proposed:** order the maintenance work-list by need (largest head bytes / deepest merge
backlog first) and/or round-robin across tenants so no single tenant hogs all workers within a
tick. Cheap: it's a sort/interleave of the existing per-engine work-list before the
`internal/parallel` fan-out. Surface backlog via Track 1's `MergeBacklog` so the policy is
observable.

**Testing:** with a skewed load (one huge tenant + many small), assert small tenants still get
flushed within a bounded number of ticks (no starvation).

---

## Track 4 — Wire up built-but-unwired edges (do first)

Cheap, high-ROI, closes real correctness holes. Each has the hard part already built.

### 4a. Consume `rebalance.Plan` in the executor
`cluster/rebalance.Plan(shards, prev, next, rf)` computes the minimal one-in/one-out ownership
diff and is fully tested, but `etcd/ownership.go:Reconcile` ignores it and re-derives from the
ring (ARCHITECTURE §3i/§3j: "the minimal-move diff is currently informational only").
**Wire it:** on a membership change, compute `Plan(prevRing, nextRing)` and reconcile only the
shards that moved, instead of acquiring/releasing the entire owned set. Event-driven, less
etcd churn, and it gives `Admin().Rebalance` + `Inspect().Cluster.RebalancePending` real data.

### 4b. Admission on the clustered write path
**Current:** the cluster primary only does the OOO check (`ApplyPrimary`); rate / cardinality /
in-flight valves and budgeted sampling are **single-node only** (ARCHITECTURE §3k). A clustered
tenant has *no overload protection* — a real stability gap.
**Wire it:** run the facade admission stage on the primary before `ApplyPrimary` (the primary
is the single authority for the shard, so it's the right place to meter rate/cardinality). The
reject/sampled counts already flow back into `Accepted` — extend that to carry the admission
reasons over the primary-write RPC.

### 4c. Cluster fan-out for `LogSeries` / `LogKeys`
**Current:** both are **node-local** (ARCHITECTURE §4: "cluster fan-out is a follow-up"), so on
a non-owner they return wrong/empty enumeration. `ProfileSeries`/`ProfileResolver` already fan
out via `cluster/enum.go` — **copy that pattern** for log series/keys (re-apply non-equality
matchers to the superset, hedge across owners).

### 4d. WAL-persist the scale factor
**Current:** a crash recovers unflushed *sampled* data as weight 1 (ARCHITECTURE §3k) — a
correctness window in the unbiased-sampling path.
**Wire it:** add the `sf` value to the WAL sample frame (it already exists as the optional 4th
part column), so replay restores the representative weight. Format change → bump WAL version,
add golden + round-trip fuzz.

### 4e. Smaller items
- Enforce `MaxPartSize` at flush/merge (defined, not enforced).
- `compress/lz4` is a stub — either implement or remove the seam.
- Extend SIMD kernels beyond `int64 min/max` (the codec hot paths are the payoff; always with
  pure-Go fallback behind arch build tags, per CLAUDE.md).

---

## Cross-track testing & rollout discipline
- Every track ships with ≥90% coverage in the same change (CLAUDE.md).
- Format changes (4d) get golden + fuzz round-trip and a version bump.
- New public surface (`Inspect`, `Admin`) documented in `ARCHITECTURE.md` §4 when landed.
- Bench gate: confirm `Inspect`/`Admin` add no hot-path allocs (`efficiency_test.go` ceilings
  unchanged; `Inspect` runs zero part decodes).
```
