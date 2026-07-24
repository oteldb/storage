# Design: per-signal sharding for record signals (Track 3b)

**Status:** proposal (design only — no code yet). For review before implementation.

Today `Config.ShardsPerTenant` (per-series sharding across the ring) is **metrics-only**
(`cluster/ARCH.md`, "Sharding"). Logs/traces/profiles pin a whole tenant to one owner set
(`Ring().Primary(tenant)`), so one large record tenant cannot scale out. This generalizes the
existing shard-key machinery to the record signals.

## What already exists (reuse, don't reinvent)

- `shardKeyOf(tenant, idx, n)` / `shardOf(id, n)` / `tenantOfShard(key)` — the shard-key helpers,
  collapsing to the bare tenant at `n == 1` (so the default layout is byte-identical).
- `writeMetricsClustered` — the template: group each point by `shardKeyOf(tenant, shardOf(seriesID,
  n), n)`, frame per shard, route each to its `Ring().Primary(shardKey)`.
- `clusterFetcherFor` — the read template: gather across all `n` shards (local when owned, else fan
  out to an owner) and merge.
- Compaction ownership already keys on the shard string, and `metricMergeOptions` recovers the tenant
  via `tenantOfShard` — both work unchanged for record shard keys.

## Proposed changes

1. **Write path (`writeRecordsClustered`).** Group each `recordengine.Batch` by
   `shardKeyOf(tenant, shardOf(streamID, n), n)` (a record batch is one stream ⇒ one shard) instead
   of by bare tenant, and route each shard's payload to `Ring().Primary(shardKey)`. Mirrors
   `writeMetricsClustered`. Origin rate admission stays per real tenant (Track 4b).

2. **Read path (`clusterRecordFetcherFor`).** Gather across all `n` shards like `clusterFetcherFor`:
   serve a shard locally when this node owns it, else fan out to an owner; concatenate (records are
   append-only, not ts-deduped). The current single-`Lookup(tenant)` becomes a per-shard loop.

3. **Cross-shard reassembly (the careful part).** Two read primitives assume a tenant lives on one
   owner set and must now gather across shards:
   - **`Trace(tenant, id)`** — a trace's spans share a `trace_id` but belong to different *streams*
     (services), which hash to different shards. The trace-by-id fetch must run against **every
     shard** and concatenate, or the result is missing spans. (Equality bloom pruning still applies
     per shard.)
   - **`ProfileResolver` / profile symbols** — a stack's symbols live in the side store of whichever
     shard ingested it; resolving a `stack_id` seen in shard A may need shard B's symbol tables.
     `clusterProfileSymbols` must union the side stores **across all shards**, not just owners of the
     bare tenant. (Content-addressing makes the union a plain dedup — no id remap.)
   - **`ProfileSeries` / `LogSeries` / `LogKeys` enumeration** — must fan across shards and merge
     (series concat/dedup by identity; keys union by key+scope).

4. **Config.** Decide whether `ShardsPerTenant` applies uniformly to all signals or gains a
   per-signal override (e.g. shard metrics ×8 but traces ×2). Recommendation: **one knob for all
   signals** initially (simplest, matches metrics); add per-signal overrides only if a real workload
   needs it.

## Risk / why this is not a quick wire-up

- Trace-by-id and profile-symbol correctness depend on **gathering across all shards** — a missed
  shard silently returns partial results (a correctness bug, not a crash). Each cross-shard primitive
  needs an explicit test that a trace/stack split across shards reassembles fully.
- The read fan-out widens from "owners of one key" to "owners of n keys"; hedging/concurrency bounds
  must still apply per shard so a wide tenant doesn't issue an unbounded RPC burst.

## Testing

- N>1 cluster: a single tenant's logs/spans scatter across shards; a query on any node returns the
  full set (mirrors the existing metrics sharding test).
- Trace-by-id: a trace whose spans hash to ≥2 shards reassembles completely from any node.
- Profiles: a flamegraph whose stacks' symbols span shards resolves fully (`ProfileResolver` unions
  all shards' side stores).
- N=1 equivalence: byte-identical routing/layout to the unsharded path (a guard test).

## Files touched (estimate)

`cluster.go` (`writeRecordsClustered` grouping, `clusterRecordFetcherFor`, `recordOwners` →
per-shard, `clusterProfileSymbols`/`clusterSeries`/`clusterKeys` cross-shard), `records_facade.go`,
`traces.go` (`Trace` cross-shard), `profiles.go` (`ProfileResolver` cross-shard), plus tests and an
update to `cluster/ARCH.md` + `recordengine/ARCH.md`.
