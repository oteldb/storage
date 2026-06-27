# Agentic worklog

A running log of the agent-driven improvement work (tracks from
`docs/design/improvement-roadmap.md`). Newest entries at the bottom. Each entry maps to one or
more commits; the code of record is the git history and `ARCHITECTURE.md`.

## Track 4 — wire-ups

- **4a — event-driven minimal-move rebalance executor.** `cluster/etcd/ownership.go` `Reconcile`
  became stateful (tracks held claims, ring-primary lookups in memory, etcd write only on a claim
  change → zero round-trips in steady state) and records the enacted `rebalance.Plan`
  (`Owned()`/`LastPlan()`).
- **4b — admission on the clustered write path.** Rate valve at the origin; cardinality + in-flight
  on the shard primary (`engine`/`recordengine.ApplyPrimary` take `AppendLimits`, return per-reason
  `AppendResult`); reasons flow back over the primary-write RPC (`primaryReject`).
- **4c — Log/Trace enumeration fan-out.** Signal-dispatched series RPC + new keys RPC
  (`cluster.KeysPath`); `LogSeries`/`LogKeys`/`TraceSeries` fan out to owners (hedged). Keys codec
  fuzzed.
- **4d — WAL-durable scale factor.** New `recordSamplesSF` WAL frame; replay restores sampling
  weights so a crash recovers unflushed sampled data at its representative weight.
- **4e — deferred** (MaxPartSize part-splitting; lz4 needs a dep; SIMD codegen). Follow-ups.

## Track 1 — introspection

- **`Storage.Inspect()` → `StoreStats`.** In-memory pull snapshot (per-tenant/signal counts +
  time span + admission, decode-cache totals, cluster membership/ownership/last-rebalance). No I/O.

## Track 2 — control

- **`Storage.Admin()`.** `Flush`/`Compact`/`Retention`/`Rebalance`/`MaintainNow`; cluster
  flush/compact gated to the shard's ring-primary (`ErrNotOwner`).

## Track 3 — multi-tenancy

- **3c — fair maintenance scheduling.** Maintenance work-list ordered by head pressure (fullest
  heads flush first).
- **3a — cardinality budget + `__overflow__` routing — DONE.** `Limits.MaxSeriesSoft` + a
  signal-supplied `AppendLimits.Overflow` remapper; past the soft line a new metric series routes to
  a collapsed `{__name__, __overflow__}` series (head-enforced, WAL-consistent via the effective id),
  counted as accepted+overflowed (`storage.ingest.overflowed`). No hysteresis (monotonic index). See
  `docs/design/cardinality-overflow.md`.
- **3b — per-signal record sharding.** (in progress — see `docs/design/record-sharding.md`)
