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
- **4e — DONE.** (1) **lz4**: AlgorithmLZ4 now does real pierrec/lz4/v4 block compression
  ([uvarint origLen][block] framing) instead of storing raw. (2) **SIMD**: added an AVX2
  `MinMaxFloat64` kernel (NaN via ±Inf blend, +Inf/-Inf all-NaN sentinel) with a fuzz-parity pure-Go
  fallback, used in `block.fillFloat64Stats`. (3) **MaxPartSize**: `Config.MaxPartBytes` splits
  flush/merge output so no part exceeds the cap (from `tenant.Limits.MaxPartSize`).

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
- **3b — per-signal record sharding — DONE.** `ShardsPerTenant` now shards logs/traces/profiles too:
  write groups by `shardKeyOf(tenant, hash(streamID)%N)`, reads gather/concat across shards. Cross-
  shard reassembly handled: trace-by-id across shards, `Log/Trace/ProfileSeries` concat, `LogKeys`
  union, profile symbol-store union (`clusterProfileSymbols`). N=1 byte-identical. See
  `docs/design/record-sharding.md`.

## Performance — read-path allocation/GC (see `docs/design/perf-plan.md`)

Profile-guided via `scripts/bench-pprof.sh` (benchstat + CPU/alloc_space pprof; `.bench/`).

- **P0.2** peek var-length prefixes (`bitstream`); **P0.1** pool part-decode buffers (`engine.decPool`);
  **P1.4** map-free metric sample merge; decimal/uvarint/scale-hoist micro-wins; **Win1** pool head/flush
  windows. Cumulative metric read: `fetch_all` **−74.7% time / +295% throughput**.
- **P1.3** opt-in batch-release pooling (`Request.Recycle` + `Batch.Release`, shared hook): metric
  recycling read **B/op −59% / time −24%**; default path byte-for-byte unchanged.
- **P1.5 — record engine read path.** Pool the part-decode int columns (`i64Pool`, always-on; the
  int columns are copied by value into accumulators, so no aliasing) and the per-stream accumulators
  (`recPool`, opt-in via `Recycle` + `Batch.SetReleaseState` carrying the `*recordCols` handle).
  Plain log read **B/op −25% / allocs −23%**; recycling log read **B/op −63% / time −44% / +85%
  throughput**. `TestFetchRecycleMatchesPlain` guards reuse correctness; race-clean. Remaining record
  allocator is the aliased byte columns (`readBytes`) — needs refcounting or a record decode cache.

- **P1.6 — PromQL adapter sub-millisecond instant queries.** Golden end-to-end benches
  (count_cpu_cores, full_scan_count over a 512-series node_exporter corpus). Zero-copy Prometheus
  series over the batch slices (kills floatSamples + per-sample interface boxing), Recycle batches
  released on querier.Close, SeriesID-keyed label-projection cache + shared scratch builder, decode
  cache enabled in the fixture. count_cpu_cores 1.6ms → ~0.61ms, full_scan_count → ~0.55ms; allocs
  −88%, B/op −62%. TestSelectZeroCopyAndRelease guards the lifecycle.
