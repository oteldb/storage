# Design: Concurrency optimization — lock scope & cross-shard parallelism

Status: **proposal**

## Summary

The storage engines are correct and already shard cleanly by tenant, but throughput and tail latency
are left on the table by **one recurring pattern: a single per-engine `sync.RWMutex` held across
object-store I/O**, plus **orchestration loops that walk independent shards strictly sequentially**.
The data model (immutable parts, a self-contained head drain) makes most of this serialization
*unnecessary* — it is an artifact of lock scope, not a real data dependency.

This document maps what serializes today (with file:line evidence, current as of the audit) and gives
concrete, ranked designs to fix it. It is the source of truth for the concurrency work; implement in
the priority order in [§7](#7-prioritization).

The problem decomposes into two halves:

- **Half 1 — inside one engine** ([§3](#3-half-1--inside-a-single-engine)): the per-engine lock is
  held across flush/merge/fetch backend I/O, so a tenant's appends and reads stall for the full
  duration of its own background compaction.
- **Half 2 — across shards** ([§4](#4-half-2--cross-shard--cross-tenant-orchestration)): maintenance,
  WAL-sync, cross-shard read-merge, and cross-shard write-routing all loop over independent
  engines/shards one at a time.

## 1. The core pattern

Both engines — `engine` (metrics) and `recordengine` (logs/traces/profiles) — guard *all* mutable
state (head, parts slice, sequence counter) with a single `sync.RWMutex`. That lock is held across
backend round-trips during flush, merge, and fetch. Because parts are **immutable** and a flush drain
produces a **self-contained snapshot**, the heavy work has no data dependency on the lock — only the
slice swap does. The fix throughout is the same shape:

```
  (under lock, microseconds)   snapshot the immutable inputs, reserve identifiers
  (lock-free, the slow part)   do the decode / encode / object-store I/O
  (under lock, microseconds)   publish the result (swap the parts slice, update the index)
```

The single genuinely hard problem this unlocks is **deferred reclamation** of parts that a lock-free
reader may still touch ([§3.3](#33-the-hard-part-deferred-part-reclamation)). It must land before
lock-free fetch is safe.

## 2. What is already right (do not regress)

- **Quorum replication is parallel** — one goroutine per target, quorum-bounded wait
  (`cluster/replica/replica.go:94-127`).
- **Ring lookups are lock-free** (`atomic.Pointer[ring.Ring]`, `cluster/etcd/membership.go:86,166`);
  rebalance is a metadata-only ownership handoff, non-blocking (`cluster/rebalance`).
- **Snapshot-then-iterate** is already used for maintenance/WAL-sync inputs
  (`storage.go:1055-1076`): the copy is taken under `tmu` and iterated lock-free. Only the *loops over
  the copy* are sequential — the snapshot itself is fine.
- **Hedged reads within a shard** race replicas with a delay (`cluster_retry.go:109-128`,
  `internal/retry`).
- The cache lock is not held across I/O (`query/scale/cache.go`); file/S3 backends are lock-free;
  admission is per-tenant with tiny critical sections (`admission.go`).

These set the precedent patterns the work below should reuse (e.g. the `query/scale` parallel fan-out,
the replica goroutine-per-target).

## 3. Half 1 — inside a single engine

### 3.1 Flush & merge hold the exclusive lock across I/O — **P0**

**Evidence.** `engine/merge.go:48-50` and `recordengine/merge.go:32-34` take `e.mu.Lock()` and hold it
across `compactParts` (reads + decodes **every** part from the backend, plus downsample CPU for
metrics), then `writePart`, `openPart`, `deletePart`. `flushLocked`
(`engine/engine.go`, `recordengine/engine.go:596`) holds it across
`writePart`+`openPart`+`updateIndexLocked`+`writeStreamIndexLocked`+`WAL.Checkpoint()`. For the whole
duration of a merge of large parts, every `Append` and `Fetch` for that tenant blocks.

**Design.** Restructure `flushLocked` / `mergeLocked` into three phases:

1. **Plan (under `e.mu.Lock`, microseconds).** Snapshot `parts := e.parts` (the slice header — parts
   are immutable), reserve `seq := e.nextSeq; e.nextSeq++`, and for flush drain the head into a
   `flushColumns` (already self-contained; `drainHead` clears the buffers). Release the lock.
2. **Build (lock-free, the slow part).** `compactParts`/encode/`writePart`/`openPart` over the
   snapshot. New appends land in the fresh head; new reads see the *old* parts — both correct.
3. **Publish (under `e.mu.Lock`, microseconds).** Re-acquire, replace `e.parts` with the new set
   (for merge: `oldParts → [newPart]`; for flush: `append(parts, newPart)`), build the bucket-index
   payload in memory. Then release and write the index + stream index to the backend (a CAS/atomic
   object write needs no engine lock), and retire the old parts via deferred reclamation
   ([§3.3](#33-the-hard-part-deferred-part-reclamation)).

**Concurrency invariant to preserve.** Flush and merge for one engine are only ever invoked by the
single maintenance goroutine (`storage.go:949-968`), so they never overlap *each other* — the only
concurrency to design against is append/fetch. That simplifies phase 3: `e.parts` is mutated by at
most one background actor at a time, so the swap is a plain assignment under the lock, not a CAS loop.
**If maintenance is later parallelized per-tenant ([§4.1](#41-parallelize-the-maintenance-loop--p0))
this stays true** — the parallelism is *across* engines, one goroutine per engine, so a given engine
still sees a single flush/merge actor.

### 3.2 Fetch holds `RLock` across part reads — **P0**

**Evidence.** `engine/engine.go:191-200` and `recordengine/engine.go:207-216` hold `RLock` for the
whole fetch, including `p.mergeInto`/`p.readCols` backend reads (`engine/part.go:126-160`,
`recordengine/part.go:122-147`). Concurrent reads don't block each other, but they hold off flush/merge
(writer lock) for the full backend-read duration, and vice-versa.

**Design.** Under `RLock`: resolve matchers, snapshot the live parts slice, and copy the head's
in-window rows into an owned buffer (this is already what `accumulate` materializes —
`recordengine/engine.go:549`). Release the lock; read the snapshotted parts lock-free; merge. The only
added cost is the head-window copy, which is small and already on the path. The lazy index sort stays
as today (one-time upgrade to `Lock` via `ensureIndexSorted`, `engine/engine.go:194`).

This is safe **only** with deferred reclamation: a reader holding a snapshotted part pointer must not
have its backend objects deleted by a concurrent merge.

### 3.3 The hard part: deferred part reclamation

Once §3.1 deletes parts off the lock and §3.2 reads parts off the lock, a merge can `deletePart` the
backend objects a reader is mid-flight on → the reader gets `ErrNotExist`. This is the classic
immutable-LSM GC problem and is the real engineering cost of P0.

**Options (pick one; recommend A for first cut, B later):**

- **A. Grace-cycle deletion (simple).** Merge/flush never delete inline. Retired part prefixes go onto
  a per-engine `pendingDelete []retired{prefix, retiredAt}`. The maintenance loop deletes entries
  older than one full maintenance interval (longer than any fetch). Correct as long as a fetch cannot
  outlive the interval; enforce with a fetch deadline. Cheap, no hot-path cost, slightly delayed
  space reclamation.
- **B. Reference-counted parts (precise).** Each `*part` carries an `atomic.Int32` refcount. A fetch
  snapshot `Acquire()`s the parts it will read and `Release()`s on iterator `Close()`; a retired part
  is deleted when its count hits zero. Precise reclamation, but adds two atomics per part per fetch
  and couples deletion to iterator lifetime (every `fetch.Iterator.Close` path must release). More
  surface area; defer until A proves insufficient.

Either way: the bucket index is the source of truth for *liveness*; a stateless reader that reloads
(`LoadParts`) only ever sees committed parts, so reclamation is purely a live-process concern.

### 3.4 WAL: per-sample writes under lock; inline fsync — **P1**

**Evidence.** `engine/engine.go:140-151` (`AppendBatch`) calls `WAL.WriteSeries`/`WriteSamples`
**inside the per-sample loop, under `e.mu.Lock()`** — N syscalls per batch under the exclusive lock.
The WAL fsyncs immediately after each framed write when sync is on (`wal/segment.go:255-273`); the only
batched fsync path is the background `syncWALs` sweep (`storage.go:873-891`). The record engine already
batches frames per batch (`recordengine/engine.go:481`) — metrics should match.

**Design.**
1. Buffer a batch's WAL frames and emit **one** `WriteSamples`/`WriteRecords` per batch (mirror the
   record engine), so the lock covers one append, not N.
2. Release `e.mu` *before* fsync; let a group-commit goroutine coalesce fsyncs across tenants
   (one fsync serves all writers parked since the last). This trades a tiny latency add for a large
   syscall-rate and lock-hold reduction under concurrent ingest.

Keep the zero-alloc hot-path contract (`CLAUDE.md`): the per-batch frame buffer must come from the
existing reusable WAL buffers (`wal/wal.go:44-45`), not a fresh allocation.

### 3.5 Intra-tenant write sharding (the single-tenant ceiling) — **P2**

In single-node mode there is exactly one engine (one lock) per tenant; the cluster's
`hash(seriesID) % N` sharding (`cluster.go:65-71`) affects *placement*, not local locking. A single
hot tenant is therefore capped by one mutex. If profiling ever shows a single tenant saturating a core
on ingest, stripe the head into N sub-heads keyed by series-hash (N locks, fan-in at flush). Larger
change; only pursue on evidence.

## 4. Half 2 — cross-shard / cross-tenant orchestration

### 4.1 Parallelize the maintenance loop — **P0**

**Evidence.** `storage.go:949-968`: `maintain()` walks metric→log→trace→profile, and within each,
tenant-by-tenant, calling `flush()` then `merge()` strictly sequentially. With T tenants × 4 signals,
each holding its engine lock across backend I/O, the cycle takes the **sum** of every engine's I/O
time. The engines are independent; the input snapshots are already lock-free
(`engineSnapshotByTenant`, `storage.go:1068`).

**Design.** Fan out over the snapshot with a **bounded** `errgroup` (or a worker pool over a channel),
concurrency `= min(runtime.NumCPU(), cap)` with `cap` a new option (default ~8–16) so the backend
isn't overwhelmed. Per-engine work stays `flush()` then `merge()`. Errors are already swallowed
per-engine (`maintainEngine`), so a fan-out needs no error-aggregation semantics change — just collect
and log. **Highest impact-to-risk ratio in this document; independent of all P0 lock work** (it
parallelizes *across* engines whether or not each engine's internal lock scope is fixed) — do it
first.

Note the cluster-ownership reconcile (`storage.go:929`) must stay **before** the fan-out (it is a
single etcd round-trip that decides which tenants this node owns); only the per-engine flush/merge
parallelizes.

### 4.2 Parallelize WAL-sync — **P1 (small)**

**Evidence.** `syncWALs` (`storage.go:873-891`) fsyncs each engine's WAL sequentially across all four
signals. **Design.** Same bounded fan-out as §4.1 over the same snapshots. Lower impact than
maintenance (fsync ≪ compaction) but trivial once §4.1's helper exists; share the worker pool. (If
§3.4's group-commit lands, this loop largely subsumes into it.)

### 4.3 Parallelize cross-shard scatter-gather — **P1**

**Evidence — reads.** `fetch.Merge` (`query/fetch/merge.go:55`) drains children **sequentially**, and
it is the cross-shard gather (`cluster.go:200`), the cross-tenant query path (`storage.go:520`), and
logs' `concatFetcher` (`logs.go:70-88`). A query over N shards pays the **sum** of N latencies;
hedging only races replicas *within* one shard (`cluster_retry.go:109-128`), never across shards.

**Evidence — writes.** `writeMetricsClustered` (`cluster.go:629`) and `writeRecordsClustered`
(`records_facade.go:147`) route each shard/tenant primary one at a time — sum of RTTs.

**Design.** Reuse the exact pattern `query/scale` SplitFetcher already uses (`scale.go:48-80`: parallel
window fan-out, per-index result slices, `WaitGroup`) for the shard/tenant fan-in in `fetch.Merge` and
the two cluster write loops. **Add a concurrency bound** — `scale`'s fan-out is currently *unbounded*
(one goroutine per window); a wide cross-shard query or write must not spawn hundreds of in-flight
RPCs. Factor the bounded fan-out into one helper and use it in all three places (and retrofit
`scale`).

Correctness: `fetch.Merge`'s post-merge (`MergeBatches`, dedup by series id) is order-independent, so
parallel child drain changes nothing about results. The write loops accumulate `rejected` counts —
make that accumulation concurrency-safe (atomic or per-shard result slice then sum).

### 4.4 `tmu` held across WAL file I/O on first write — **P2**

**Evidence.** `engineFor`/`recordEngineCached` (`storage.go:781-805`, `records_facade.go:24-57`) hold
the **single `tmu`** (guarding all four tenant maps) across `wal.Create()` filesystem I/O
(`storage.go:818`). A first-write to a new metric tenant blocks first-writes to new log/trace/profile
tenants. Steady state is unaffected (cached pointer, no lock). **Design.** Double-checked creation:
fast-path read under `tmu`; on miss, create the WAL *outside* `tmu`, then re-lock and insert (losing a
race just discards the spare WAL). Alternatively shard `tmu` per signal. Only matters under tenant
churn / cold start.

## 5. Cross-cutting constraints (from `CLAUDE.md`)

- **Zero-alloc hot paths are a hard requirement.** Every change above on the append/fetch path must
  reuse existing buffers/pools (WAL frame buffers, `recordCols`, `sync.Pool` scratch) — no new
  per-record/per-sample allocation. The fan-out helpers (§4) allocate per *query*/*cycle*, not per
  record, which is acceptable.
- **One merge engine, immutable parts.** The snapshot-swap (§3.1) and deferred reclamation (§3.3) must
  not introduce a parallel compaction subsystem — they restructure the *existing* flush/merge, not add
  a new path.
- **Single-node must work with the cluster absent.** §4.3's cluster fan-out changes live behind
  `s.cluster != nil`; the local `fetch.Merge` parallelization must stay correct (and bounded) in
  single-node cross-tenant queries.
- **Backends interchangeable / in-memory first-class.** All changes must keep working with
  `backend.Memory()` + `Durability=Ephemeral` (no WAL, no flush). The deferred-reclamation grace cycle
  must no-op cleanly head-only.

## 6. Testing strategy

- **Race detector** (`go test -race`) on new concurrent paths is mandatory; add tests that hammer
  Append+Fetch+Flush+Merge concurrently on one engine (catches §3.1/§3.2 reclamation bugs).
- **Property test for fetch-vs-merge:** under concurrent merge, a fetch must return a superset of a
  brute-force scan and never error with `ErrNotExist` (validates §3.3). Mirror the existing
  `TestFetchSupersetOfBruteForce` (`recordengine/engine_test.go:243`) with a background merge loop.
- **Deferred-reclamation invariant:** assert no live reader observes a deleted part; a part retired at
  cycle N is gone by cycle N+1 (grace-cycle option A).
- **Maintenance fan-out:** assert bounded concurrency (never more than `cap` engines mid-flush at once
  — instrument via a counting hook) and that a slow/erroring engine doesn't stall the others.
- **Benchmarks** on the hot paths before/after, reporting throughput via `b.SetBytes` and
  `b.ReportAllocs()` (per `CLAUDE.md`): single-tenant ingest under concurrent merge (§3.1), cross-shard
  read latency (§4.3), maintenance-cycle wall-clock at T tenants (§4.1). Measure to confirm the lock
  scope was the bottleneck, not the backend.

## 7. Prioritization

| # | Change | §  | Impact | Risk | Notes |
|---|--------|----|--------|------|-------|
| 1 | Parallelize maintenance loop (bounded errgroup) | 4.1 | High | Low | Do first; independent of all lock work |
| 2 | Parallelize cross-shard read-merge + write-routing (bounded) | 4.3 | High (cluster) | Low-Med | Reuse `scale` fan-out; add bound |
| 3 | Flush/merge I/O outside the engine lock + deferred reclamation | 3.1, 3.3 | Very High | **High** | Reclamation (option A) is the real cost; gates #4 |
| 4 | Fetch reads parts lock-free | 3.2 | High | Med | Requires #3's reclamation |
| 5 | WAL group-commit + lock-free fsync; one frame/batch for metrics | 3.4 | Med | Med | Metrics catches up to record engine |
| 6 | Parallelize WAL-sync | 4.2 | Low | Low | Trivial once #1's helper exists |
| 7 | `tmu` off the WAL-create path | 4.4 | Low-Med | Low | Tenant-churn / cold-start only |
| 8 | Intra-tenant head striping | 3.5 | Med | High | Only on profiling evidence |

\#1 and #2 are cheap, safe, and large — start there. #3 is the headline win, but its cost is the
deferred-reclamation scheme; rushing it risks readers hitting deleted backend objects, so it must land
with #3 before #4 is enabled.

## 8. Non-goals

- A new lock-free/concurrent data structure for the head (postings/symbols/series stay
  caller-synchronized; the engine lock still guards them — we only *shorten* its hold).
- Changing the sharding model (HRW ring, series-hash shards) or replication/quorum semantics.
- Backend-internal parallelism (e.g. parallel S3 list pagination) — separate, lower-value follow-up.
