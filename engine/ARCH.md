# `engine/` — the metrics vertical

One `Engine` per tenant (or metric shard) ties index, parts and WAL into an ingest+query path. Its
twin for logs/traces/profiles is [`../recordengine/ARCH.md`](../recordengine/ARCH.md), which shares
the locking discipline below.

## Locking discipline (shared with `recordengine`)

**Invariant: the engine lock is never held across object-store I/O.** Parts are immutable, so every
phase is `plan under lock → I/O off lock → publish under lock`.

| element | rule |
|---|---|
| `parts` slice | copy-on-write; a snapshot stays valid after unlock |
| fetch | plans under RLock (matchers, `acquire()`, head seed), then reads lock-free |
| flush/merge | parts swap, index commit and WAL checkpoint publish atomically |
| writers | only the maintenance loop mutates `parts` |
| retired parts | refcounted, reclaimed deferred, so a fetch never races a delete |
| head detach | detached buffers stay readable via `flushing` until the part lands |

Publishing atomically keeps the durable watermark in step with part discoverability: exactly-once
crash consistency.

**Invariant:** a record is visible in exactly one of head / `flushing` / part. Never neither
(visibility gap), never both (double count).

## Head

The index (`symbols`+`series`+`postings`) plus per-series `(ts, value)` buffers.

**`OOOWindow` is per-series** (`head.seriesNewest`): a sample more than `OOOWindow` behind *that
series'* newest admitted sample is rejected, so a fast or clock-skewed series cannot shed the slower
ones sharing the head, and a first sample is never late. Watermarks outlive a flush.

**The series index outlives a flush** — only sample buffers drain, so flushed series stay queryable
and re-appends do not re-index.

**`AppendBatch`** takes a precomputed `SeriesID`, columns, and a `materialize` callback used only on
first sight, under one lock. `appendByID` does one map probe: a present buffer means the series is
known, so no `signal.Series` is built or hashed. WAL frames group by series, one write a batch.

## Flush

Drains the head into one flat part, one row per sample:

```
{tenant}/metrics/{seq}/
  columns    [series:int128, ts:int64, value:float64]   sorted by (series, ts)
  sidx       series index sidecar
  stats      aggregate stats sidecar        (optional, Config.AggregateStats)
  identity   this part's series identities  (format in index/identity)
bucket index (part list + time bounds)      ← written last: the commit point
```

Merge writes the same shape, committing the new part set before deleting sources.

### Identity is part-scoped

Each part carries its own series' identities, written and deleted with its other objects. Three
properties follow, and they are why the whole-set `series.bin` is gone:

| property | consequence |
|---|---|
| retention self-cleans | dropping a part drops the identities naming its rows |
| a flush persists only what it wrote | 88 B to add one series to a 20k-series tenant |
| every node derives its own live set | a replica prunes its own parts, no ownership rule |

The old object was re-serialized in full on any change: ~40 B/series interned, against ~218 B/series
repeating the label bytes. An older prefix's `series.bin` is read at open, then deleted once every live
part carries its own identities, so recovery cannot resurrect identities whose data is gone. Identity
is read on recovery and by a replica adopting a part, never by a query.

**The WAL carries its own identities.** A flush checkpoints it, discarding the series records written
when identities were first seen, so a record is logged whenever a series starts a **new sample
buffer** — once per actively-appending series per flush window, not only when its identity is new.
Otherwise a sample logged after a checkpoint names a series the log no longer describes, and identity
now lives in parts retention can drop at any time.

**Metered apart.** The resident half — symbol table, series index, postings, OOO watermarks — is
`Stats.IdentityBytes`, not `HeadBytes`: a flush drains samples, not identities, so folding them would
have the size-triggered flush chase a number it cannot lower.

### Publish order: the part's objects first, the bucket index last

The bucket index makes a part durably visible, so writing it is the commit point and a readable part
always carries the identities its rows resolve through. A crash in between leaves an orphan — objects
and identity together — swept at the next open, stranding nothing.

**No `FlushedEpoch`** here, unlike `recordengine`: the WAL `Checkpoint` runs last and is the WAL's
commit point, so replay recovers a part that failed to publish. The unreadable-commit window bites
only a persistent backend with no `WALDir`, where nothing else holds the rows.

## Identity prune

`PruneIdentities` drops the identities retention leaves behind, which otherwise accumulate for the
process' lifetime under series churn.

| | |
|---|---|
| live set | the live parts' id sets ∪ head ∪ mid-flush detachment ∪ recent |
| when | after a merge, or a replica's refresh — and only if parts went away |
| ownership | none needed: the live set is what this node can still serve |

Each `sidx` already lists its part's ids, so no new on-disk format. Identities die no other way, so an
engine whose data only grew skips even the live-set walk.

Symbol ids are dense and referenced by the postings, so nothing is removed in place. Survivors are
**rebuilt** off-lock against `series.Index.Snapshot()` — the append-only entry log, immutable without
the lock because registration only extends it — then swapped in under the lock.

| step | 200k identities | 1M |
|---|---|---|
| rebuild, off-lock, far too long to hold `e.mu` | ~0.6 s | ~3.4 s |
| swap, under lock, allocation-free | ~5 ms per 200k pruned | ~40 ms |

**Two sets must survive a rebuild that decided without them:** entries registered past the snapshot's
end, and dead-found series that **regained samples** — not re-registered on a new sample, so leaving no
log entry, and dropping them would strand samples the next flush writes into a part with no identity
naming them. A dead series' OOO watermark is deleted by key, so cost tracks what died, not cardinality;
a live one keeps its own, or a late sample would be re-admitted.

The WAL needs no `walExpiries` equivalent: checkpointed every flush and re-logging a series record when
a series starts a buffer, its live records name only series it describes. The prune writes nothing
durable. `recordengine/prune.go` is the same prune over resident `part.ranges`; **known gap there:**
merged symbol sidecars stay unbounded.

## Lifecycle and part sequences

Driven by the facade's single maintenance loop, plus a head-bytes trigger flushing only the
over-threshold engines. `Engine` is exported and `Close`/`Reset` callable from anywhere, so
concurrency is enforced, not assumed: flush and merge hold one `flushMu` across their whole body.
`Reset` takes it too, draining an in-flight flush/merge that would otherwise publish its part — and a
stale sequence — into the emptied engine, dropping detached buffers with the head, and **retiring**
live parts rather than deleting their objects, so a fetch holding one is not read out from under it.

**Invariant: part sequences are append-only.** Flush and merge reserve `{seq}` as they write and
advance immediately, so a failed attempt burns its sequence rather than handing it to the retry: a
rewrite replaces only the objects it produces, so a part reusing the sequence inherits leftovers it
never wrote.

`LoadParts` sweeps that residue — a failed attempt's objects, or a retired part whose reclaim delete
failed. It lists the prefix, deletes every object under a part directory the bucket index does not
name, and resumes past the highest sequence seen *on the backend*, which is what makes the guarantee
survive a restart. The sweep assumes this node **owns** the prefix, so a replica's `RefreshReplica`
skips it: the owner's in-flight part is not in the index yet.

## Flush and merge are bounded separately

| phase | splits at | measured in | why |
|---|---|---|---|
| flush | `MaxPartBytes` | approximate uncompressed bytes | it sizes rows already in the head |
| merge | `mergeCapBytes` (`mergecap.go`) | bytes on disk | what the writer actually encoded |

Splitting at row boundaries is safe: parts are independent, and a series spanning two is merged back
by the read seam.

```
mergeCapBytes = min( MergeCeilingBytes,
                     free        / MergeConcurrency / 2,
                     mergeMemory / MergeConcurrency / 2 )  ← whole-object backends only
```

**Why not a constant:** cardinality spends the budget in *breadth*, so a fixed cap shrinks a part's
time span as active series grow, and a fixed-range query opens proportionally more parts. Both
references size against storage instead (VictoriaMetrics `getMaxOutBytes`, ClickHouse
`max_bytes_to_merge_at_max_space_in_pool` lowered by free space).

**Why divided:** by `MergeConcurrency`, so concurrent merges cannot collectively fill the disk; halved
again to let a merge's output coexist with inputs it has not yet retired. `MergeConcurrency` is a
**callback**, since fan-out is bounded by the node's engine count as much as by the worker limit and
engines appear lazily; fixed at engine creation it would divide a single-tenant node's disk by its core
count, landing a 32-core box back at the constant it replaces.

**Degenerate cases.** Backends that cannot report free space (`Memory`, object stores) keep the
ceiling. A nearly full disk falls to `minMergeCapBytes` rather than sealing everything, since stranding
the part count high is worst when compaction matters most; that floor binds derived bounds only, so a
configured ceiling below it is honored.

**Memory bounds the cap only over a backend that takes objects whole.** There the output accumulates in
RAM until sealed and `build` serializes it into one buffer per column, so peak resident ≈ 2× the part.
Free space says nothing about that: a 4 GiB pod on a 464 GiB volume derives 232 GiB, clamps to the
16 GiB ceiling, and OOMs. Hence the third term, split across concurrent merges and halved for the
serialize step; the allowance defaults to an eighth of the process budget (`GOMEMLIMIT`, else the
cgroup limit, else host memory — `internal/memlimit`).

Over a `backend.ObjectCreator` (`file`) the term drops: the writer hands each column's frames over as
they seal (`block/ARCH.md`, "Two writers") and never holds the part. A streamed merge still holds
**per-series** state — id runs, series index, aggregate sidecar — which is O(distinct series), not a
function of part bytes; a merge of very short series holds far more per encoded byte. So the loop also
seals on `partStreamWriter.residentBytes()` against `mergeMemoryBudgetBytes()`, the same allowance at
face value, bounding memory directly.

## Merge — one pass, five modes

```go
MergeWith(MergeOptions{RetainFrom, Downsample, Recompress, Precision})
```

Compacts a bounded, size-tiered group of parts, never the whole set. All five modes are the one merge
engine; no parallel subsystem.

| mode | effect |
|---|---|
| compact | merge per series by timestamp, freshest wins |
| retention | drop samples past `RetainFrom` |
| downsample | roll up by tier |
| recompress | size-graduated level; the age tier only on a fully cold part |
| precision | re-encode at a lossy budget, fully cold parts only |

**Determinism:** absolute timestamps, never clock reads — the caller resolves policy against one `now`
per pass — and grid-aligned downsample buckets, so a rollup does not depend on when the merge runs.

**Fixed points:** repeated merges are stable for last/first/min/max/sum/avg, count being the documented
exception. Recompression checks the part's recorded algorithm *and* level, precision the manifest's
recorded budget; only an upgrade rewrites, and a part denser than the target is left alone.

**Weight-aware:** compaction and rollup honor the lossy-sampling scale factor, keeping a sampled series
unbiased.

### Graduated compression (`recompress.go`)

| tier | level |
|---|---|
| row-count ladder, every merge output | zstd 1 ≤ 64k rows, 2 ≤ 1M, else 3 |
| age tier `RecompressSpec` | at most one, above the ladder, fully cold only |
| flush | codec-only framing; a hot flush is unaffected |

The ladder is VictoriaMetrics', capped the same way. A merge rewrites the data regardless, so the only
cost is the level's CPU, and a bigger part — older, read less per byte, merged less often — earns a
denser level. **Recompression is decode-transparent:** the reader keys off the manifest's per-column
algorithm, a pure ratio/CPU trade with no format change. The level is recorded only so a merge can tell
a part at the target from one below it.

### Retention drops whole parts first

A part whose `maxTime` is past the cutoff holds no row retention would keep, so `dropExpired` retires
it on the manifest alone: no decode, no output part, only a straddler rewritten. Retention is then O(1)
in the expired data rather than O(bytes), as in Prometheus (whole blocks) and VictoriaMetrics (whole
partitions). The drop publishes like any merge, so a failed commit rolls back and the parts stay live.
Disk-pressure eviction shares `RetainFrom`, so it drops whole parts too.

### Selection is confined to an aligned time bucket (`timebucket.go`)

Size tiers have no notion of time, so selection merged a part covering hour 3 into one covering hours
0–48; the output spanned the whole store and every part then overlapped every query window (#308).

```
mergeLadder:  1h  →  6h  →  24h      each level divides the next, so buckets nest
              ↑ flushes land here    ↑ widest part built; the coarsest locality a query can rely on
```

Parts are grouped by aligned bucket, and the size-tiered selector runs unchanged inside one group — as
in ClickHouse, where the size selector is only ever fed one partition. The ladder is walked
narrowest-first, so each part is rewritten once per level rather than repeatedly at the widest.

**The still-filling newest bucket is skipped**, since merging it now guarantees merging it again. The
finest level is exempt: flushes land there, and letting them accumulate is the part-count growth
sealing exists to bound.

**Forced rewrites are confined too, and win the cycle.** Unioned with the size-tiered run they merged
parts from opposite ends of the store into one spanning both, on the cycle most likely to run. Now the
oldest forced part picks the bucket, the rest of that bucket rides along if it fits the cap (merging
inside a bucket cannot widen), and the size-tiered run waits.

**A straddler belongs to no bucket** and cannot join one without widening the output, so it is left out
of ladder groups — but rewritten *alone* when forced, since retention correctness does not wait on
straddle splitting. Parts written before bucketing are mostly straddlers, so the ladder narrows new
parts and leaves old wide ones until straddle splitting lands.

### Run selection (`compact.go`)

Picks only what is worth merging: any part a forced rewrite must touch, so age-driven work is never
starved, plus the best run of *unsealed* parts.

| rule | effect |
|---|---|
| a part at the merge cap is sealed | part count ≈ dataset / cap, not growing per flush |
| runs score `m = output / largest input` | the inverse of write amplification |
| escape: total under `smallRunBytes` | skips both guards |
| escape: `mergeIdleRounds` fruitless merges | best run taken regardless of score |
| `MergeOptions.Force` | the operator's version of the idle escape |

Re-merging a sealed part would only re-split it. Sealing and the run budget are in on-disk bytes
(`part.sizeBytes`), and `maxMergeParts` bounds a run too, so a large disk-derived budget cannot make
one merge unbounded. Scoring follows VictoriaMetrics' `appendPartsToMerge`.

**Ordered by size, not bucketed.** Bucketing stranded leftovers: a part alone in its power-of-two tier
never reached the two-per-tier threshold, nothing landed in that tier again, and it was carried by
every query for the engine's lifetime (#285). Ordering alone does not fix it — leftovers spread
geometrically, one per former tier, so every run over them fails the balance test or scores under
`minMergeMultiplier`. Hence the escapes: the guards argue about the proportion of bytes rewritten,
meaningless when the whole rewrite is cheap, and rewriting a large part once to absorb a stray beats
carrying it forever. `Force` supplies an idle count already at `mergeIdleRounds`, so it is the same
selection the engine would reach on its own — bypassing the heuristic, never the memory bound.

The selector works in size order but returns the run in engine part order, so the merge visits sources
oldest → newest and a later part's value wins a duplicate timestamp.

### Merge shape (`mergeshape.go`)

`Engine.MergeShape` reports the selector's inputs off the merge path (fields in `ADMIN.md`). A no-op
merge is otherwise indistinguishable from an idle engine, and a store can sit at a part count it will
never reduce for thousands of cycles with nothing saying so. The cap comes from the last merge rather
than being derived on demand: deriving it reads free space, and introspection does no I/O.

### Streaming both ways (`compactStream`)

```
source part ─ forward cursor ┐
source part ─ forward cursor ┼→ merge per series → partStreamWriter → block.StreamWriter
source part ─ forward cursor ┘  (one series range   (streampart.go)    encodes a full granule
                                 at a time)
```

Working set is O(parts × one series range) plus the *encoded* output — not O(dataset), and not the
output's uncompressed rows. That second half matters for granularity: the row cap used to set peak
merge memory as well as part size (`capRows × 32 B`, 512 MiB at the default), so raising it to widen
parts raised merge RSS; now it only sets part size. Measured on a 2M-row merge, total allocation fell
3.7–5.4× and peak heap 2.9–3.8×, the wider margin on counter-shaped data.

Sidecars stream with it: series index, identities and aggregate stats come from the same per-series
calls, each run- or series-shaped, so they cost O(distinct series), not O(rows). Encoding decisions
must be fixed before the first row — compression profile, precision budget, whether a weight column
exists — so `mergeEncoding` derives them from the sources up front; its doc has why that is equivalent.

The single-part forced-rewrite path (`writeColumns`) still buffers whole columns: its fixed-point check
needs the post-downsample row count before deciding to write at all. It is bounded by one part, and is
not the routine compaction tick.

### Publish ordering

A merge retires its sources only after the bucket index naming their replacement is committed. The
index is what a restart and every replica read, so a part it still names must never become reclaimable:
retiring first would let the next reclaim delete referenced objects, and `LoadParts` hard-fails the
engine on a missing part. A failed commit rolls the in-memory swap back, so the uncommitted output is
never observable as published; its objects are orphans, swept at the next open.

## Read path

`Fetch` resolves matchers over the index, then merges each series' head buffer ∪ every part by
timestamp — **one series per `Next`**, so a consumer that folds and releases each batch never holds
more than one series' samples, whatever the matched count.

The plan (acquired parts, decode reservation, head snapshots) and the fetch's span, profile and metrics
span the whole iteration, settled by `Close` — **which the caller owes even when it stops early**. What
stays O(matched series) is the plan: one identity per matched series, plus head and mid-flush
snapshots, copied under the lock because a concurrent flush moves a series' buffer into a part the plan
did not acquire.

Layered optimizations, each opt-in:

**Series index sidecar** (`{prefix}/sidx`) — sorted distinct SeriesIDs and run-start rows, fixed-width,
binary-searched in the raw bytes and held only while a fetch reads the part, re-fetched through
`backend.ReadView` as a zero-copy cache hit. Resident index memory then follows the read cache budget
rather than series count, and opening a part reads no series column. Derived: a missing or corrupt
sidecar scans the series column once instead, so no migration burden.

**Block slicing + decode cache** (`Config.DecodeCacheBytes`) — column blocks are sliced from a
byte-bounded LRU keyed by `(part, column, block)` and added to the merge as **views**, never
materializing a decoded part. Entries are immutable and refcounted, released per series after copy-out;
an evicted, unpinned buffer recirculates through a bounded freelist, cutting miss-path allocation
without enlarging the resident set. Cache-off, or constant/unblocked columns, decodes per fetch,
series-skipped to the blocks the matched row ranges touch. With the cache on a fetch also prefetches
the parts it will touch.

**Granule time pruning** — block boundaries align with the part's marks granules, so the marks index
already carries each block's `[MinKey, MaxKey]` sample times (`block/ARCH.md`). A block-sliced fetch
drops blocks that cannot intersect the window before reading them, and sizes the decode reservation
over the survivors. Rows are `(series, ts)`-sorted, so granule bounds are not monotonic across a part,
but they are inside one series' row range — where the test applies. Without it a narrow window cost a
full part scan: a long series was decoded whole and discarded row by row. Derived and advisory —
absent, corrupt or mismatched marks prune nothing.

**Recent tier** (`Config.RecentWindow`) — mirrors the most recent flush window in RAM across flushes,
so a query inside the window acquires no part at all; overlap is deduped by the freshest-wins merge.

**Buffer recycling** (`Request.Recycle` + `Batch.Release`) — default-off. Result buffers come from a
GC-stable doubly-bounded freelist, not `sync.Pool`, which empties at every GC and lost the capacity
under allocation-driven collections.

### Decode-memory budget (`Config.DecodeMemoryBytes`)

A shared byte semaphore over in-flight decoded column bytes, bounding the query-concurrency RSS cliff.
Reserved once per *fetch* off the lock — never incrementally per part, so two queries cannot deadlock
holding partial reservations — with a fetch larger than the whole budget admitted alone. The facade
builds one budget for all tenants, so the cap is process-wide.

**Per-fetch is only deadlock-free while a caller keeps one fetch open at a time.** True of a
drain-then-close consumer, false of a streaming one holding several iterators across an evaluation:
there the second acquire is hold-and-wait against the query's own reservation, and admit-alone cannot
fire, `used` being non-zero precisely because this query holds it.

**`Request.Scope`** (a `fetch.Scope`) names the logical query — the read-side analogue of Prometheus'
`Storage.Querier`, one per request, passed on every read under it. Its first read blocks as usual;
while it holds, later reads sharing the scope are charged without queueing. Accounting stays exact,
only the waiting is skipped, so such a query may overshoot the ceiling by its own later estimates — the
same latitude a single over-budget read has. A nil scope is unchanged; `fetch.WithScope` installs one
on the request context where a call path cannot thread it through every `Request`.

**A scope is an optimization, never a correctness requirement.** The library cannot verify "one fetch
at a time", so the wait is bounded twice and a missing scope costs latency and memory, never liveness:

- **Cancellable.** Admission takes the fetch's `ctx`; a done context aborts and reserves nothing, so a
  deadline or disconnect always recovers the goroutine and an unscoped hold-and-wait degrades to one
  failed query, not a wedged engine. `Fetch`/`Count`/`Aggregate*` can therefore fail before reading
  anything, releasing the plan on that path.
- **Force-admitted.** A queue head starved for `DefaultDecodeBudgetForceAfter` goes over the ceiling,
  counted and logged (`ADMIN.md`). The ceiling is already soft, so trading an RSS overshoot for
  liveness is the same trade.
- **FIFO with hand-off** — the releaser reserves on the waiter's behalf before waking it, since a
  cancellable waiter must not have to hand a turn back and a barging arrival must not starve a large
  query. `used == 0` still admits unconditionally.

## Count and series enumeration

`Count`/`CountBy`/`Series` share one **existence plan**: matched ids from the head index, which
outlives a flush, and in-window existence from the live buffers. No batch, no value column, no label
projection. They differ in what they ask of the parts:

| call | parts contribute by | decode |
|---|---|---|
| `Count`/`CountBy`, exact | sorted intersection, or binary search at a window edge | the two edge parts, ts only |
| `Series`, series-only | the matched ids the part's series index holds | never |

The intersection applies to a part fully inside the window. `Series` therefore costs matched
cardinality alone, not the window's depth, and its window filter is **part-granular**: a series in an
overlapping part is listed even if its samples sit just outside — the granularity `recordengine.Series`
and Prometheus' label endpoints also have. It is the primitive the metrics label endpoints need, whose
fetch-based answer would decode and discard every sample of every match.

## Label metadata

`LabelNames`/`LabelValues` answer from the index, not from series. With no matchers the walk is over
the postings' (name → values) map, O(distinct values), materializing no identity; with matchers it
narrows to the matched ids and reads only the requested name off each identity.

The head's series index is **all-time**: it outlives flushes, and retention prunes samples and parts
but never identities, so the index alone would keep offering labels of series whose data is long gone.
Every value is therefore **liveness-probed** — an in-window in-memory sample, or membership in a part
overlapping the window — stopping at the first live series, so a live value costs one probe rather than
a scan of its postings list. Part indexes are warmed before the engine lock is taken, keeping the probe
I/O-free under it.

## Aggregate pushdown

`AggregateRange`/`AggregateStep` return per-series count/sum/min/max (→avg) over a window or a
step-aligned grid. With `Config.AggregateStats` each part writes a small stats sidecar
(`{prefix}/stats`), and a range **fully covering** a part folds it without decoding the value column.

Taken only when provably exact: in-window parts fully covered *and* pairwise time-disjoint, else it
falls back to decode+merge, which dedups. Derived, so absent or corrupt means decode. In cluster mode
it survives the network — each owner aggregates locally and ships per-series identity plus buckets, so
only aggregates cross the wire.

**The rejection is reported, not just taken.** `aggPushdownCheck` returns a reason and the number of
sources that tripped it, on every aggregate span. The three causes — query shape, store layout,
boundary mismatch — are unrelated and need separating; `ADMIN.md` reads them operationally.

**Trade-off worth knowing about.** A whole-part stat is exact only for a part lying entirely inside the
query range, so eligibility falls as parts grow: compaction — otherwise strictly good, giving fewer
parts, less per-part overhead and better compression — works against this pushdown. 71 small parts give
a 6h dashboard window many parts it wholly contains; the same data compacted to 3 gives it none, and
every such query drops to decoding every sample. The engine deliberately does not arbitrate: a merge
policy keeping parts small enough for a *particular* window would guess at the query mix and give up
the compaction wins for every other read. Reporting the reason instead lets an operator weigh it.

**Buckets accumulate in a `stepGrid`,** allocated once per call and reused across every series in it: a
dense array indexed by arithmetic on the timestamp, so filling costs no hashing and draining no sort of
aggregate structs. It spans the plan's *data* — parts' ranges ∪ head span, clipped to the request — not
the request, routinely unbounded on one side. A grid too wide to index densely, a fine step over a long
span, falls back to a map sized by the samples.

### Overlapping windows

`AggregateWindow`/`AggregateWindowNamed` answer the *overlapping* range-vector shape: one aggregate per
step-aligned evaluation timestamp `t` over the half-open window `(t-W, t]`, where `W` may be many steps
wide — a 1h range at a 5m step is a 12× overlap. Cost stays proportional to the data in the request,
not to the overlap factor:

1. Samples fold **once** into disjoint fine buckets on the same `stepGrid`, so the sidecar pushdown
   still applies and a part inside one fine bucket never decodes.
2. A **sliding accumulator** walks each series' buckets once, adding the bucket entering a window and
   subtracting the one leaving.

Count and sum slide by arithmetic. An extremum cannot be subtracted back out — dropping the current
minimum would force a rescan — so min/max ride **monotonic deques** of entry indices: an arrival pops
every tail entry it dominates, such an entry being no better *and* expiring no later, leaving the front
as the answer, each entry pushed and popped once for O(1) amortized steps.

**The fine grid is left-open** (`(b, b+step]`, unlike `AggregateStep`'s `[b, b+step)`), the only
convention a half-open window edge never splits. The decomposition is exact only when `W` is a multiple
of the step; otherwise an edge falls inside a bucket and the call slides over merged raw samples.

**The grid is anchored.** `WindowSpec.Anchor` names a timestamp on it and windows end at
`Anchor + k*Step`. An evaluation grid belongs to the query, not the clock — PromQL anchors at the
query's start, a multiple of the step only by coincidence — so the fine buckets are phased to match, a
window edge never falling inside one. The zero value is the absolute grid.

Both forms drain one internal `iter.Seq2`, so only one series' windows are resident rather than
series × steps. That also shapes the instrumentation: decode and fold alternate per series, so they are
accumulated durations on one span plus a planning child span, not sub-spans (`ADMIN.md`).

## Cluster surface

| call | contract |
|---|---|
| `ApplyPrimary(walBytes)` | the shard's single authoritative accept/reject decision |
| `ApplyReplicated` | applies a payload verbatim, like WAL replay |
| `RefreshReplica` | reloads parts from the store and trims the head, series-scoped |

`ApplyPrimary` OOO-checks and admission-checks each sample, returning the accepted set re-framed plus a
per-reason reject breakdown. `RefreshReplica` trims only series actually present in the flushed parts;
a global trim would leave the primary the sole holder of quorum-acked backfill.
