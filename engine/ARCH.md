# `engine/` — the metrics vertical

One `Engine` per tenant (or per metric shard) ties index, parts and WAL into a working
ingest+query path. Its structural twin for logs/traces/profiles is
[`../recordengine/ARCH.md`](../recordengine/ARCH.md); both share the locking discipline below.

## Locking discipline (shared with `recordengine`)

**The engine lock is never held across object-store I/O.** Parts are immutable, so every phase
splits into plan-under-lock → I/O off-lock → publish-under-lock:

- The `parts` slice is **copy-on-write**, so a reader that snapshots under the lock keeps a stable
  backing array after releasing it.
- **Fetch** plans under the read lock (resolve matchers, snapshot + `acquire()` in-window parts,
  seed from the head), then reads columns lock-free and releases.
- **Flush/merge** plan under the lock, build the part off the lock, then publish under it —
  swapping the parts slice *and* committing the small bucket-index + WAL checkpoint together, so
  the durable watermark stays atomic with part discoverability (exactly-once crash consistency).
  Only the maintenance loop mutates `parts`, so the swap is single-writer.
- Retired parts are **refcounted and reclaimed deferred**, so a lock-free fetch never races a
  delete.
- A flush **detaches** the head buffers into a `flushing` set that fetches still read, swapped for
  the part atomically at publish — a record is visible in exactly one of the two, never neither
  (visibility gap) nor both (double count).

## Head

In-memory write buffer: the index (`symbols`+`series`+`postings`) plus per-series `(ts, value)`
append buffers. `OOOWindow` is a **per-series** lateness bound: a sample more than `OOOWindow` behind
*that series'* own newest admitted sample is rejected (`head.seriesNewest`), so a fast or
clock-skewed-ahead series cannot shed the slower series sharing the head, and a series' first sample
is never out of order. The watermarks outlive a flush — samples becoming durable does not reset a
series' lateness bound. **The series index outlives a flush** (only sample buffers drain), so flushed
series stay queryable and re-appends don't re-index.

`AppendBatch` is the hot path: a metric's **precomputed** `SeriesID` + columns + a `materialize`
callback invoked only on first sight, ingested under a **single lock**. Per sample `appendByID`
does one map probe — a present buffer means the series is known, so no `signal.Series` is built or
hashed. WAL frames are grouped by series and written once per batch, not per sample.

## Flush

Drains the head into one flat 3-column part `[series:int128, ts:int64, value:float64]`, one row per
sample, sorted by `(series, ts)`, under `{tenant}/metrics/{seq}`, together with the part's sidecars —
the series index (`sidx`), the optional aggregate stats, and the part's **identity object**. It then
updates the **bucket index** (part list + time bounds). Merge does the same, committing the new part
set *before* deleting sources.

### Identity is part-scoped

Each part carries the identities of the series it holds (`{part}/identity`, format in
`index/identity`), written with the part's other objects and deleted with them. Three properties
follow, and they are why the whole-set `series.bin` this replaces is gone:

- **Retention is self-cleaning.** Dropping a part drops the identities that named its rows.
- **A flush persists what it wrote**, not the whole set — 88 B for a flush adding one series to a
  20k-series tenant, where the old object was re-serialized in full whenever the set changed
  (~40 B/series interned, against ~218 B/series repeating the label bytes per series).
- **Every node derives its own live set**, so a replica prunes identity from its own parts with no
  ownership rule (it reaches the prune through its refresh, since it never merges).

A prefix written by an older build still has `series.bin`: it is read at open and **deleted once
every live part carries its own identities**, completing the migration and stopping recovery from
resurrecting identities whose data is gone. Identity objects are written and read uncached — read on
recovery and by a replica adopting a part, never on the query path.

**The WAL must therefore carry its own identities.** A flush checkpoints it, discarding the series
records written when identities were first seen, so a series record is logged whenever a series
starts a **new sample buffer** — once per actively-appending series per flush window — not only when
its identity is new. Otherwise a sample logged after a checkpoint would reference a series the log no
longer describes, and with identity in the parts (which retention can drop at any time) there is no
whole-set object left to resolve it through.

The resident half of that identity state — the head's symbol table, series index, postings lists and
per-series OOO watermarks — is metered separately as `Stats.IdentityBytes`, not folded into
`HeadBytes`: a flush drains samples, not identities, so folding the two would have the
size-triggered flush chase a number it cannot lower.

## Identity prune

Retention drops samples and whole parts; `PruneIdentities` drops the identities they leave behind,
which otherwise accumulate for the process' lifetime under series churn. It needs no new on-disk
format — a part's `sidx` sidecar already lists its series ids, so the **live set** is the union of
the live parts' id sets with the in-memory tiers (head, mid-flush detachment, recent). It runs after
a merge — and, on a replica, after the refresh that adopts a new part set — and only when parts
actually went away: identities die no other way, so an engine whose data only grew skips even the
live-set walk. No ownership rule is needed now that identity is scoped to the part: a node's live set
means exactly "what this node can still serve", and a part a replica has not synced yet brings its
identities with it.

Symbol ids are dense and referenced by the postings lists, so nothing can be removed in place:
the survivors are **rebuilt** into fresh structures. That rebuild is ~0.6 s per 200k identities
(~3.4 s at 1M), far too long to hold `e.mu`, so it runs **off-lock** against
`series.Index.Snapshot()` — the index's append-only entry log, which registration only extends, so
a snapshot of it is immutable without the lock. Under the lock the swap then reconciles what changed
meanwhile and installs the result: ~5 ms per 200k pruned identities, ~40 ms at 1M, allocation-free.

Two sets must survive a rebuild that decided without them: entries registered *after* the snapshot
(past its end in the same log), and series the prune found dead that **regained samples** — a series
whose identity the old index still held is not re-registered when a sample arrives, so it leaves no
log entry, and dropping it would strand samples the next flush would write into a part with no
identity naming them. A dead series' OOO watermark is deleted by key (cost tracks what died, not
cardinality); a live one keeps its own, or a late sample would be re-admitted.

The WAL needs no `walExpiries` equivalent: it is checkpointed at every flush and re-logs a series
record whenever a series starts a buffer, so its live records name only series it describes itself.
The prune writes nothing durable — the identities it drops went with the parts that held them.

The record engines have the same prune (`recordengine/prune.go`), reaching the live set from their
resident `part.ranges` instead of a sidecar read. What is still unbounded there is the **union-only
merged symbol sidecars**, which need a shrink path of their own.

**Publish order: the part's objects first, the bucket index last.** The bucket index is what makes a
part durably visible, so writing it is the commit point, and a readable part always carries the
identities its rows resolve through — a committed part whose identities are missing would hold
samples no matcher can reach. A crash in between leaves an orphan part, objects and identity
together, swept at the next open, so a failed publish strands nothing (with a whole-set object its
identity was instead left behind permanently, resolvable and backed by nothing). Unlike `recordengine`, the metrics bucket index carries no `FlushedEpoch` — the WAL
`Checkpoint` (which runs last, after both) is the WAL's commit point, so with durability enabled
replay still recovers a part that failed to publish. The unreadable-commit window bites the
backend-only configuration (a persistent backend with no `WALDir`), where nothing else holds the rows.

Flush and merge are bounded separately, because they answer different questions. A flush splits at
`MaxPartBytes` — approximate *uncompressed* bytes, since it is sizing rows it already holds in the
head. A merge splits at `mergeCapBytes`, a size **on disk**, measured against what the streaming
writer has actually encoded (`mergecap.go`).

That cap is derived from the resources the merge actually consumes rather than held at a constant:
`min(MergeCeilingBytes, free / MergeConcurrency / 2, mergeMemory / MergeConcurrency / 2)`. A byte constant is correct at exactly one
deployment size — cardinality consumes a byte budget in *breadth*, so under a fixed cap the time
span a part covers is inversely proportional to active series and a fixed-range query opens
proportionally more parts as cardinality grows. Both references size against the storage instead
(VictoriaMetrics `getMaxOutBytes`, ClickHouse `max_bytes_to_merge_at_max_space_in_pool` lowered by
free space); dividing by the merge worker count is what stops concurrent merges from collectively
filling the disk, and the further halving leaves room for a merge's output to coexist with the
inputs it has not yet retired.

`MergeConcurrency` is a callback, not a number, because the fan-out is bounded by the node's engine
count as much as by its worker limit and engines appear lazily. Fixing it at engine creation would
divide a single-tenant node's disk by its core count — on a 32-core box that lands back at roughly
the constant this replaces, on the exact deployment shape the change is for.

A backend that cannot report free space — `Memory`, object stores, where local free space has no
meaning — keeps the ceiling, so every backend still works. A nearly full disk falls to
`minMergeCapBytes` rather than sealing everything, since stranding the part count high is worst
exactly when compaction matters most. The floor applies to the *derived* bounds only: a ceiling
configured below it is honored, since an embedder that sets one means it.

**Memory bounds the cap independently of disk, and usually binds first.** `block.StreamWriter`
streams the *inputs*, not the output: the encoded output part accumulates in RAM until it is sealed,
and `build` then serializes it into one buffer per column, so a merge's peak resident is about twice
the part it is writing. Free space says nothing about that — a 4 GiB pod over a 464 GiB volume
derives a 232 GiB share, clamps it to the 16 GiB ceiling, and OOMs building the part. So the cap is
also lowered by `MergeMemoryBytes / MergeConcurrency / 2`: the merge allowance divided across the
merges that may run at once, halved for the serialize step. The allowance defaults to an eighth of
the process's memory budget — `GOMEMLIMIT`, else the cgroup limit, else host memory
(`internal/memlimit`) — leaving the rest to the head, the caches and the decode budget. Until the
writer can spill a part to the backend incrementally, part size *is* merge memory, and no disk-derived
figure may override that.

Splitting at row boundaries is safe — parts are independent and a series spanning two is merged back
by the read seam.

Driven by the facade's single background maintenance loop, plus a head-bytes pressure trigger that
flushes just the over-threshold engines. Concurrency is nonetheless enforced, not assumed: flush and
merge run under one `flushMu` held across their whole body, since `Engine` is exported and
`Close`/`Reset` are callable from anywhere.

`Reset` takes it too — it drains an in-flight flush/merge (which would otherwise publish its part,
and a stale sequence, into the emptied engine), drops the detached flushing buffers with the head,
and **retires** the live parts instead of deleting their objects outright, so a fetch that already
acquired one is not read out from under it; the deferred reclaim deletes them once the reader
drains.

**Part sequences are append-only.** Flush and merge reserve each output part's `{seq}` as they write
it and advance the counter immediately, so an attempt that failed after writing some objects burns
its sequence rather than handing it to the retry — a rewrite replaces only the objects it itself
produces, so a part landing on the leftovers inherits objects it never wrote. `LoadParts` sweeps the
residue: it lists the prefix, deletes every object under a part directory the bucket index does not
name (a failed attempt's, or a retired part whose reclaim delete failed), and resumes the sequence
past the highest one seen on the backend — so the guarantee survives a restart, where the index
alone would hand the orphan's sequence back out. The sweep assumes this node **owns** the prefix, so
a replica's `RefreshReplica` skips it: the owner's in-flight part is not in the index yet.

## Merge — one pass, five modes

`MergeWith(MergeOptions{RetainFrom, Downsample, Recompress, Precision})` compacts a **bounded,
size-tiered group** of parts (not the whole set), merging per series by timestamp (freshest wins),
dropping samples past the retention cutoff, downsampling by tier, compressing the output at a
size-graduated level, and — when the merged part is **fully cold** — recompressing at the age tier
and/or re-encoding at a lossy precision budget. All of it is the one merge engine; no parallel
subsystem.

- **Determinism:** `Before`/`retainFrom` are absolute timestamps, never clock reads; the caller
  resolves policy against one `now` per pass. Downsample buckets align to the absolute grid, so a
  rollup is independent of when the merge runs.
- **Fixed points:** repeated merges are stable for last/first/min/max/sum/avg (count is the
  documented exception); recompression checks the part's recorded algorithm *and level* and precision
  checks the manifest's recorded budget, so re-merges don't churn. Only an upgrade forces a rewrite —
  a part denser than the target is left alone.
- **Weight-aware:** compaction and rollup both honor the lossy-sampling scale factor, so a sampled
  series stays unbiased.
- **Graduated compression** (`recompress.go`): a merge always compresses its output, at a level its
  row count selects (zstd 1 ≤ 64k rows, 2 ≤ 1M, else 3 — VictoriaMetrics' ladder, capped the same
  way). A merge rewrites the data regardless, so the only cost is the level's own CPU, and a bigger
  part — older, read less per byte, merged again less often — earns a denser level. Above the ladder
  sits at most one *age* tier, `RecompressSpec`, applied to a fully cold part. A hot flush is
  unaffected: it still writes codec-only framing.
- **Recompression is decode-transparent** — the reader keys off the per-column algorithm in the
  manifest, so it is a pure ratio/CPU trade with no format change. The *level* is recorded too, but
  only so the merge can tell a part already at the target from one below it; nothing reads it back.
- **Run selection** (`compact.go`) picks only what is worth merging: any part a forced rewrite must
  touch (retention/downsample/recompress/precision — so age-driven work is never starved), plus the
  best run of *unsealed* parts. A part at the merge cap is **sealed** — re-merging it would only
  re-split it — so part count is bounded at ≈ dataset / cap instead of growing per flush. Sealing
  and the run budget are in on-disk bytes (`part.sizeBytes`); a run is bounded by `maxMergeParts` as
  well, so a large disk-derived budget cannot turn one merge into an unbounded one.

  Parts are **ordered** by size and every run of adjacent ones is scored `m = output / largest
  input`, the inverse of write amplification (VictoriaMetrics' `appendPartsToMerge`). They are not
  *bucketed* by size, which is what used to strand leftovers: a part alone in its power-of-two tier
  could never reach the two-per-tier threshold, and nothing would ever land in exactly that tier
  again, so it was carried by every query for the engine's lifetime (#285).

  Ordering alone does not fix it, because those leftovers are spread geometrically — one per former
  tier — so every run over them fails the balance test or scores under `minMergeMultiplier`. Two
  escapes close it: a run whose *total* is under `smallRunBytes` skips both guards (they argue about
  the proportion of bytes rewritten, which is meaningless when the whole rewrite is cheap), and
  after `mergeIdleRounds` merges that selected nothing, the best run is taken regardless of score.
  Rewriting a large part once to absorb a stray is far cheaper than carrying that stray forever.

  The selector works in size order but returns the run in the engine's part order: the merge visits
  its sources oldest → newest so a later part's value wins a duplicate timestamp.
- **Streaming both ways** (`compactStream`): each source is read through a forward cursor decoding
  one series range at a time, and each merged series is handed straight to a `partStreamWriter`
  (`streampart.go`) wrapping a `block.StreamWriter`, which encodes a granule as soon as one fills.
  So the working set is O(parts × one series range) + the *encoded* output part — not O(dataset),
  and not the output part's uncompressed rows either. That second half matters for granularity: the
  row cap used to set peak merge memory as well as part size (`capRows × 32 B`, 512 MiB at the
  default cap), so a cap raised to widen parts raised merge RSS with it. Now it only sets part size.
  Measured on a 2M-row merge, streaming the output cut total allocation 3.7–5.4× and peak heap
  2.9–3.8×, the wider margin on counter-shaped data that encodes densest.

  The sidecars stream with it: the series index, identities and aggregate stats are all built from
  the same per-series calls, each run- or series-shaped, so they cost O(distinct series) not O(rows).
  Encoding decisions that the batch writer takes per output part — compression profile, precision
  budget, whether a weight column exists — must instead be fixed before the first row is encoded, so
  `mergeEncoding` derives them from the source parts; see its doc for why that is equivalent (and
  self-correcting where it is not exact).

  The **single-part forced-rewrite path** (`writeColumns`, one part selected for
  retention/downsample/recompress/precision) still buffers whole columns: its fixed-point check —
  skip the rewrite if the part is already at its target — needs the post-downsample row count before
  it can decide to write at all. It is bounded by one part, and is not the routine compaction tick.

### Publish ordering

A merge retires its source parts — queues them for backend deletion — **only after** the bucket index
naming their replacement is committed. The index is what a restart and every other replica read, so a
part the persisted index still names must never become reclaimable; retiring first would let the next
maintenance tick's reclaim delete objects the index references, and `LoadParts` hard-fails the whole
engine on a missing part. A failed commit rolls the in-memory part swap back to the committed set, so
the uncommitted output is never observable as published; its objects are orphans, swept by the
`LoadParts` orphan sweep at the next open.

## Read path

`Fetch` resolves matchers over the index, then merges each series' head buffer ∪ every part by
timestamp — **one series per `Next`**, so a consumer that folds and releases each batch never has
more than one series' samples resident, whatever the matched count. The plan (acquired parts, decode
reservation, head snapshots) and the fetch's span/profile/metrics therefore span the whole iteration
and are settled by `Close`, which the caller owes even when it stops early. What remains O(matched
series) is the plan itself: one identity per matched series, and the head/mid-flush snapshots — those
must be copied under the lock, since a concurrent flush moves a series' head buffer into a part the
plan did not acquire.

Layered optimizations, each opt-in:

- **Series index sidecar** (`{prefix}/sidx`) — sorted distinct SeriesIDs + run-start rows as
  fixed-width entries, binary-searched **in the raw bytes**, held only while a fetch is reading the
  part (re-fetched through `backend.ReadView`, a zero-copy cache hit). So resident index memory is
  governed by the read cache budget rather than series count, and opening a part reads no series
  column. It is derived — a missing/corrupt sidecar falls back to scanning the series column once
  into a resident index — so it carries no format-migration burden.
- **Block slicing + decode cache** (`Config.DecodeCacheBytes`) — with block-framed parts, a fetch
  slices the spanning column blocks straight from a byte-bounded LRU keyed by
  `(part, column, block)` and adds them to the merge as **views**, never materializing a whole
  decoded part. Entries are immutable and **reference-counted**, released per series as soon as the
  samples are copied out; an evicted+unpinned buffer recirculates through a bounded freelist that
  the next miss decodes into, cutting miss-path allocation rate without enlarging the resident set.
  Cache-off (or constant/unblocked columns) falls back to a per-fetch decode, **series-skipped** —
  only the blocks the matched row ranges touch. With the cache on a fetch also **prefetches** the
  parts it will touch, so backend reads and decodes overlap.
- **Decode-memory budget** (`Config.DecodeMemoryBytes`) — a shared byte semaphore over in-flight
  decoded column bytes, reserved once per *fetch* off the lock (never incrementally per part, so
  two queries can't deadlock holding partial reservations); a fetch bigger than the whole budget is
  admitted alone. The facade builds **one** budget for all tenants, so the cap is process-wide. It
  bounds the query-concurrency RSS cliff.
  - Per-fetch is only deadlock-free while a caller keeps **one** fetch open at a time — true of a
    drain-then-close consumer, false of a streaming one that holds several iterators across an
    evaluation. There the second acquire is hold-and-wait against the query's own reservation, and
    the admit-alone escape cannot fire: `used` is non-zero precisely because this query holds it.
  - `Request.Scope` (a `fetch.Scope`) names the logical query, the read-side analogue of
    Prometheus' `Storage.Querier`: the caller makes one per request and passes it on every read
    under it. Its first read blocks as usual; while it still holds, later reads sharing the scope
    are charged **without queueing**. Accounting stays exact — only the waiting is skipped — so
    such a query may overshoot the ceiling by its own later estimates, the same latitude a single
    over-budget read already has. A nil scope is unchanged.
- **Recent tier** (`Config.RecentWindow`) — mirrors the most recent flush window in RAM across
  flushes, so a query inside the window acquires **no part at all**; overlap with the part is
  deduped by the freshest-wins timestamp merge.
- **Buffer recycling** (`Request.Recycle` + `Batch.Release`) — opt-in, default-off. Result buffers
  come from a GC-stable doubly-bounded freelist (not `sync.Pool`, which empties at every GC and
  lost the capacity under allocation-driven collections).

## Count and series enumeration

`Count`/`CountBy`/`Series` share one **existence plan**: matched ids from the head index (which
outlives a flush) and in-window existence from the live buffers, with no batch, no value column and
no label projection. They differ in what they ask of the parts:

- `Count`/`CountBy` are **exact**: a part fully inside the window contributes by sorted intersection
  against its series index (**no** decode); a window-edge part decodes its timestamp column only and
  binary-searches. So at most the two edge parts decode.
- `Series` is **series-only**: every window-overlapping part contributes the matched ids its series
  index holds — no column is ever read, so the cost is proportional to matched cardinality alone, not
  to the window's depth. The window filter is therefore **part-granular** (a series in an overlapping
  part is listed even if its samples sit just outside), the same granularity `recordengine.Series`
  and Prometheus' own label endpoints have. It is the primitive the metrics label endpoints need,
  whose fetch-based answer would decode and discard every sample of every matching series.

## Label metadata

`LabelNames`/`LabelValues` answer from the **index**, not from series. With no matchers the walk is
over the postings' (name → values) map — O(distinct values), no identity materialized; with matchers
it narrows to the matched ids and reads only the requested name off each identity (never a whole
label set).

The head's series index is **all-time**: it outlives flushes, and retention prunes samples and parts
but never identities, so the index alone would keep offering the labels of series whose data is long
gone. Every value is therefore **liveness-probed** — an in-window in-memory sample, or membership in
a part overlapping the window — stopping at the first live series, so a live value costs one probe
rather than a scan of its postings list. The part indexes are warmed *before* the engine lock is
taken, keeping the probe I/O-free under the lock.

## Aggregate pushdown

`AggregateRange`/`AggregateStep` return per-series count/sum/min/max (→avg) over a window or a
step-aligned grid. With `Config.AggregateStats`, each part writes a small **stats sidecar**
(`{prefix}/stats`), and a range that **fully covers** a part folds it **without decoding the value
column**. Taken only when provably exact — in-window parts fully covered *and* pairwise
time-disjoint — else it falls back to decode+merge, which dedups. Derived, so absent/corrupt ⇒
decode. In cluster mode the pushdown survives the network: each owner aggregates locally and ships
per-series identity + buckets, so only aggregates cross the wire.

Buckets accumulate in a **`stepGrid`** allocated once per call and reused across every series in
it — a dense array indexed by arithmetic on the timestamp, so filling costs no hashing and draining
costs no sort of aggregate structs. It spans the plan's *data* (parts' ranges ∪ head span, clipped
to the request), not the request, which is routinely unbounded on one side; a grid too wide to index
densely (a fine step over a long span) falls back to a map, sized by the samples instead.

### Overlapping windows

`AggregateWindow`/`AggregateWindowNamed` answer the *overlapping* range-vector shape — one aggregate
per step-aligned evaluation timestamp `t` over the half-open window `(t-W, t]`, where `W` may be
many steps wide (a 1h range at a 5m step is a 12x overlap). Cost stays proportional to the data in
the request, not to the overlap factor: samples fold **once** into disjoint fine buckets on the same
`stepGrid` (so the sidecar pushdown still applies — a part inside one fine bucket never decodes),
and a **sliding accumulator** then walks each series' buckets once, adding the bucket that enters a
window and subtracting the one that leaves. The fine grid is **left-open** (`(b, b+step]`, unlike the
`[b, b+step)` of `AggregateStep`) — the only convention a half-open window edge never splits.

Count and sum slide by arithmetic; an extremum cannot be subtracted back out (dropping the current
minimum would force a rescan), so min/max ride **monotonic deques** of entry indices — an arrival
pops every tail entry it dominates, since such an entry is no better *and* expires no later, leaving
the front as the window's answer. Each entry is pushed and popped once, so a step stays O(1)
amortized.

The decomposition is exact only when `W` is a multiple of the step; otherwise a window edge can fall
inside a bucket, and the call falls back to sliding over the merged raw samples of each series.

The grid is **anchored**: `WindowSpec.Anchor` names a timestamp on it, and windows end at
`Anchor + k*Step`. An evaluation grid belongs to the query, not to the clock — PromQL anchors at the
query's start, which is a multiple of the step only by coincidence — so the fine buckets are phased
to match, since a window edge must never fall inside one. The zero value is the absolute grid.

Both forms drain one internal `iter.Seq2`, so only one series' windows are resident while it is
computed rather than series × steps of them.

## Cluster surface

`ApplyPrimary(walBytes)` OOO-checks and admission-checks each sample and returns the accepted set
re-framed plus a per-reason reject breakdown — the shard's **single authoritative decision**.
`ApplyReplicated` applies a payload verbatim (like WAL replay). `RefreshReplica` reloads parts from
the store and trims the head — **series-scoped**: only for series actually present in the flushed
parts, since a global trim would leave the primary the sole holder of quorum-acked backfill.
