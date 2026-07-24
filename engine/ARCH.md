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
append buffers. Samples older than `newest − OOOWindow` are rejected. **The series index outlives a
flush** (only sample buffers drain), so flushed series stay queryable and re-appends don't re-index.

`AppendBatch` is the hot path: a metric's **precomputed** `SeriesID` + columns + a `materialize`
callback invoked only on first sight, ingested under a **single lock**. Per sample `appendByID`
does one map probe — a present buffer means the series is known, so no `signal.Series` is built or
hashed. WAL frames are grouped by series and written once per batch, not per sample.

## Flush

Drains the head into one flat 3-column part `[series:int128, ts:int64, value:float64]`, one row per
sample, sorted by `(series, ts)`, under `{tenant}/metrics/{seq}`. It then updates the two durable
index objects: the **bucket index** (part list + time bounds) and the **identity index**
(`series.bin`). Merge updates both too, committing the new part set *before* deleting sources.

`MaxPartBytes` bounds output: flush splits at the cap, a merge splits at the taller
`mergeHeight × MaxPartBytes` so same-tier siblings promote instead of re-splitting. Splitting at
row boundaries is safe — parts are independent and a series spanning two is merged back by the read
seam.

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
dropping samples past the retention cutoff, downsampling by tier, and — when the merged part is
**fully cold** — recompressing and/or re-encoding at a lossy precision budget. All five are the one
merge engine; no parallel subsystem.

- **Determinism:** `Before`/`retainFrom` are absolute timestamps, never clock reads; the caller
  resolves policy against one `now` per pass. Downsample buckets align to the absolute grid, so a
  rollup is independent of when the merge runs.
- **Fixed points:** repeated merges are stable for last/first/min/max/sum/avg (count is the
  documented exception); recompression checks the part's recorded algorithm and precision checks
  the manifest's recorded budget, so re-merges don't churn.
- **Weight-aware:** compaction and rollup both honor the lossy-sampling scale factor, so a sampled
  series stays unbiased.
- **Recompression is decode-transparent** — the reader keys off the per-column algorithm in the
  manifest, so it is a pure ratio/CPU trade with no format change.
- **Size-tiered selection** (`compact.go`) picks only what is worth merging: any part a forced
  rewrite must touch (retention/downsample/recompress/precision — so age-driven work is never
  starved), plus the largest group of same-tier *unsealed* parts. A part at the merge cap is
  **sealed** — re-merging it would only re-split it. Part count is bounded at
  ≈ dataset / (mergeHeight × MaxPartBytes) instead of growing per flush.
- **Streaming both ways** (`compactStream`): each source is read through a forward cursor decoding
  one series range at a time, and merged rows accumulate in one reused buffer flushed at the cap —
  so working set is O(parts × one series range) + one part's output, not O(dataset). This is what
  keeps background merge memory bounded as parts grow.

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
timestamp. Layered optimizations, each opt-in:

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
  decoded column bytes, reserved once per query off the lock (never incrementally, so two queries
  can't deadlock holding partial reservations); a query bigger than the whole budget is admitted
  alone. The facade builds **one** budget for all tenants, so the cap is process-wide. It bounds
  the query-concurrency RSS cliff.
- **Recent tier** (`Config.RecentWindow`) — mirrors the most recent flush window in RAM across
  flushes, so a query inside the window acquires **no part at all**; overlap with the part is
  deduped by the freshest-wins timestamp merge.
- **Buffer recycling** (`Request.Recycle` + `Batch.Release`) — opt-in, default-off. Result buffers
  come from a GC-stable doubly-bounded freelist (not `sync.Pool`, which empties at every GC and
  lost the capacity under allocation-driven collections).

## Aggregate pushdown

`AggregateRange`/`AggregateStep` return per-series count/sum/min/max (→avg) over a window or a
step-aligned grid. With `Config.AggregateStats`, each part writes a small **stats sidecar**
(`{prefix}/stats`), and a range that **fully covers** a part folds it **without decoding the value
column**. Taken only when provably exact — in-window parts fully covered *and* pairwise
time-disjoint — else it falls back to decode+merge, which dedups. Derived, so absent/corrupt ⇒
decode. In cluster mode the pushdown survives the network: each owner aggregates locally and ships
per-series identity + buckets, so only aggregates cross the wire.

## Cluster surface

`ApplyPrimary(walBytes)` OOO-checks and admission-checks each sample and returns the accepted set
re-framed plus a per-reason reject breakdown — the shard's **single authoritative decision**.
`ApplyReplicated` applies a payload verbatim (like WAL replay). `RefreshReplica` reloads parts from
the store and trims the head — **series-scoped**: only for series actually present in the flushed
parts, since a global trim would leave the primary the sole holder of quorum-acked backfill.
