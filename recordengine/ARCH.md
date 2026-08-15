# `recordengine/` — the shared record engine (logs · traces · profiles)

Record-shaped signals are a **stream** (a Resource+Scope identity, indexed by postings exactly like
a metric series) of rows carrying a primary timestamp plus a fixed set of typed columns. Unlike a
metric's `(ts, float)` sample, a record's fields vary *within* the stream, so they are **columns
filtered by predicate**, not identity — the dual-shape contract: **Matchers resolve the stream,
Conditions filter its records** (see [`../query/ARCH.md`](../query/ARCH.md)).

All three signals share this engine; only the column schema, the projection, and (profiles) a side
store differ. It is the metrics engine's structural twin — head, flush, size-tiered merge with
retention, durable bucket-index + part-scoped identity stateless read path, `MaxPartBytes` splitting,
and the same lock discipline (see [`../engine/ARCH.md`](../engine/ARCH.md)). Notable divergences:

- Merge is **append-only**: retention only. Downsampling, recompression and precision are
  metrics-specific. As in the metric engine, retention **drops whole parts first** (`dropExpired`):
  a part whose `maxTime` is already past the cutoff is retired on the manifest alone, with no
  decode and no output part, so only a part *straddling* the cutoff is rewritten. A record part's
  side-store and bloom sidecars live under its own prefix, so `deletePart` reclaims them with it.
- **Merge selection is confined to an aligned time bucket** (`timebucket.go`), mirroring the metric
  engine. Size tiers have no notion of time, so a merge folded a part covering one hour into one
  covering the whole retention and every part then overlapped every query window. Parts are grouped
  by their aligned bucket on the `mergeLadder` (1h → 6h → 24h, each level dividing the next so
  buckets nest) and `pickTierGroup` runs **unchanged inside one group**; the ladder is walked
  narrowest-first, so a part is rewritten once per level rather than repeatedly at the widest. Above
  the finest level the still-filling newest bucket is skipped; the finest level is exempt, since
  that is where flushes land.

  This matters more here than for metrics: record queries are overwhelmingly narrow and recent, and
  a record row carries far more bytes than a sample, so opening a part the window did not need costs
  more.

  **Retention-forced rewrites are confined too, and win the cycle** rather than being unioned with a
  tier group — that union merged parts from opposite ends of the store into one spanning both, on
  the cycle most likely to run. The oldest forced part picks the bucket and the rest of that bucket
  rides along when it fits the cap (merging inside a bucket cannot widen). A part straddling every
  level belongs to no bucket and is rewritten *alone* rather than skipped: retention correctness
  does not wait on straddle splitting.
- Records are variable-width, so every size is **measured, not modeled**. `MaxPartBytes` is spent in
  the *decoded* bytes a row holds (`flushColumns.rowBytes`), the flush splits on them
  (`byteRanges`), a part records its decoded footprint in its manifest (`Manifest.RawBytes` →
  `part.sizeBytes`), and the size tiers and the seal threshold compare those. A row count cannot
  stand in for any of it: the same count is ten 1 MiB records or ten thousand 1 KiB ones, and the
  assumed-average-row-size model that preceded this was wrong by that ratio — a 256 B assumption
  against ~950 B real rows decoded ~4× the intended bytes per merge. `recordRowBytes` survives only
  as the fallback for a part written before the manifest carried the figure.

- The merge cap is `min(mergeHeight × MaxPartBytes, MergeMemoryBytes / MergeConcurrency / 3)`,
  floored at one flushed part so retention can always rewrite one. The memory term is the one the
  metric engine also grew (see [`../engine/ARCH.md`](../engine/ARCH.md)) and for the same reason: a
  cap sized against storage says nothing about what the process can hold. It is *decoded* bytes
  here rather than bytes on disk, because that is what this merge holds — the selected sources are
  decoded up front and the output accumulates decoded before it is encoded, hence dividing by three
  (sources + output buffer + the encode of it). Free space does not enter into it; the flush cap
  and the tiering target bound the disk.

- **Merge shape and forcing** (`mergeshape.go`). `Engine.MergeShape` reports the selector's inputs —
  parts, sealed, the unsealed remainder (the real backlog), what the next merge would take, the cap
  in effect, and the tier spread — because a no-op merge is otherwise indistinguishable from an idle
  engine. This engine has no idle waiver: two parts in different tiers of one bucket are a
  *permanent* fixed point, and `MergeOptions.Force` is the only way out. It takes a bucket's
  unsealed parts smallest-first whatever their tiers, still truncated at the same cumulative-bytes
  cap and still confined to one time bucket — the tier rule is waived, the memory bound is not.

## Schema

`Schema` of `Column{Name, Kind(Int64|Bytes), Codec, Bloom(None|FullText|Attrs|Equality)}`; the
timestamp sort key and the int128 stream id are implicit. A signal projects its model into the
engine's column vectors; the engine treats the columns **opaquely**.

## Byte columns

Head buffers, fetch accumulators and the part read path all use a contiguous **offsets+blob**
layout (`byteCol`: one `[]byte` blob + `[]int32` row end-offsets) rather than `[][]byte` — the GC
scans two headers per column instead of one per row, and a scan walks one allocation with locality.
Cell views alias the blob under the **read-only-until-next-append** rule (an append may move the
blob, so a value retained past one is copied); `fetch.NamedColumn` materializes views at the
boundary, pooled across recycled fetches.

Flush is a **pass-through**: `block.Column` accepts the blob+offsets form directly, encoded
byte-identically to the per-row form, so writing a part never materializes a view per row. The
per-stream ts ordering is applied **at copy time**: the flush computes each stream's ts permutation
and gathers rows into the flush buffer through it, never sorting the source. That is a correctness
requirement, not a preference — the detached buffers stay fetchable through `e.flushing` while the
part is written off the lock, so a concurrent fetch is reading them (§ Flush failure). An
already-ordered stream — the common case — computes no permutation at all.

## Flush failure

A flush detaches the head, then writes the part off the lock — so every step after the detach must be
**undoable**. Any error before the publish folds the detached buffers back into the head (merging with
whatever arrived meanwhile) and restores the side-store snapshot via `SideStore.Restore`, then clears
`e.flushing`. Nothing else holds those records: the part was never published, and the WAL checkpoint
only runs on a successful publish, so without the fold-back the rows would be lost the moment the next
flush overwrote the in-flight buffer.

## Stream identity is part-scoped

Each part carries the identities of the streams it holds (`{part}/identity`, format in
`index/identity`), written with the part's other objects and deleted with them — the same shape as
the metrics engine. Retention is therefore self-cleaning, a flush persists the identities it wrote
(88 B for one new stream, ~40 B/stream, against a whole-set `streams.bin` re-encoded and rewritten on
**every** flush and merge whether or not the set changed), and every node derives its live identity
set from its own parts. A prefix written by an older build still has `streams.bin`: it is read at
open and **deleted once every live part carries its own identities**.

**The WAL must carry its own identities.** A flush checkpoints it, so a stream record logged when the
identity was first seen is gone afterwards; a record is logged again whenever a stream starts a fresh
**record buffer** (`head.needsStreamRecord`) — once per actively-appending stream per flush window.
Without it, a record logged after a checkpoint would reference a stream the log no longer describes,
and with identity in the parts (which retention can drop) nothing else would name it.

## Identity prune

Retention drops records and whole parts; `PruneIdentities` drops the identities they leave behind,
which otherwise accumulate for the process' lifetime under stream churn (the `instance.id`-shaped
attributes that turned ~24 stable streams into ~15k). The **live set** is the union of every live
part's stream ids — resident already, in `part.ranges`, so unlike the metrics engine it costs no I/O
— with the head and the mid-flush detachment.

Symbol ids are dense and referenced by the postings, so the survivors are **rebuilt** into fresh
structures rather than deleted in place: off the engine lock, against `series.Index.Snapshot()`
(the append-only entry log, which registration only extends), then swapped in under the lock. The
swap re-registers what changed meanwhile — entries past the snapshot's end, and streams the prune
found dead that regained buffered records — and drops the rest's out-of-order watermarks by key.

It runs after a merge, and on a replica after the refresh that adopts a new part set, and only when
parts actually went away. No ownership rule: identity is scoped to the part, so a node's live set
means exactly "what this node can still serve". `Admin.PruneIdentities` forces a sweep past the
size/dead-fraction thresholds for an operator.

## Publish ordering

Publishing a flush writes the part's objects — identity included — **first** and the bucket index
**last**, then checkpoints the WAL. The bucket index carries `FlushedEpoch` — the watermark replay
starts from — so writing it is the commit point, and only what is already durable may be committed. A
committed part whose identities were missing would be unrecoverable: it holds rows no matcher can
name, while the advanced watermark makes replay skip the WAL records that would have re-registered
them. The reverse leftover is harmless — an uncommitted part is an orphan the next open sweeps, and
its identities were never loaded.

## Merge publish ordering

A merge retires its source parts — queues them for backend deletion — **only after** the bucket index
naming their replacement is committed. The index is what a restart and every other replica read, so a
part the persisted index still names must never become reclaimable; retiring first would let the next
maintenance tick's reclaim delete objects the index references, and `LoadParts` hard-fails the whole
engine on a missing part. A failed commit rolls the in-memory part swap back to the committed set, so
the uncommitted output is never observable as published; its objects are orphans, swept at the next
open.

## Part sequences & orphans

Part prefixes (`<prefix>/%010d`) are **append-only**: flush and merge reserve each output part's
sequence as they write it and advance the counter immediately, so an attempt that failed after
writing some objects burns its sequence rather than handing it to the retry. Reuse would be
unsound — a rewrite replaces only the objects it itself produces, and two of a part's objects are
conditional (`keys.bin` is skipped when the rows carry no record attributes, the `sym-*.bin`
sidecars when there is no side data), so the new part would silently adopt the failed attempt's.

The leftovers are **swept at open**: `LoadParts` lists the engine prefix and deletes every object
under a part directory the bucket index does not name (a failed attempt's, or a retired part whose
reclaim delete failed), and resumes the sequence past the highest one seen on the backend — so the
guarantee survives a restart, where the index alone would hand the orphan's sequence back out.
The sweep assumes this node **owns** the prefix, so a replica's `RefreshReplica` skips it: it shares
the store with the owner, whose in-flight part is not in the index yet.

## Lifecycle guards

Flush and merge run under one `flushMu` held across their whole body — both mutate the parts slice,
reserve part sequences, and reuse the flush column buffer off the engine lock. The facade drives them
from a single maintenance goroutine, but `Engine` is exported and `Close`/`Reset` are callable from
anywhere, so the single-mutator invariant is enforced rather than assumed.

`Reset` takes it too: it drains an in-flight flush/merge (which would otherwise publish its part —
and a stale sequence — into the emptied engine), drops the detached `flushing` buffers with the head,
and **retires** the live parts instead of deleting their objects outright, so a concurrent fetch that
already acquired one is not read out from under it. The deferred reclaim deletes them once the reader
drains.

The head has a hard byte ceiling of `headByteCap` (2 GiB) independent of `MaxInFlightBytes`: a flush
concatenates every stream's cells into one blob per byte column, indexed by `byteCol`'s int32
offsets. Past the cap records are rejected as memory backpressure — overflowing would write negative
offsets into a part. The cap is a bound on the *next* part's blobs, so it meters the live buffers
only; `MaxInFlightBytes` is memory backpressure, so it meters everything resident.

"Resident" includes the detached buffers: `head.detach` moves them aside but they (and the flush
columns built from them) live until the part is published, so their size is parked in
`head.detachedBytes` and `head.inFlightBytes` (= `HeadBytes` = `Stats.HeadBytes`) keeps counting it.
It is cleared at the publish, or handed back to the live count by `head.reattach` on a failed flush —
never both. Without it the measure would read zero for the whole duration of a slow flush, and
`MaxInFlightBytes` would admit a second full head on top of the one still being written out.

Stream *identity* is deliberately outside that measure: `Stats.IdentityBytes` reports the symbol
table, stream index, postings and per-stream watermarks on their own, since a flush drains records
but not identities (they outlive the data they named, and only `Reset` reclaims them) — the same
split as the metrics engine.

`OOOWindow` is a **per-stream** lateness bound, not a head-global one: each stream carries its own
newest-admitted watermark (`head.streamNewest`), so a fast or clock-skewed-ahead stream cannot shed
the slower streams sharing the head, and a stream's first record is never out of order. The
watermarks outlive a flush — records becoming durable does not reset a stream's lateness bound — and
are cleared only with the head. (The metrics engine still compares against a head-global `newest`.)

## Fetch

Heavily tuned around decoding as little as possible:

- **Granule time pruning** (`granule.go`) — every column of a part is **block-framed**, so a reader
  decodes one granule at a time, and the marks sidecar carries each granule's `[minTime, maxTime]`.
  A windowed fetch decodes only the granules its rows occupy. Without it part span was the *only*
  time filter records had — a 15-minute query against a day-wide part decoded the day, measured at
  286× the rows needed on a real log corpus.

  The selection is taken from the **requested streams'** row ranges, not the whole part. Rows are
  `(stream, ts)`-ordered, so granule bounds are not monotonic across a part, but each stream owns one
  contiguous ts-ascending run — which is what makes a service-filtered query touch a handful of
  granules. `nil` means "decode everything", returned both when marks are unusable and when nothing
  pruned, so the whole-column path stays on its simpler route.

  Selection walks whichever is smaller, the requested ids or the part's own streams (the latter via
  the plan's id set, built once per query). Both yield the same granules; a query with no matchers
  requests every stream in the tenant, so walking the request would cost hundreds of thousands of
  lookups per part to skip a handful of granules.

  Decoded rows land at their **part row offsets**, pruned or not, so the row-range index and
  `tsWindow` keep working unchanged. Rows outside the selected granules are *unspecified*.

  **The timestamp column is therefore never pruned** — it is decoded whole however narrow the window.
  Row selection reads timestamps directly (binary search over each stream's ts-ascending run, see
  `tsWindow`), so unspecified timestamps break the search's precondition and let it return rows the
  window never covered, whose value columns are equally unspecified. Measured on the real log corpus,
  pruning the timestamp column made a 15-minute level filter report 889,390 rows where the true answer
  is 483,076. Decoding it whole restores the invariant everything else rests on: selection is driven by
  real timestamps, and every row selection can reach lies in a granule overlapping the window — which
  is a granule pruning always keeps. The column is delta-of-delta int64 and tiny next to the bodies and
  attributes the pruning exists to skip.

  The stream id column stays unframed: a fetch resolves streams through the row-range index and never
  decodes it.
- **Lazy column decode** — materialize only the columns the request's conditions + projection
  reference (a body search projecting body touches just `ts`+`body`).
- Decode each surviving part **once**, distributing rows to per-stream accumulators pre-sized from
  row-range counts; bulk-append in-window ranges, filter in place, skip the sort when already
  ts-ordered.
- **Bloom pruning** — skip a part whose per-column bloom proves a required `Condition.Tokens`/
  `Condition.Equal` absent, then re-check per row. See [`../index/ARCH.md`](../index/ARCH.md).
- **Two-phase filtered fetch** (`fetchlazy.go`, taken for `AllConditions` + conditions — the by-id
  lookups): phase 1 decodes only ts and the *condition* columns (byte columns as lazy
  `chunk.DictColumn`, O(1) `At`) and records the matching rows; a part with no match (a bloom false
  positive) never decodes its projected columns. Phase 2 decodes the rest and gathers the recorded
  rows. Each row is matched **once**: phase 2 replays phase 1's hit list, and the post-scan
  `filterPrefix` re-applies the conditions only to the head/flushing-seeded accumulator prefix (part
  rows pass by construction).
  - **Compiled conditions** (`fetcheval.go`): each condition is resolved to its column once per part,
    then evaluated cheap-first (equality bitmap → dictionary memo → int-value memo → per-row call).
    A dictionary-encoded column's predicate is memoized per *distinct entry* (≤ 65536 of them,
    filled lazily so a high-selectivity scan still pays only per touched entry), which is what keeps
    a regex — or an attribute lookup that re-parses the `attrs` blob — off the per-row path. An int
    column memoizes per *distinct value* over a small fixed non-negative domain — where enum-shaped
    columns (`severity`, status codes) live; a value outside it costs only a range check. The domain
    is fixed rather than derived from the column's own min/max because the deriving pass measured as
    a net loss: a condition an earlier, more selective one short-circuits may never be probed at
    all, so a memo must cost nothing until its first use. Reordering is sound because conditions are
    an AND of pure predicates; `Match` stays an opaque callback.
  - **Equality fast path**: an exact-match condition against a `CodecBytesRaw` column no other
    condition targets skips the dict decode — the flat blob is decoded once and scanned with
    `internal/simd.EqualFixed16` into a per-row match bitmap, which also serves phase 2's gather.
    This relies on `Condition.Equal` being byte-identical to `Match` for that column; a future
    caller using `Equal` as an approximate prune hint would break it (the fast path never rechecks).
- **Top-N pushdown** (`Limit`+`Reverse`, `limitscan.go`) — an *unfiltered* limited request reads
  live parts in time order and stops once it holds `Limit` rows whose watermark is strictly past
  every unread part's bounds. Strict comparison keeps boundary ties, so the result stays a correct
  **superset** for the caller's own exact ordering. Disabled with conditions, whose per-part
  survivor count is unknown until the filter runs. The watermark heap lives for the whole scan and
  is fed only the rows each part appends — the accumulators are append-only during a scan, so the
  bounded top-`Limit` heap is exact incrementally and the scan stays linear in rows read.
- **Recycling** — `Recycle` pools the per-stream accumulator via `Batch.SetReleaseState`.
  Independently, part-decode int columns are **always** pooled: they are copied by value into
  accumulators, so they are dead once a part is distributed.

Conditions over a non-fixed column are per-record **attributes**, resolved by the zero-allocation
`signal.LookupAttribute` over the serialized `attrs` column.

## Part sidecars

- `bloom-{col}.bin` — per-column token blooms.
- `keys.bin` (`OTKY`, magic+version+CRC32C) — the part's distinct per-record **attribute keys**
  (not values — bounded by the schema, so tiny). `Engine.Keys` enumerates keys across head ∪
  in-window parts tagged with a `KeyScope` bitset (resource/scope/record), so an embedder can list
  and push down record-attribute labels that `Series`-based resolution cannot see. It is the
  enumeration twin of `Engine.Series`.
- `sym-{name}.bin` (`OTSP`) — the optional **side store**: a content-addressed auxiliary store a
  signal attaches per batch (`Batch.Side`) that rides the part lifecycle — absorbed into a live
  accumulator, written as sidecars on flush, **unioned** on merge (content addressing makes the
  union a plain dedup with no id remap), and **restored** into the accumulator when a flush fails.
  Profiles' symbol store is the first user; nil for logs/traces.

## Cost attribution

`Engine.StreamCost` (`streamcost.go`) attributes the live parts to streams — or to a label's values
— with rows, decoded bytes, an apportioned compressed share, and per-column distinct estimates.
Three decisions shape it:

- **It reads, it does not accumulate.** Every input exists at write time, so accumulating it there
  looks free — it is not. Measured on real log bodies, the per-row work (one value hash, the digit
  collapse, one collapsed hash) costs 350 ns/row against a 2288 ns/row record merge: **+15% on the
  merge**, for one column, where bloom construction already takes ~21%. A write-time figure would
  also have to be *persisted* to survive a restart or describe parts this process did not write,
  which means a new sidecar and a format addition. Reading instead makes the report a decode the
  operator pays for once, on data of any age, and leaves flush and merge byte-for-byte unchanged.
- **Only byte columns are decoded.** An int column's rows are a fixed width, so its raw share
  follows from the row ranges alone; the same is true of the implicit ts and stream columns. The
  `(stream, ts)` sort order is what makes the whole pass tractable: a stream is one contiguous run,
  so the row ranges and the columns' compression frames (`block.ColumnReader.Frames`) both tile
  `[0, rows)` and one merged walk covers them.
- **The plan locks per part, not once.** Resolving streams to groups is proportional to the store's
  total stream count, so one lock over the whole plan stalls writers for as long as that takes
  (102 ms at 337k streams, growing linearly); per part it is 1.7 ms. Sorting happens off lock, and an
  identity pruned mid-plan falls back to the stream id, as it already does after a retention prune.

`DiskBytes` is an estimate and says so: a frame's compressed size is split across the streams whose
rows it holds by their raw-byte share, because compression is per column per frame and the frame is
the floor of what is separable. Distinct counts reuse `bloom.Sketch` — the same estimator the bloom
builder sizes its filters with, not a second one — held one pair per group for one column at a time
and bounded by `MaxSketchGroups`, so the sketch state is a budget rather than groups × columns.

## WAL & cluster

The WAL frame is signal-agnostic (an opaque engine-encoded payload) plus an optional side frame;
`recordengine` owns the codec and `EncodeWAL` (the cluster write form, which appends the side frame
so the profile symbol store replicates). `ApplyPrimary`/`ApplyReplicated` mirror the metric engine's
primary-authoritative contract.

A stream's identity frame is logged when the head **registers** the stream, not when it first has an
accepted record: a stream is new exactly once, and replay drops records it cannot attribute to a
registered stream — so a first batch rejected in full (OOO window, in-flight bytes) would otherwise
strand every later record of that stream. The identity is written before the head commits the
registration (`head.admitStream` decides, `head.ensureStream` commits), so a failed write never
leaves a registered stream claiming a durability it does not have. An identity frame for a stream
that never gets rows is cheap and harmless.
