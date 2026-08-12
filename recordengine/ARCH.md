# `recordengine/` — the shared record engine (logs · traces · profiles)

Record-shaped signals are a **stream** (a Resource+Scope identity, indexed by postings exactly like
a metric series) of rows carrying a primary timestamp plus a fixed set of typed columns. Unlike a
metric's `(ts, float)` sample, a record's fields vary *within* the stream, so they are **columns
filtered by predicate**, not identity — the dual-shape contract: **Matchers resolve the stream,
Conditions filter its records** (see [`../query/ARCH.md`](../query/ARCH.md)).

All three signals share this engine; only the column schema, the projection, and (profiles) a side
store differ. It is the metrics engine's structural twin — head, flush, size-tiered merge with
retention, durable bucket-index + `streams.bin` stateless read path, `MaxPartBytes` splitting, and
the same lock discipline (see [`../engine/ARCH.md`](../engine/ARCH.md)). Notable divergences:

- Merge is **append-only**: retention only. Downsampling, recompression and precision are
  metrics-specific.
- Records are variable-width, so the byte cap converts to a row cap at an assumed average row size
  (`recordRowBytes`, calibrated to a real structured-log row) rather than the metric engine's exact
  per-row size. Without a flush split a part was sized only by flush cadence, and one oversized
  part distorted the size-tiered selection it feeds.

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

## Publish ordering

Publishing a flush writes `streams.bin` **first** and the bucket index **last**, then checkpoints the
WAL. The bucket index carries `FlushedEpoch` — the watermark replay starts from — so writing it is the
commit point, and only what is already durable may be committed. The stream set is superset-safe: an
identity persisted with no rows behind it resolves to a `SeriesID` no part range holds, so it costs a
lookup and contributes nothing. The reverse is unrecoverable — a committed part whose streams are
missing from `streams.bin` holds rows no matcher can name, while the advanced watermark makes replay
skip the WAL records that would have re-registered them.

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
