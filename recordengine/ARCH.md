# `recordengine/` — the shared record engine (logs · traces · profiles · exemplars)

A record-shaped signal is a **stream** — a `signal.Series` identity, indexed by postings exactly like a
metric series — of rows carrying a primary timestamp plus a fixed set of typed columns. A record's
fields vary *within* the stream, unlike a metric's `(ts, float)` sample, so they are **columns filtered
by predicate**, not identity. Hence the dual-shape contract: **Matchers resolve the stream, Conditions
filter its records** (see [`../query/ARCH.md`](../query/ARCH.md)).

All four signals share this engine; only the column schema, the projection and (profiles) a side
store differ. It is the metrics engine's structural twin — head, flush, size-tiered merge with
retention, durable bucket-index, part-scoped identity, stateless read path, `MaxPartBytes` splitting,
and the same lock discipline ([`../engine/ARCH.md`](../engine/ARCH.md)).

**All three of the identity's attribute sets are postings-indexed** — resource, scope, and the
signal-level `Series.Attributes`. Logs, traces and profiles leave the last empty (their per-record
attributes are a column, and profiles fold their type labels into the resource), so it costs them
nothing. Exemplars carry the metric's data-point attributes and reserved labels (`__name__`, …)
there, and a metric selector resolves exemplar streams only because those are matchable.

## Divergences from the metrics engine

| area | here |
|---|---|
| merge modes | append-only: retention only, no downsample/recompress/precision |
| sizes | measured, not modeled — records are variable-width |
| merge cap | decoded bytes, memory-derived; free space does not enter |
| selection | size tiers (`sizeTier`, `minTierParts`), not the metric engine's scored runs |
| forcing | no idle waiver; `Force` is the only escape from a fixed point |

**Retention drops whole parts first** (`dropExpired`), as in the metric engine: a part past the cutoff
is retired on the manifest alone, only a straddler rewritten. A record part's side-store and bloom
sidecars live under its own prefix, so `deletePart` reclaims them with it.

**Merge selection is confined to an aligned time bucket** (`timebucket.go`) — the same `mergeLadder`
(1h → 6h → 24h, nesting), walked narrowest-first, newest bucket skipped above the finest level, forced
rewrites confined to one bucket and winning the cycle. [`../engine/ARCH.md`](../engine/ARCH.md),
"Selection is confined to an aligned time bucket", has the mechanics of the **bucket** rules and what
each prevents. The selector inside one bucket differs: `pickTierGroup` takes the fullest size tier once
it holds `minTierParts`, where the metric engine scores runs — which is why there is no idle waiver
here, there being no scoring heuristic to waive.

It matters more here than for metrics: record queries are overwhelmingly narrow and recent, and a
record row carries far more bytes than a sample, so opening an unneeded part costs more.

**Straddlers are split on day boundaries**, as in the metric engine (`../engine/ARCH.md`, "A straddler
is split, never grouped"): `selectStraddlers` batches parts that fit no level oldest-first up to the
cap, after forced rewrites and the ladder, and `compactParts` writes any merge spanning more than one
day one day-wide window at a time. Records are where this bites: an exemplar producer re-exporting
stale exemplars with their original timestamps makes every flush a straddler, and the ladder alone
merged none of 12,735 such parts. The fix heals them but does not stop them being written; dropping
re-exported exemplars at ingest is its own change. Record specifics:

- The window is cut on the record timestamps read, unlike the metric engine's output cut: records are
  never aggregated, so a pass holds no state another needs. A forward `partCursor` decodes only the
  timestamp column for a stream with no row in the window, so a pass costs a decode of its own rows
  plus the timestamps.
- A side-store engine (profiles) writes the unioned symbol sidecar under each window's part, since
  each window's part is the one home a reader looks in.
- A split merge takes about cap / part size straddlers. Measured on the stand's shape (655 stale
  records plus one fresh per part, ≈44 KiB decoded, 64 MiB cap): 4,000 straddlers converged to 6
  parts in 6 cycles — 3 split merges of ≈1,500, 3 ladder merges — in 4 s in memory.

### Every size is measured

| what | measured as |
|---|---|
| `MaxPartBytes` | the *decoded* bytes a row holds (`flushColumns.rowBytes`) |
| flush split | on those bytes (`byteRanges`) |
| a part's footprint | `Manifest.RawBytes` → `part.sizeBytes`, recorded at write |
| size tiers, seal threshold | compare those |

A row count cannot stand in: the same count is ten 1 MiB records or ten thousand 1 KiB ones. Nor can an
assumed average row size, and the spread is what makes it unsafe: real structured-log rows average
**~950 B**, so an assumption low by 4× decodes 4× the intended bytes per merge. `recordRowBytes`
(1024 B, calibrated to that measurement) is only the fallback for a part whose manifest omits the figure.

Parts also record per-column sizing stats (`block.WithSizingStats`), so what reading a source column
holds is known from its manifest (`block.PartReader.ColumnInputSize`), and write manifest version 3
for it (`ADMIN.md`, the upgrade rule).

### Merge cap

```
cap = max( one flushed part,                              ← floor: retention must always
           min( mergeHeight × MaxPartBytes,                       be able to rewrite one
                MergeMemoryBytes / concurrency / 3 ) )
```

The metric engine has the same memory term, for the same reason: a cap sized against storage says
nothing about what the process can hold. It is *decoded* bytes here, because that is what
this merge holds — the output accumulates decoded before it is encoded. The divisor of three prices
sources + output buffer + the encode of it, and it overstates the first term: sources are read
through a read-ahead window per column (§ Merge read side), not held decoded. Free space does not
enter; the flush cap and the tiering target bound the disk.

The cap reaches the merge as `mergestream.Budget{ResidentBytes: cap}` — one type for both engines'
seal units, so this engine cannot grow a second, differently-named one when it gains a disk bound
(`internal/mergestream/ARCH.md`). The stream union the merge walks comes from the same package:
`mergestream.Keys` over each part's already-sorted `ranges`, a k-way heap rather than a map of every
distinct stream. It collapses repeats within a part as the map did, which matters because an
unsorted stream column leaves `buildRanges` with two runs carrying the same id.

`concurrency` is derived from the memory budget rather than from the core count, and enforced by the
process-wide `internal/memlimit.Pool` through `Config.MergeAdmission` — `engine/ARCH.md` ("Merge
cap") has the reasoning, including why a `MergeOptions.Background` merge defers rather than waits. Both engines
draw on the same pool because they share one process. There is no free-space term here, so unlike
the metric engine this cap has only the one divisor.

### Merge shape and forcing (`mergeshape.go`)

`Engine.MergeShape` reports the selector's inputs (fields in `ADMIN.md`), because a no-op merge is
otherwise indistinguishable from an idle engine. `MergeShape.Bytes` sums the parts' manifest sizes
(no backend stat calls), so the same snapshot separates parts that grew from a merge that stopped. This engine has **no idle waiver**: two parts in
different tiers of one bucket are a *permanent* fixed point, and `MergeOptions.Force` is the only way
out. It takes a bucket's unsealed parts smallest-first whatever their tiers, still truncated at the
cumulative-bytes cap and still confined to one bucket — the tier rule is waived, the memory bound is
not. `Candidates` and `ForceCandidates` run the real selector with and without `Force`, so the two
zero states separate: `ForceCandidates > 0` is a tier spread only `Force` breaks, both zero is every
unsealed part alone in its bucket, which nothing reduces without widening a part.

## Schema

```go
Schema of Column{Name, Kind(Int64|Bytes), Codec, Bloom(None|FullText|Attrs|Equality)}
```

The timestamp sort key and the int128 stream id are implicit. A signal projects its model into the
engine's column vectors; the engine treats the columns **opaquely**.

## Byte columns

Head buffers, fetch accumulators and the part read path use a contiguous **offsets+blob** layout
(`byteCol`: one `[]byte` blob + `[]int32` row end-offsets) rather than `[][]byte`. The GC then scans
two headers per column instead of one per row, and a scan walks one allocation with locality.

Cell views alias the blob under a **read-only-until-next-append** rule, since an append may move it; a
value retained past one is copied. A ts sort counts as one: it permutes into per-column scratch and
swaps, so the array a sort leaves behind is the one the next sort of that buffer writes into.
`fetch.NamedColumn` materializes views at the boundary, pooled across recycled fetches. Flush is a
**pass-through**: `block.Column` accepts blob+offsets directly, encoded byte-identically to the
per-row form, so writing a part never materializes a view per row.

**Every bulk accumulation is pre-sized.** A `byteCol` grown from nothing doubles its way to size,
re-copying its blob ~log₂(size) times and leaving each intermediate for the collector — and merge
accumulates a whole part's worth of bodies. So the flush buffer is sized from the head's tracked
per-stream byte counts, and the merge output buffer from `mergeShape`: the sources' manifests — row
counts, and per byte column the bytes its objects hold — scaled down to `capBytes` when the merge will
emit more than one part. That reads no column, so it is exact only for a column whose object is its
decoded size (a raw or near-unique column written uncompressed, the usual flat ones) and an
undercount the blob grows past otherwise. A column that falls back to the flat carry mid-merge
reserves the larger of that hint and its average cell size so far. The buffer is re-armed after each
part rather than reallocated — the part is read back from the backend, so nothing outlives the write
holding it.

### The merge carries byte columns as a dictionary and ids

A merge never expands a byte column it was handed dictionary-encoded. It **unions the sources'
dictionaries** (`mergeDict`: entries distinct by value) and the accumulator carries the column as
ids into that union (`splitCol`) instead of copying cells into a blob. `writePart` hands `BytesDict`
+ `BytesIDs` straight to `block.Column`, which encodes it byte-identically to the blob form. So a
merge copies no cell and re-hashes no row to rebuild a dictionary its sources already had: resolving
a source into the union costs one hash probe per distinct entry of a granule (per shared dictionary
once), where expanding cost one map probe and one blob copy per row, the engine's largest allocation
site.

**The union is built as the sources are read**, since a streamed source's entries arrive a granule at
a time: its order is first-seen, which is free to differ from any whole decode's because the writer
renumbers per granule and emits the same object for any entry order. Entries a source owns for the
whole merge — a shared dictionary, a column read whole — are kept as they are; a granule's own
entries alias a frame the next granule overwrites, so they are copied into an arena
(`TestMergeCopiesSelfGranuleEntries` is the shape that exposes an alias; the golden corpus does not).
The lookup index lives only while a source can still add entries: a column whose sources were all
read whole is resolved as they open and its index handed back before the next column takes one, which
is why byte columns open column by column and why the index is taken on first use — one grown index
serves every column in turn. Holding every column's index at once cost 21.7 MiB of a 62 MiB peak on
`BenchmarkMergeCompact`'s near-unique log corpus, and taking one per column up front +14% B/op.

**The decision starts from the codec and can only fall back.** A column takes the split carry when
the schema's codec accepts the split form: a `CodecBytesRaw` column such as `trace_id` has no
dictionary to hand the writer and stays flat. A split column falls back to the flat carry for the
rest of the merge (`mergeCarry.flatten` expands the ids both accumulators hold) when a source hands
it no dictionary — a whole column or granule past 65536 distinct, decoded flat — or when the union
passes 65536 entries per source, the most it holds while every source has a real dictionary. That
bounds the union at the resident size the flat carry would have, not above it. A mixed set is the
normal case: `body` and `attrs` carry ids while `trace_id` carries a blob, in the same accumulator.

**Size accounting stays expanded.** `byteSize`/`rowBytes` are what seal an output part
(`buf.byteSize() >= capBytes`) and what bound the merge's working set, and the merge cap is
denominated in *decoded* bytes. A split column therefore reports `Σ len(entries[ids[i]])`, never its
id array — reporting ids would inflate every output part by the column's compression ratio and remove
the memory bound the cap exists for. The total is maintained on append (O(1) per row) because
`byteSize` is called once per stream, so recomputing it would be O(rows × streams).

Consumers that walk the accumulator's values per row — the bloom build, the record-keys footer —
read through `cells`, a pointer-passed view over either form. Those walks branch on the form once and
then run a straight-line loop per form: the bloom build is ~20% of merge CPU and its per-row bodies
are small enough that a form test per row is measurable.

**Per-stream ts ordering is applied at copy time:** the flush computes each stream's ts permutation and
gathers rows into the flush buffer through it, never sorting the source. That is a correctness
requirement — the detached buffers stay fetchable through `e.flushing` while the part is written off
the lock, so a concurrent fetch is reading them (§ Flush failure). An already-ordered stream, the
common case, computes no permutation at all.

**A merge filters a stream's window row by row.** Both writers leave each stream's rows ts-ascending,
but nothing checks it when a part opens, and on a part that broke it a binary search would skip
in-window rows the merge then retires with the part. The forward cursor decodes each stream's `ts`
anyway, so it keeps each row by its own timestamp and loses nothing; a stream found out of order is
logged and counted as `corruption.detected{component="stream_order"}`, since the part's windowed
fetches are already wrong. A part decoded whole checks the order up front (`decodedPart.tsSorted`) and
binary-searches only where it holds.

### Merge read side (`mergesource.go`)

```
source part ─ partCursor: ts, ints, bytes ┐   one granule per column,
source part ─ partCursor: ts, ints, bytes ┼→  one window per column   → acc (one stream) → buf (one output part)
source part ─ wholeSource (fallback)      ┘   (block.PartReader.ColumnScan)
```

Each source is a `mergeSource` the stream sweep asks for one stream's rows at a time. A `partCursor`
opens every column through `block.PartReader.ColumnScan` with `defaultMergeReadWindow` (1 MiB)
read-ahead and decodes one granule at a time (`Decoder.DecodeInt64Into`, `Decoder.DecodeBytesBlock`),
so a source costs a window and a granule per column rather than its decoded columns. A constant column
costs no read. A column with no granules is read whole: the pre-framing layout, and — a *current*
layout — a dictionary column none of whose granules joined a shared dictionary, which the writer emits
as one unframed stream. Near-unique log columns (`trace_id`, `span_id`, high-cardinality bodies) are
often that shape, and the forward cursor cannot bound them; only the writer framing them can.

**Forward is checked, not assumed.** `buildRanges` sorts the ranges of a part whose stream column
arrived unsorted, so its sorted ids can point backwards through the rows. `forwardReadable` walks
`p.ranges` once at open (`mergestream.CheckForward`); a part that fails is decoded whole
(`readForMerge`, `wholeSource`) and counted as `stream_order` corruption. The cursor addresses granules
by row, so a range stepping back would only cost it re-fetched frames; but it takes one range per
stream, and an unsorted stream column can hold a stream in two runs, adjacent after the sort. So
`forwardReadable` also requires strictly ascending ids, and the whole source appends *every* run — a
lookup returning one of them loses the other's rows. A cursor serves a stream only when the sweep asks
for its id, so one the sweep skipped would strand every later stream of the part; `checkDrained` fails
the merge with `block.ErrCorrupt` before its last part is written rather than commit a part missing
them.

**What it holds, measured.** The output is still buffered, so the read side lowers the peak only where
the sources were a term of it. `TestMergeResidentFlatInPartSize` (trace-shaped, file backend, output
sealed at 4 MiB): growing the sources 8× (28.5 → 227.8 MiB decoded) moves the peak live heap from 20.4
to 37.4 MiB, the growth being the windows filling to 1 MiB; decoding the sources whole moves it from
21.4 to 130.1 MiB. A merge that writes one part holds that part decoded plus its encode, however it
reads: 257 MiB against 292 MiB for the same 228 MiB of sources, sampled per stream as well. CPU moves
by −6% on those parts (`BenchmarkMergeCompactTraces`) and within ±2% on `BenchmarkMergeCompact`'s log
corpus, whose large columns are unframed and read whole either way. The merge unit is still **one whole
stream** — `acc` gathers a stream across every source before it is sorted and sealed at a stream
boundary — so a few long streams hold O(stream × sources), and the budget divisor above has not been
re-derived for any of it.

## Flush failure

A flush detaches the head, then writes the part off the lock, so **every step after the detach must be
undoable**. Any error before the publish folds the detached buffers back into the head (merging with
whatever arrived meanwhile), restores the side-store snapshot via `SideStore.Restore`, and clears
`e.flushing`. Nothing else holds those records: the part was never published, and the WAL checkpoint
only runs on a successful publish, so without the fold-back the rows would be lost the moment the next
flush overwrote the in-flight buffer.

### Out of space is not a retryable flush failure

The fold-back above is what makes a *transient* failure survivable; it does nothing for a disk that
is full, where the next flush fails identically and the head grows without bound. The shared
`internal/diskguard` (design and rationale in [`../engine/ARCH.md`](../engine/ARCH.md)) checks free
bytes and free inodes before the detach and latches either verdict, so `AppendBatch`/`ApplyPrimary`
reject with `backend.ErrNoSpace` instead of buffering records that cannot be stored. The abort path
also latches an ENOSPC the write itself returned.

The inode axis is sized from the **schema**: a record part is one object per column plus its blooms,
footer, identities and sidecars, and a record schema is an order of magnitude wider than a metric
one — width is exactly what the inode axis spends.

## Stream identity is part-scoped

Each part carries the identities of the streams it holds (`{part}/identity`, format in
`index/identity`), written and deleted with its other objects — the same shape and the same three
properties as the metrics engine. Retention self-cleans; a flush persists only the identities it wrote
(88 B for one new stream, ~40 B/stream, against a whole-set `streams.bin` re-encoded and rewritten on
**every** flush and merge whether or not the set changed); every node derives its live set from its own
parts. An older build's `streams.bin` is read at open and deleted once every live part carries its own
identities.

**The WAL must carry its own identities.** A flush checkpoints it, so a stream record logged when the
identity was first seen is gone afterwards; a record is logged again whenever a stream starts a fresh
**record buffer** (`head.needsStreamRecord`), once per actively-appending stream per flush window.
Without it a record logged after a checkpoint would reference a stream the log no longer describes, and
with identity in the parts (which retention can drop) nothing else would name it.

## Identity prune

`PruneIdentities` drops the identities retention leaves behind, which otherwise accumulate for the
process' lifetime under stream churn: `instance.id`-shaped attributes turn ~24 stable streams into ~15k.

The **live set** is every live part's stream ids, resident already in `part.ranges` (so unlike the
metrics engine it costs no I/O), plus the head and the mid-flush detachment. Symbol ids are dense and
referenced by the postings, so survivors are **rebuilt** into fresh structures off the engine lock
against `series.Index.Snapshot()` — the append-only entry log, which registration only extends — then
swapped in under the lock. The swap re-registers what changed meanwhile (entries past the snapshot's
end, and streams the prune found dead that regained buffered records) and drops the rest's
out-of-order watermarks by key.

It runs after a merge, on a replica after the refresh adopting a new part set, and only when parts
actually went away. No ownership rule: identity is scoped to the part, so a node's live set means
exactly what this node can still serve. `Admin.PruneIdentities` forces a sweep past the size and
dead-fraction thresholds.

## Publish ordering

**Flush** seals the WAL as it detaches the head, writes the part's objects — identity included —
first, the bucket index last, then checkpoints the WAL through the sealed sequence, keeping the
segments that hold records appended during the part write (`wal/ARCH.md`, "Epochs"). The bucket index carries the flush watermark replay starts from — one slot per
writer, since the index is shared and the watermark is not (`wal/ARCH.md`, "Epochs") — so
writing it is the commit point and only what is already durable may be committed — each part is
written deferred and synced once with `backend.SyncPrefix` after its last sidecar (the `sym-*.bin`
side data lands after `openPart`, so the sync follows it). A committed part
whose identities were missing would be **unrecoverable**: it holds rows no matcher can name, while the
advanced watermark makes replay skip the WAL records that would have re-registered them. The reverse
leftover is harmless — an uncommitted part is an orphan a later open sweeps, its identities never
loaded.

**Merge** is identical to the metric engine: sources are retired only after the bucket index naming
their replacement is committed. See [`../engine/ARCH.md`](../engine/ARCH.md), "Publish ordering".

**A rebased commit opens what it adopts**, so the index this engine publishes and the parts it
serves stay the same set; the adopted parts are readable but not owned. See
[`../engine/ARCH.md`](../engine/ARCH.md), "Adopted parts".

**Block identity** is allocated the same way: a flush output takes a fresh `{n}` at level 0, a merge
that writes one part the union of the block sets its inputs covered at one level above them, and a
merge that splits a fresh contiguous run plus the joint `bucketindex.Claim` over it. Numbering runs
above the index's persisted high-water mark, and every assignment is made per CAS attempt and
written onto the part only once the commit lands. See [`../engine/ARCH.md`](../engine/ARCH.md),
"Block identity is allocated by the commit that publishes the part".

**Repair** is identical to the metric engine, down to `Config.Repair` and its `bucketindex` types, the satisfaction rule (the
exact part, the largest containing part at a higher level, or a split group whose members are all
present), the second fetch round that completes such a group inside one commit, and the two gates a
want must clear before its loss is acknowledged as a revocable hole. See [`../engine/ARCH.md`](../engine/ARCH.md),
"Repair — a want is discharged by committing a part", "An unrepairable want becomes a revocable
hole" and "A store without a cluster layer is its own complete owner set".

## Part identity & orphans

Part prefixes are `<prefix>/{partid}`, a minted globally unique id, and `LoadParts` sweeps orphans at
open — exactly as in the metric engine ([`../engine/ARCH.md`](../engine/ARCH.md), "Lifecycle and part
identity"), including the `Config.OrphanGrace` age guard that spares another writer's in-flight part
(a record merge is bounded by `MaxPartBytes` and `MergeMemoryBytes` rather than a ceiling, well
inside the one-hour default), and the replica exception: `RefreshReplica` sweeps nothing, because the owner's
in-flight part is not in the index yet, and a part the store lacks becomes a *pending* want rather than
an error — counted and disclaimed over (`Stats.WantedParts`, `Engine.WantOverlaps`) until a refresh
finds it or this node commits as an owner. `LoadPartsReadOnly` reaches that same sweep-nothing load
directly, for a handle that must not mutate the prefix at all (`storage.WithReadOnly`).

Reuse would be unsound here for one extra reason: two of a part's objects are conditional — `keys.bin`
is skipped when the rows carry no record attributes, the `sym-*.bin` sidecars when there is no side
data — so a new part would silently adopt a failed attempt's.

A part the owner cannot open is handled identically: only `backend.ErrNotExist` drops it from
`Entries` and records a `bucketindex.Want` in the same compare-and-swap, every other error still
fails the load, and the sweep spares a wanted part's remaining objects
([`../engine/ARCH.md`](../engine/ARCH.md), "A part the owner cannot read becomes a want, not a
removal"). `AdoptWants` is shared with it too: obligations partsync discovers for parts a peer
holds that this index never named, which survive a load because nothing in the index implies them.

A load is all-or-nothing and a failed one fences every commit until a load succeeds, retried by
`ReloadFenced` each maintenance cycle; a reload keeps a flushed part whose commit failed; a part
corrupt over three consecutive loads becomes a want. All of it is shared with the metric engine
([`../engine/ARCH.md`](../engine/ARCH.md), "A failed load changes nothing, and fences every commit"
and "Corruption that persists is a want"). A corrupt record-keys footer (`ErrCorruptKeys`) counts as
corruption here too.

## Lifecycle guards

Flush and merge run under one `flushMu`, and `Reset` takes it too, with the same rationale and the same
retire-don't-delete behavior as the metric engine ([`../engine/ARCH.md`](../engine/ARCH.md)). Here they
also reuse the flush column buffer off the engine lock — a third reason the single-mutator invariant is
enforced rather than assumed.

### Memory accounting

| bound | meters |
|---|---|
| `headByteCap` (2 GiB, hard) | the live plus detached buffers: a bound on the *next* part's blobs |
| `MaxInFlightBytes` | everything resident |
| `byteColCap` (2 GiB, hard) | one fetch accumulator's blob per byte column |

A flush concatenates every stream's cells into one blob per byte column, indexed by `byteCol`'s int32
offsets, so overflowing `headByteCap` would write **negative offsets** into a part; past the cap
records are rejected as backpressure. It meters both sides of `head.inFlightBytes`, not just the live
half: a failed flush folds the detached buffers back in (`head.reattach`), so bounding only the live
buffers would let a reattach restore ~2x the cap and the next flush write those negative offsets.

**Invariant: every accumulation into a `byteCol` is bounded by its caller.** A fetch accumulator is
the second one. It merges the head, the in-flight flush buffer and every part the stream appears in,
so neither `headByteCap` (one buffer) nor `MaxPartBytes` (one part) bounds it, and a wide window over
a hot stream can exceed 2 GiB. The append paths do not check, so all three of its
producers reject first: `appendColsWindow` and `appendWindowRows` on the bulk/window paths, and
`appendLazyRow` per row on the two-phase filtered scan (`gatherHits`), which appends out of a lazily
decoded part and is otherwise bounded by nothing but the read budget, which may be off. An overflow is
silent at the append and surfaces as a slice-bounds panic in `byteCol.at` once the accumulator is
ts-sorted.

**"Resident" includes the detached buffers.** `head.detach` moves them aside, but they — and the flush
columns built from them — live until the part is published, so their size is parked in
`head.detachedBytes` and `head.inFlightBytes` (= `HeadBytes` = `Stats.HeadBytes`) keeps counting it,
cleared at the publish or handed back by `head.reattach` on a failed flush, never both. Otherwise the
measure would read zero for the whole duration of a slow flush and `MaxInFlightBytes` would admit a
second full head on top of the one still being written out.

**How long it has been waiting** is `head.since`, stamped when the head takes its first bytes after a
flush and cleared by `head.detach` (`Stats.HeadAge`). Wall time, not record timestamps: backfill puts
arbitrarily old records in a brand-new head, so data time cannot say how long a flush has stalled.

**Stream identity sits outside that measure:** `Stats.IdentityBytes` reports the symbol table, stream
index, postings and per-stream watermarks on their own, since a flush drains records but not identities
— they outlive the data they named, and only `Reset` reclaims them. `OOOWindow` is likewise
**per-stream** (`head.streamNewest`), so a fast or clock-skewed stream cannot shed the slower ones
sharing the head and a stream's first record is never late; the watermarks outlive a flush and are
cleared only with the head.

Every append is a *run* over one stream — a `Batch` is one stream, and so are WAL replay and the
replica apply — so `head.appenderFor` resolves the record buffer and the watermark once per run
(`streamAppender`) instead of per record, which costs two map reads and, on the common monotonic
path, a map write for every record. The run carries its own watermark, so a record is still measured
against the newest record ahead of it in the same run; `commit` publishes it. A run that admits
nothing leaves the head untouched, which is what keeps `needsStreamRecord` true for a stream whose
whole batch was rejected.

**A logged write decides, logs, then applies.** With a WAL, `AppendBatch` and `ApplyPrimary` run every
record through `streamAppender.admit` — the out-of-order and in-flight-byte checks, charged against
the bytes already admitted in the same write rather than the head's growth — then write the log, and
only then `apply` the admitted records and `commit` the watermark. Applying first would leave a failed
log write's records in the head un-logged while the caller, seeing the error, retried; records are
append-only, so the retry would store them twice. Deciding first admits exactly what applying first
did, because under the exclusive lock the head grows only by this write's own records. Replay and the
replica apply keep the one-pass `append`: their records are already durable.

The side delta goes to the log **before** the records, and `ApplyPrimary` puts every side frame ahead
of the records in its payload. A delta is content-addressed and absorbing it again is a dedup, so a
delta that reached the log without its records costs nothing on retry; records that reached it ahead
of a failed delta would be replayed and then logged again.

## Fetch

Heavily tuned around decoding as little as possible.

### Stream resolution (`part.ranges`)

A part's stream index is a **slice of `{rowRange, id}` sorted by id**, not a map: rows are
`(stream, ts)`-ordered so each part's stream set is already ascending and each stream one contiguous
run, and `openPart` only sorts when a column arrives out of order. Every resolution — part selection,
accumulator pre-sizing, granule selection, the per-part row append — then merge-joins the query's
once-sorted ids against it, `O(ids + streams)` sequential comparisons per part with no hashing and no
allocation, plus an `O(1)` min/max reject for a part whose id space misses the request entirely.
Point lookups (`part.lookup`, used by the merge and by `streamInRangeLocked`) binary-search it.

Planning is what a fragmented store's fetch spends its time on: a query over 664 parts × 16.7k streams
resolves in **22.6 ms against 1.12 s** of `map[SeriesID]rowRange` probes, and **11 µs against 170 ms**
when the parts' id spaces are disjoint from the request. Hashing a 16-byte key per (part, stream) pair
was 41% of `Engine.Fetch` on the real log corpus — as much as decoding the columns the query reads —
and it runs under the read lock, so it also widened the window flush and merge publishes contend with.

### Granule time pruning (`granule.go`)

Every column of a part is **block-framed**, so a reader decodes one granule at a time, and the marks
sidecar carries each granule's `[minTime, maxTime]`. A windowed fetch decodes only the granules its
rows occupy. Without it part span is the *only* time filter records have, and a 15-minute query against
a day-wide part decodes the day: **286× the rows needed** on a real log corpus.

Selection comes from the **requested streams'** row ranges, not the whole part. Rows are
`(stream, ts)`-ordered, so granule bounds are not monotonic across a part, but each stream owns one
contiguous ts-ascending run — which is what makes a service-filtered query touch a handful of granules.
Selection merge-joins the plan's sorted ids with the part's stream index, so a query with no matchers —
requesting every stream in the tenant to skip a handful of granules — costs one walk of the two sorted
lists per part rather than a lookup per requested stream. `nil` means
"decode everything", returned both when marks are unusable and when nothing pruned, keeping the
whole-column path on its simpler route. Decoded rows land at their **part row offsets**, pruned or not,
so the row-range index and `tsWindow` work unchanged; rows outside selected granules are *unspecified*.

**Invariant: the timestamp column is never pruned** — it is decoded whole however narrow the window.
Row selection reads timestamps directly (binary search over each stream's ts-ascending run, see
`tsWindow`), so unspecified timestamps break the search's precondition and let it return rows the
window never covered, whose value columns are equally unspecified. On the real log corpus, pruning the
timestamp column makes a 15-minute level filter report **889,390 rows where the true answer is
483,076**. Decoding it whole is what everything else rests on: selection is driven by real timestamps,
so every row selection can reach lies in a granule overlapping the window — which is a granule pruning
always keeps. The column is delta-of-delta int64 and tiny next to the bodies and attributes the pruning
exists to skip.

The stream id column stays unframed: a fetch resolves streams through the row-range index and never
decodes it.

### The rest of the read path

| optimization | what it does |
|---|---|
| lazy column decode | materialize only columns the conditions + projection reference |
| one decode per part | rows distributed to per-stream accumulators, pre-sized from row-range counts |
| bloom pruning | skip a part whose per-column bloom proves a required token or value absent |
| top-N pushdown (`limitscan.go`) | stop an *unfiltered* limited request once the watermark clears unread parts |
| recycling | `Recycle` pools the per-stream accumulator via `Batch.SetReleaseState` |

A body search projecting body touches just `ts`+`body`. Distribution bulk-appends in-window ranges,
filters in place, and skips the sort when already ts-ordered. Bloom pruning re-checks per row after the
skip ([`../index/ARCH.md`](../index/ARCH.md)).

Top-N (`Limit`, at whichever end `Reverse` selects — newest-first when set, oldest-first otherwise)
stops once it holds `Limit` rows whose watermark is strictly past every
unread part's bounds; strict comparison keeps boundary ties, so the result is a correct **superset**
for the caller's own exact ordering. It is disabled with conditions, whose per-part survivor count is
unknown until the filter runs. The watermark heap is fed only the rows each part appends, and
accumulators are append-only during a scan, so the bounded heap is exact incrementally and the scan
stays linear in rows read.

Beyond `Recycle`, part-decode int columns are **always** pooled: copied by value into accumulators,
they are dead once a part is distributed. Conditions over a non-fixed column are per-record
**attributes**, resolved by the zero-allocation `signal.LookupAttribute` over the `attrs` column.

### Two-phase filtered fetch (`fetchlazy.go`)

Taken for `AllConditions` + conditions, the by-id lookups:

1. Decode only ts and the *condition* columns (byte columns as lazy `chunk.DictColumn`, O(1) `At`) and
   record the matching rows. A part with no match — a bloom false positive — never decodes its
   projected columns.
2. Decode the rest and gather the recorded rows.

**Each row is matched once:** phase 2 replays phase 1's hit list, and the post-scan `filterPrefix`
re-applies the conditions only to the head/flushing-seeded accumulator prefix, part rows passing by
construction.

**Compiled conditions** (`fetcheval.go`) resolve each condition to its column once per part, then
evaluate cheap-first: equality bitmap → dictionary memo → int-value memo → per-row call.

A dictionary column memoizes per *distinct entry* (≤ 65536, filled lazily so a high-selectivity scan
pays only per touched entry), keeping a regex — or an attribute lookup that re-parses the `attrs` blob
— off the per-row path. An int column memoizes per *distinct value* over a small fixed non-negative
domain, where enum-shaped columns live (`severity`, status codes); a value outside it costs only a
range check. The domain is fixed, not derived from the column's own min/max: a deriving pass costs more
than it saves, since a condition short-circuited by an earlier, more selective one may never be probed
at all, so a memo must cost nothing until its first use. Reordering is sound because conditions are an
AND of pure predicates; `Match` stays an opaque callback.

**Equality fast path.** An exact-match condition against a `CodecBytesRaw` column no other condition
targets skips the dict decode: the flat blob is decoded once and scanned with
`internal/simd.EqualFixed16` into a per-row match bitmap, which also serves phase 2's gather. A set
membership (`Condition.AnyEqual`) takes the same blob without the kernel, `evalRawSet` testing each
row's cell out of it under no width constraint. The path needs an **unframed** column: a flush frames
every byte column, and `ColumnReader.BytesRaw` refuses a framed one, so on parts this writer produces
the column falls back to the dictionary path. This
relies on `Condition.Equal` being byte-identical to `Match` for that column — a future caller using
`Equal` as an approximate prune hint would break it, since the fast path never rechecks.

## Part sidecars

| object | contents |
|---|---|
| `bloom-{col}.bin` | per-column token blooms |
| `keys.bin` (`OTKY`, magic+version+CRC32C) | the part's distinct per-record **attribute keys** |
| `sym-{name}.bin` (`OTSP`) | the optional **side store**, in the signal's format (`../signal/ARCH.md`) |

The blooms are **advisory**: they only ever remove parts the per-row re-check would have removed
anyway, so a sidecar that is absent *or fails to decode* degrades to "this column does not prune
this part" — the part is scanned and the exact predicate decides. Opening the part logs the
corruption and continues; failing recovery instead would take a whole prefix offline over a
structure that changes no result, and a merge rewrites the sidecar. This matches the metric
engine's derived sidecars (`../engine/ARCH.md`), which fall back on absent or corrupt alike.

`keys.bin` holds keys, not values: the schema does not bound values, while keys are tiny.
`Engine.Keys` enumerates them across head ∪ in-window parts tagged with a `KeyScope` bitset
(resource/scope/series/record), so an embedder can list and push down record-attribute labels that
`Series`-based resolution cannot see. It is the enumeration twin of `Engine.Series`.

**Values** need no sidecar: `Engine.ColumnValues` (`values.go`) unions each in-window part's
**column dictionary** — already the part's distinct value set — with a scan of the unflushed head,
so tag/label autocomplete costs O(distinct values) instead of decoding every record. The flat
fallback (`IDWidth == 0`, one entry per row) reads the same way because the accumulator dedups. With
`ValuesRequest.AttrKey` set the cells are serialized attribute blobs and only that key's value is
kept: each *distinct blob* is decoded once, which is still far below per-row. Values project through
`signal.Value.AppendText`, the form the matching layer compares against.

Three contract points the callers depend on: the result is a **superset** for the window (an
overlapping part contributes its whole dictionary, matching the fetch contract); `Limit` truncates
the sorted union to its lexicographically smallest values and does not signal that it did; and
numeric columns are not enumerable — a signal's numeric enums are a static set the caller knows.
Backend reads happen off the engine lock: the head is drained and the in-window parts acquired under
it, then released after their dictionaries are read.

The side store is a content-addressed auxiliary store a signal attaches per batch (`Batch.Side`),
riding the part lifecycle: absorbed into a live accumulator, written as sidecars on flush, **unioned**
on merge (content addressing makes the union a plain dedup with no id remap), and **restored** into the
accumulator when a flush fails. Profiles' symbol store is the first user; nil for logs/traces.

`Encode` and `Union` return the in-memory form, which `SideSnapshot` hands to a resolver per
query; `SideStore.Stored` converts to the on-disk form, and the engine applies it only to what
`writeSidecars` writes: the flush snapshot once per flush (shared by every part a split produces)
and the merged union. A store that compresses its sidecars thus pays that encode at flush and
merge, never on the query path.

**Symbols follow their records' visibility.** A record is in exactly one of head / `e.flushing` / a
published part, and `Engine.SideSnapshot` must union the side data of all three the same way a fetch
reads all three. The flush's `Encode`+`Reset` at detach hands the accumulator's snapshot to
`e.flushingSide`, cleared under the same lock that publishes the part (whose sidecars now carry it) or
that restores it into the accumulator on abort. Without that hop the snapshot lives only in a
flush-local variable, and for the length of an object-store flush the detached records resolve their
symbols against an empty set — wrong answers, not an error.

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

The WAL frame is signal-agnostic — an opaque engine-encoded payload plus an optional side frame.
`recordengine` owns the codec and `EncodeWAL`, the cluster write form, which appends the side frame so
the profile symbol store replicates. The accepted payload `ApplyPrimary` returns leads with the side
frames instead (see "A logged write decides, logs, then applies"); replicas apply either order. `ApplyPrimary`/`ApplyReplicated` mirror the metric engine's
primary-authoritative contract, and so does `RefreshReplica`'s **per-stream** trim watermark
([`../engine/ARCH.md`](../engine/ARCH.md), "Cluster surface"): a stream absent from every part keeps
its whole head, one present keeps every record past *its own* newest flushed timestamp — the bucket
index records time bounds per part only, and a part-wide figure is another stream's flush, which says
nothing about this one's durability.

`writePart` writes those watermarks beside the part as the `{prefix}/smax` sidecar
(`internal/watermark`, framing in [`../engine/ARCH.md`](../engine/ARCH.md)), so a refresh resolves them
without decoding the timestamp column. A part without the sidecar — or with one that does not pair
with its stream ranges — falls back to that decode. The result is held on the part handle, and
`loadPartsLocked` reuses handles by prefix across an index reload exactly as the metric engine does,
so the blooms, record keys, stream ranges and identities a part carries are read once rather than per
maintenance tick; a reused handle is probed with `block.PartPresent` so a part whose objects went away
still becomes a want. As there, a published part is never written. A handle is reused only while its
entry is unchanged, and a changed entry gets a fresh handle. `MergeShape` (through the ladder
selector's `fitsLevel`), `PartsDetailed` and the top-N scan (`orderPartsForLimit`, `beyondWatermark`)
read part fields after releasing the engine lock. The commit's stamping of block identity relies on
`flushMu` for the same reason given in the metric engine's ARCH.md.

**A stream's identity frame is logged when the head registers the stream**, not when it first has an
accepted record. A stream is new exactly once, and replay drops records it cannot attribute to a
registered stream, so a first batch rejected in full (OOO window, in-flight bytes) would otherwise
strand every later record of that stream. The identity is written *before* the head commits the
registration (`head.admitStream` decides, `head.ensureStream` commits), so a failed write never leaves a
registered stream claiming a durability it does not have. An identity frame for a stream that never
gets rows is cheap and harmless.
