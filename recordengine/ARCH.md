# `recordengine/` — the shared record engine (logs · traces · profiles)

A record-shaped signal is a **stream** — a Resource+Scope identity, indexed by postings exactly like a
metric series — of rows carrying a primary timestamp plus a fixed set of typed columns. A record's
fields vary *within* the stream, unlike a metric's `(ts, float)` sample, so they are **columns filtered
by predicate**, not identity. Hence the dual-shape contract: **Matchers resolve the stream, Conditions
filter its records** (see [`../query/ARCH.md`](../query/ARCH.md)).

All three signals share this engine; only the column schema, the projection and (profiles) a side
store differ. It is the metrics engine's structural twin — head, flush, size-tiered merge with
retention, durable bucket-index, part-scoped identity, stateless read path, `MaxPartBytes` splitting,
and the same lock discipline ([`../engine/ARCH.md`](../engine/ARCH.md)).

## Divergences from the metrics engine

| area | here |
|---|---|
| merge modes | append-only: retention only, no downsample/recompress/precision |
| sizes | measured, not modeled — records are variable-width |
| merge cap | decoded bytes, memory-derived; free space does not enter |
| forcing | no idle waiver; `Force` is the only escape from a fixed point |

**Retention drops whole parts first** (`dropExpired`), as in the metric engine: a part past the cutoff
is retired on the manifest alone, only a straddler rewritten. A record part's side-store and bloom
sidecars live under its own prefix, so `deletePart` reclaims them with it.

**Merge selection is confined to an aligned time bucket** (`timebucket.go`) — the same `mergeLadder`
(1h → 6h → 24h, nesting), walked narrowest-first, newest bucket skipped above the finest level, forced
rewrites confined to one bucket and winning the cycle. [`../engine/ARCH.md`](../engine/ARCH.md),
"Selection is confined to an aligned time bucket", has the mechanics and what each rule prevents; here
`pickTierGroup` is the selector that runs unchanged inside one group.

It matters more here than for metrics: record queries are overwhelmingly narrow and recent, and a
record row carries far more bytes than a sample, so opening an unneeded part costs more.

### Every size is measured

| what | measured as |
|---|---|
| `MaxPartBytes` | the *decoded* bytes a row holds (`flushColumns.rowBytes`) |
| flush split | on those bytes (`byteRanges`) |
| a part's footprint | `Manifest.RawBytes` → `part.sizeBytes`, recorded at write |
| size tiers, seal threshold | compare those |

A row count cannot stand in: the same count is ten 1 MiB records or ten thousand 1 KiB ones. Nor can an
assumed average row size — a 256 B assumption against ~950 B real rows decodes ~4× the intended bytes
per merge. `recordRowBytes` is the fallback for a part whose manifest does not carry the figure.

### Merge cap

```
cap = max( one flushed part,                              ← floor: retention must always
           min( mergeHeight × MaxPartBytes,                       be able to rewrite one
                MergeMemoryBytes / MergeConcurrency / 3 ) )
```

The metric engine has the same memory term, for the same reason: a cap sized against storage says
nothing about what the process can hold. It is *decoded* bytes here, because that is what
this merge holds — sources are decoded up front and the output accumulates decoded before it is
encoded, hence dividing by three (sources + output buffer + the encode of it). Free space does not
enter; the flush cap and the tiering target bound the disk.

### Merge shape and forcing (`mergeshape.go`)

`Engine.MergeShape` reports the selector's inputs (fields in `ADMIN.md`), because a no-op merge is
otherwise indistinguishable from an idle engine. This engine has **no idle waiver**: two parts in
different tiers of one bucket are a *permanent* fixed point, and `MergeOptions.Force` is the only way
out. It takes a bucket's unsealed parts smallest-first whatever their tiers, still truncated at the
cumulative-bytes cap and still confined to one bucket — the tier rule is waived, the memory bound is
not.

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
per-stream byte counts, and the merge output buffer from `decodedShape`: the sources' expanded blob
per column (a walk over a dictionary's packed ids, touching no cell), scaled down to `capBytes` when
the merge will emit more than one part. That buffer is then re-armed after each part rather than
reallocated — the part is read back from the backend, so nothing outlives the write holding it.

**Per-stream ts ordering is applied at copy time:** the flush computes each stream's ts permutation and
gathers rows into the flush buffer through it, never sorting the source. That is a correctness
requirement — the detached buffers stay fetchable through `e.flushing` while the part is written off
the lock, so a concurrent fetch is reading them (§ Flush failure). An already-ordered stream, the
common case, computes no permutation at all.

## Flush failure

A flush detaches the head, then writes the part off the lock, so **every step after the detach must be
undoable**. Any error before the publish folds the detached buffers back into the head (merging with
whatever arrived meanwhile), restores the side-store snapshot via `SideStore.Restore`, and clears
`e.flushing`. Nothing else holds those records: the part was never published, and the WAL checkpoint
only runs on a successful publish, so without the fold-back the rows would be lost the moment the next
flush overwrote the in-flight buffer.

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

**Flush** writes the part's objects — identity included — first, the bucket index last, then
checkpoints the WAL. The bucket index carries `FlushedEpoch`, the watermark replay starts from, so
writing it is the commit point and only what is already durable may be committed. A committed part
whose identities were missing would be **unrecoverable**: it holds rows no matcher can name, while the
advanced watermark makes replay skip the WAL records that would have re-registered them. The reverse
leftover is harmless — an uncommitted part is an orphan the next open sweeps, its identities never
loaded.

**Merge** is identical to the metric engine: sources are retired only after the bucket index naming
their replacement is committed. See [`../engine/ARCH.md`](../engine/ARCH.md), "Publish ordering".

## Part sequences & orphans

Part prefixes (`<prefix>/%010d`) are **append-only**, and `LoadParts` sweeps orphans at open, exactly
as in the metric engine ([`../engine/ARCH.md`](../engine/ARCH.md), "Lifecycle and part sequences") —
including the replica exception, `RefreshReplica` skipping the sweep because the owner's in-flight part
is not in the index yet.

Reuse would be unsound here for one extra reason: two of a part's objects are conditional — `keys.bin`
is skipped when the rows carry no record attributes, the `sym-*.bin` sidecars when there is no side
data — so a new part would silently adopt a failed attempt's.

## Lifecycle guards

Flush and merge run under one `flushMu`, and `Reset` takes it too, with the same rationale and the same
retire-don't-delete behavior as the metric engine ([`../engine/ARCH.md`](../engine/ARCH.md)). Here they
also reuse the flush column buffer off the engine lock — a third reason the single-mutator invariant is
enforced rather than assumed.

### Memory accounting

| bound | meters |
|---|---|
| `headByteCap` (2 GiB, hard) | the live buffers: a bound on the *next* part's blobs |
| `MaxInFlightBytes` | everything resident |
| `byteColCap` (2 GiB, hard) | one fetch accumulator's blob per byte column |

A flush concatenates every stream's cells into one blob per byte column, indexed by `byteCol`'s int32
offsets, so overflowing `headByteCap` would write **negative offsets** into a part; past the cap
records are rejected as backpressure.

**Invariant: every accumulation into a `byteCol` is bounded by its caller.** A fetch accumulator is
the second one. It merges the head, the in-flight flush buffer and every part the stream appears in,
so neither `headByteCap` (one buffer) nor `MaxPartBytes` (one part) bounds it, and a wide window over
a hot stream can exceed 2 GiB. The append paths do not check, so `appendColsWindow` and
`appendWindowRows` reject first: an overflow is otherwise silent at the append and surfaces as a
slice-bounds panic in `byteCol.at` once the accumulator is ts-sorted.

**"Resident" includes the detached buffers.** `head.detach` moves them aside, but they — and the flush
columns built from them — live until the part is published, so their size is parked in
`head.detachedBytes` and `head.inFlightBytes` (= `HeadBytes` = `Stats.HeadBytes`) keeps counting it,
cleared at the publish or handed back by `head.reattach` on a failed flush, never both. Otherwise the
measure would read zero for the whole duration of a slow flush and `MaxInFlightBytes` would admit a
second full head on top of the one still being written out.

**Stream identity sits outside that measure:** `Stats.IdentityBytes` reports the symbol table, stream
index, postings and per-stream watermarks on their own, since a flush drains records but not identities
— they outlive the data they named, and only `Reset` reclaims them. `OOOWindow` is likewise
**per-stream** (`head.streamNewest`), so a fast or clock-skewed stream cannot shed the slower ones
sharing the head and a stream's first record is never late; the watermarks outlive a flush and are
cleared only with the head.

## Fetch

Heavily tuned around decoding as little as possible.

### Granule time pruning (`granule.go`)

Every column of a part is **block-framed**, so a reader decodes one granule at a time, and the marks
sidecar carries each granule's `[minTime, maxTime]`. A windowed fetch decodes only the granules its
rows occupy. Without it part span is the *only* time filter records have, and a 15-minute query against
a day-wide part decodes the day: **286× the rows needed** on a real log corpus.

Selection comes from the **requested streams'** row ranges, not the whole part. Rows are
`(stream, ts)`-ordered, so granule bounds are not monotonic across a part, but each stream owns one
contiguous ts-ascending run — which is what makes a service-filtered query touch a handful of granules.
It walks whichever is smaller, the requested ids or the part's own streams via the plan's id set (built
once per query): both yield the same granules, but a query with no matchers requests every stream in
the tenant, so walking the request would cost hundreds of thousands of lookups per part. `nil` means
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

Top-N (`Limit`+`Reverse`) stops once it holds `Limit` rows whose watermark is strictly past every
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
`internal/simd.EqualFixed16` into a per-row match bitmap, which also serves phase 2's gather. This
relies on `Condition.Equal` being byte-identical to `Match` for that column — a future caller using
`Equal` as an approximate prune hint would break it, since the fast path never rechecks.

## Part sidecars

| object | contents |
|---|---|
| `bloom-{col}.bin` | per-column token blooms |
| `keys.bin` (`OTKY`, magic+version+CRC32C) | the part's distinct per-record **attribute keys** |
| `sym-{name}.bin` (`OTSP`) | the optional **side store** |

`keys.bin` holds keys, not values: the schema does not bound values, while keys are tiny.
`Engine.Keys` enumerates them across head ∪ in-window parts tagged with a `KeyScope` bitset
(resource/scope/record), so an embedder can list and push down record-attribute labels that
`Series`-based resolution cannot see. It is the enumeration twin of `Engine.Series`.

The side store is a content-addressed auxiliary store a signal attaches per batch (`Batch.Side`),
riding the part lifecycle: absorbed into a live accumulator, written as sidecars on flush, **unioned**
on merge (content addressing makes the union a plain dedup with no id remap), and **restored** into the
accumulator when a flush fails. Profiles' symbol store is the first user; nil for logs/traces.

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
the profile symbol store replicates. `ApplyPrimary`/`ApplyReplicated` mirror the metric engine's
primary-authoritative contract.

**A stream's identity frame is logged when the head registers the stream**, not when it first has an
accepted record. A stream is new exactly once, and replay drops records it cannot attribute to a
registered stream, so a first batch rejected in full (OOO window, in-flight bytes) would otherwise
strand every later record of that stream. The identity is written *before* the head commits the
registration (`head.admitStream` decides, `head.ensureStream` commits), so a failed write never leaves a
registered stream claiming a durability it does not have. An identity frame for a stream that never
gets rows is cheap and harmless.
