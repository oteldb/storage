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
per-stream ts sort permutes byte columns through **one scratch column** shared across every column
and stream; an already-ordered stream skips the permute entirely.

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
  lookups): phase 1 decodes only ts, int columns and the *condition* byte columns (lazy
  `chunk.DictColumn`, O(1) `At`) and scans; a part with no match (a bloom false positive) never
  decodes its projected byte columns. Phase 2 decodes the rest and gathers only matched rows.
  - **Equality fast path**: an exact-match condition against a `CodecBytesRaw` column no other
    condition targets skips the dict decode — the flat blob is decoded once and scanned with
    `internal/simd.EqualFixed16` into a per-row match bitmap, which also serves phase 2's gather.
    This relies on `Condition.Equal` being byte-identical to `Match` for that column; a future
    caller using `Equal` as an approximate prune hint would break it (the fast path never rechecks).
- **Top-N pushdown** (`Limit`+`Reverse`, `limitscan.go`) — an *unfiltered* limited request reads
  live parts in time order and stops once it holds `Limit` rows whose watermark is strictly past
  every unread part's bounds. Strict comparison keeps boundary ties, so the result stays a correct
  **superset** for the caller's own exact ordering. Disabled with conditions, whose per-part
  survivor count is unknown until the filter runs.
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
  union a plain dedup with no id remap). Profiles' symbol store is the first user; nil for
  logs/traces.

## WAL & cluster

The WAL frame is signal-agnostic (an opaque engine-encoded payload) plus an optional side frame;
`recordengine` owns the codec and `EncodeWAL` (the cluster write form, which appends the side frame
so the profile symbol store replicates). `ApplyPrimary`/`ApplyReplicated` mirror the metric engine's
primary-authoritative contract.
