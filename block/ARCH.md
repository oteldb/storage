# `block/` — the part format (L2)

The immutable, columnar **part**. A part is not one blob: it is a set of backend objects under one
key prefix, so a reader fetches only the columns it references (projection pushdown without ranged
reads).

```
{prefix}/manifest   schema + stats, CRC32C-checked, WRITTEN LAST = the commit point
{prefix}/marks      sparse granule index (sort-key min/max per granule)
{prefix}/c/{i}      column i's stream (absent for a constant-collapsed column)
```

An incompletely written part (no manifest) is not openable — that is the commit discipline every
writer (flush, merge, partsync mirroring) relies on.

## Columns

One physical `Kind` per column (`Int64`/`Float64`/`Bytes`/`Int128`), a codec selected per kind
(overridable), the encoded stream wrapped in a `compress` frame. The writer records min/max and
**collapses a constant column** to a single manifest value with no data object — the OTel
resource-attribute win. `Int128` (the metric SeriesID sort key) is exempt: its RLE codec already
collapses a single-id run. `ColumnReader` is lazy and synthesizes constants with no I/O.

## Block framing

Opt-in (`Column.Block`): a per-row sequential column is split into granule-sized row blocks, each
an **independently decodable** stream (codecs reset their running state at every block's row 0),
flagged `flagBlocked` in the descriptor (additive, no version bump). This buys the sub-part
primitives the engines need:

- `RangeInt64`/`RangeFloat64` — decode only the blocks spanning a row range (seek).
- `DecodeBlocksInt64/Float64` — decode a chosen *set* of blocks into a full-length slice (the
  series-skip primitive).
- `TsCursor`/`FloatCursor` — forward streaming cursors that span block boundaries transparently,
  so the merge reads blocked parts unchanged.

Block boundaries align with marks granules, so marks already carry each block's time bounds.
Unblocked columns keep the prior single-stream layout byte-for-byte. Metric parts are blocked by
default.

**The decode granule is not the compression unit.** A granule is ~1.6 KB of stream — far too little
context for an entropy coder, which would restart its state every granule. Consecutive granules are
therefore concatenated into a *compression frame* of at least `WithCompressBlockBytes` (64 KiB
default, ClickHouse's `min_compress_block_size`) and compressed as a unit; the directory records the
frame spans plus each granule's span inside its decompressed frame, so a single granule is still
decodable on its own. Decode granularity stays `WithGranuleSize`; compression granularity is the
frame. Reads decompress one frame at a time and cache it (`blockStreams`), so any walk in granule
order — whole column, row range, block set, cursor — decompresses each frame exactly once.

A framed column's directory normally *leads* its frames. `flagFooter` marks the one case it trails
them, closed by a fixed 4-byte little-endian directory length so a reader finds its start from the
object's end. The directory fields are identical either way; only where they sit differs. It exists
because with the directory leading, no byte of the object is final until the last frame seals — which
is exactly what a writer streaming its output to the backend cannot accept.

The frame-packed directory is marked `flagFramed`; the older one-compressed-block-per-granule
layout has `flagBlocked` without it and is still read, so parts written before framing need no
rewrite. The writer only emits the framed form.

## Two writers

`PartWriter` takes whole columns and serializes them in one pass. `StreamWriter` builds the same
part incrementally: the schema is declared up front, rows arrive through `AppendInt64` /
`AppendFloat64` / `AppendU128Run`, and each column encodes a granule as soon as one fills. Only one
granule of raw rows per column is ever resident, so the writer's working set is the *encoded* part
rather than its uncompressed rows. Output is byte-identical to `PartWriter`'s from the same rows,
tested case-by-case and by fuzz.

`NewStreamWriter` still holds **the whole encoded part**, and `build` then serializes each column's
frames into one buffer, so its peak is about twice the part it is producing — which made part size a
memory question rather than a disk one (`blockAccum.finish` therefore allocates at the exact final
size and releases each frame as it copies it; a growing buffer would hold a second copy of a
hundreds-of-MiB column).

`NewStreamWriterTo(ctx, b, prefix, …)` removes that: each column opens a `backend.ObjectWriter` and
hands over every frame as it seals, so what stays resident is one unsealed frame per column plus the
block directory — two ints per frame and one per granule, kilobytes against a column of hundreds of
MiB. Those columns carry `flagFooter`, since a streamed directory cannot lead the frames it
describes. Two consequences worth stating:

- **A column cannot stream from its first granule.** A column that turns out constant collapses into
  the manifest and has *no* object, and an object cannot be un-created once bytes are on their way.
  So a column buffers until the rows prove it non-constant — which is monotone (two differing values
  can never become one) and, for real data, the second row. Constant data is also where buffering
  costs least: a run of one value is what these codecs compress hardest.
- **`AutoCodec` opens two writers over one key.** Both candidates stream; the denser commits and the
  loser aborts, so the choice is still made over the whole column rather than a prefix. The backend
  seam is what makes this affordable — the loser's bytes were never in RAM either.

`StreamWriter.ResidentBytes()` reports the footprint directly, so a caller bounded by memory rather
than by disk seals on the thing it is actually bounded by (`engine/ARCH.md`).

Only encodings that restart per granule can stream, which is the same property block framing needs:
blocked `Int64`/`Float64`, plus `Int128` whose RLE codec is fed runs directly and never materializes
its rows. An unblocked int64/float64 column is rejected rather than silently buffered — its single
codec stream cannot resume across appends.

Two things the batch writer settles by looking at a finished column, a streaming one cannot:

- **`AutoCodec`** picks between Gorilla and scaled-decimal by trial-encoding. `StreamWriter` runs
  *both* candidates as it streams and keeps the denser at the end, so the choice is still made over
  the whole column, not a prefix. It compares block-framed sizes where `PartWriter` compares
  whole-column ones, so the two can pick differently in a marginal case — both lossless, so the part
  decodes the same either way. (It also drops one redundant encode pass the batch path does.)
- **`OmitConstColumn`** covers a column the format leaves *absent* rather than constant — the
  sampling weight column, which a reader defaults to 1. A present-but-constant column is not
  block-framed, which would drop readers onto the whole-part decode path, so an all-unit weight
  column is dropped entirely instead. Only the last column may be omitted; dropping an earlier one
  would renumber the object keys after it.

## Manifest & marks

- **Manifest** — versioned binary record (magic `OTPM`, row count, time range, granule size,
  per-column descriptors, then the two sizes) + trailing CRC32C. `DiskBytes` is the encoded size of
  the part's column and marks objects; `RawBytes` is its **decoded** footprint, the bytes its values
  occupy in memory. Both exist because a merge is bounded by both and the ratio between them is the
  compression ratio, which varies per column and per dataset: the metric merge seals on bytes it
  writes, the record merge on bytes it holds. Each is written *after* the columns and read
  optionally, so a manifest without them decodes as 0 and an older reader ignores them: additive, no
  version bump, matching the flag-bit precedent. A descriptor is `[name][kind][codec][compress][flags]`,
  then a `FloatPrecisionBits` byte **only when `flagLossy` is set** and a compression-level byte
  **only when `flagLevel` is set** (decode-irrelevant — it exists so the merge engine can tell a part
  already at its target level from one below it), then per-kind stats/const. The
  flag-gating is what keeps lossless and pre-existing parts byte-identical (no version bump, no
  golden churn); `flagBlocked`/`flagFramed` are additive the same way. Decode bounds-checks every
  field (fuzzed).
- **Marks** — sparse granule index over the sort-key column (per-granule first row + min/max,
  delta-encoded, CRC-checked). `Overlapping(lo,hi)` prunes granules for a time window.

Sidecars written *next to* a part (series index, stats, blooms, keys, symbol tables, EC meta) are
owned by the engines, not by `block` — see [`../engine/ARCH.md`](../engine/ARCH.md) and
[`../recordengine/ARCH.md`](../recordengine/ARCH.md).
