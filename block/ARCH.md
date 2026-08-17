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

### Bytes column input forms

A `KindBytes` `Column` carries its cells in one of three shapes, so a caller hands over whatever it
already holds instead of materializing the one the encoder happens to want. Exactly one is set; the
writer picks by `bytesBlobForm()` / `bytesSplitForm()`, falling back to `Bytes`.

| form | fields | cell *i* | who produces it |
|---|---|---|---|
| slices | `Bytes [][]byte` | `Bytes[i]` | callers holding per-row values (tests, ad-hoc writers) |
| blob | `BytesBlob []byte`, `BytesOffsets []int32` | `BytesBlob[BytesOffsets[i]:BytesOffsets[i+1]]` | a flush — the head buffer's `byteCol` layout, passed straight through |
| split | `BytesDict [][]byte`, `BytesIDs []int32` | `BytesDict[BytesIDs[i]]` | a merge — what reading a dictionary-encoded column already gives it |

```
slices   ["GET /a"] ["GET /a"] ["POST /b"] ["GET /a"]      one header per row

blob     offsets  0 ──── 6 ──── 12 ─────── 19 ──── 25      one blob, one offset per row
         data     GET /a GET /a POST /b    GET /a

split    dict     0:"GET /a"  1:"POST /b"                  one entry per distinct value,
         ids      0    0    1    0                         one int32 per row
```

The split form is `chunk.CodecDict` only, and `BytesDict` must be **distinct by value**. Both are
validated or documented at the seam: the merge keeps raw columns (trace ids) flat, so a raw
split-form encoder would have no caller, and the encoders dedup by *index*, so a duplicated entry
would emit a second dictionary entry — a valid stream, but no longer the identical one.

**All three produce byte-identical objects and descriptors.** That is what makes the choice a
performance decision and never a format one, and it is what the tests assert (including a variant
whose entry table is reversed and ids renumbered, pinning that dictionary order comes from row order
rather than from the caller's table). It holds because every path that reads cells — const collapse,
the single-stream encode, the per-granule encode, and the shared-dictionary build — walks the rows in
order and appends a value on first occurrence, so entry order, id width, and the row a fallback trips
on all coincide.

Two consumers pay for the form, and the split one is cheap in both:

- The **shared-dictionary build** (below) otherwise hashes every row twice — once to count a
  granule's distinct values for the join decision, once to assign each row its dictionary id. Given
  entry indices both become array work over `int32`s: a per-entry generation stamp counts distinct
  ids without a clear between granules, and a persistent source-entry → shared-id remap replaces the
  interning map.
- The **per-granule chunk encode** (`chunk.EncodeBytesDictRange`, for granules that decline the
  shared dictionary) otherwise probes a hash map per row; from the split form it renumbers indices
  through an array.

Measured on a 64 Ki-row block-framed column, blob input against split, same object either way:

| column shape | blob | split | |
|---|---:|---:|---|
| 512 distinct attribute blobs — every granule joins the shared dictionary | 3.33 ms | 290 µs | 11.5x |
| near-unique message bodies — every granule declines and self-encodes | 11.17 ms | 1.65 ms | 6.8x |

The first isolates the shared-dictionary build: its granules hold raw ids, so no chunk stream is
written at all. The second is dominated by the per-granule encode, plus the shared-dictionary scan
that runs and then declines.

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

`ColumnReader.Frames` exposes that map — each frame's row span and compressed size — for a caller
attributing a column's *compressed* bytes to a subset of its rows. The frame is the floor: nothing
below it is separable, since the entropy coder shares state across the whole frame, so a per-row
attribution is necessarily an apportionment of a frame (see `StreamCosts` in `ADMIN.md`). An
unframed or constant column reports one extent covering every row, so the caller needs no special
case; the extents' bytes sum to less than `ObjectBytes` by the directory and any shared dictionary,
which belong to no single frame.

### Decoding a shared-dictionary column

Granules that encode ids into the column-wide dictionary decode by copying those ids
(`decodeSharedIDs`): no per-granule dictionary, no hashing, one reusable decompression buffer. A
granule that declined it carries its own stream, so a column mixing both modes cannot take that path
for the whole selection and merges through `chunk.DictMerger` instead.

That merge is seeded with the column dictionary once (`DictMerger.SeedShared`), after which a shared
granule costs one array lookup per row. Seeding is what makes the mixed path a *constant* extra cost
rather than a cliff: without it each shared granule reached the merge carrying the whole column
dictionary as its own, and the merge hashed every entry of it once **per granule** — granules ×
entries probes, against zero on the fast path.

| whole-column decode, 40 granules × 1024 rows | unseeded | seeded |
|---|---:|---:|
| every granule shares (fast path, unchanged) | 259 µs | 259 µs |
| one self-encoded granule | 7.48 ms | 1.21 ms |
| five self-encoded granules | 5.84 ms | 1.40 ms |

On real p90 columns: attributes 42.3 ms → 28.1 ms, bodies 5.88 ms → 2.73 ms. The seed is lazy — it
runs on the first shared granule of a selection, not at the start of the merge, so a selection holding
only self-encoded granules puts no unreferenced entries in the merged dictionary.

A seeded merge also keeps the dictionary where an unseeded one would abandon it. `DictMerger.Append`
flattens on a granule holding one entry per row — with nothing to repeat into, merging it would build a
column-sized dictionary and then overflow anyway — but that reasoning inverts once a column-wide
dictionary is seeded: the other granules index it, so flattening for one unique granule takes *their*
ids away too, and with them the per-distinct-entry predicate memo for every row of the column. Seeded,
the unique granule's entries are merged like any others and only the 65536-entry ceiling flattens the
result. Measured on a real attributes column, whole-column decode: 324699 flat rows before, a
60509-entry dictionary after — 5.4x fewer predicate evaluations for a scan that filters on it, and
1.5 MB of entry headers instead of 7.8 MB.

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

## Reading a column by range

`PartReader.Column` reads a column object whole; `PartReader.ColumnBlocks(ctx, name)` reads only its
**directory** and then fetches each compression frame with `backend.ReadAt` as blocks are decoded.
Both are right for different callers: the whole-object form when a caller decodes the whole column,
the ranged form on the query path, where the matched series' rows lie in a handful of granules.

Whole-column reads made read cost independent of selectivity — a selector matching 16 of 210k series
still transferred every column byte — so **part size bounded process memory rather than disk**. The
engine already decoded only the granules it needed (`engine/ARCH.md`, the block-sliced fetch); the
missing piece was purely the ability to fetch a byte range.

- The **directory** is read up front and kept: two integers per frame and one per granule, ~1.4 MB
  for an 833 MB column. Everything else is derived from it.
- Finding it needs the object's size, and asking the backend for one costs a round trip whose
  fallback (`backend.SizeOf` over a backend without `Sizer`) is *reading the whole object*. So the
  manifest records each column's object size (`flagBytes`); only a part written before that falls
  back to asking.
- The **footer** layout (`flagFooter`, what the streaming writer emits) puts the directory length at
  a known offset from the end, so one tail read usually lands the whole directory. The
  directory-leading layout has no recorded length, so its extent is bounded from the counts in its
  own header — one probe read, then one exact read.
- That bound must allow for a granule length being measured in the **decompressed** frame, which can
  dwarf the compressed object holding it (200k rows can be a 1.1 KB object). A bound derived from
  the object size would come up short and the directory would parse as corrupt.
- The **compression frame is the floor**: it is the smallest unit a ranged read can fetch, so a
  single-series fetch pays one frame per column however few rows it wants.

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
  golden churn); `flagBlocked`/`flagFramed`/`flagFooter`/`flagBytes` are additive the same way.
  `flagBytes` carries the column object's own byte size, so a ranged open needs no size round trip.
  Decode bounds-checks every field (fuzzed).
- **Marks** — sparse granule index over the sort-key column (per-granule first row + min/max,
  delta-encoded, CRC-checked). `Overlapping(lo,hi)` prunes granules for a time window.

Sidecars written *next to* a part (series index, stats, blooms, keys, symbol tables, EC meta) are
owned by the engines, not by `block` — see [`../engine/ARCH.md`](../engine/ARCH.md) and
[`../recordengine/ARCH.md`](../recordengine/ARCH.md).
