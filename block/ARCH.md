# `block/` — the part format (L2)

The immutable, columnar **part**. A part is not one blob: it is a set of backend objects under one
key prefix, so a reader fetches only the columns it references (projection pushdown without ranged
reads).

```
{prefix}/manifest   schema + stats, CRC32C-checked, WRITTEN LAST = the commit point
{prefix}/marks      sparse granule index (sort-key min/max per granule), CRC32C-checked
{prefix}/c/{i}      column i's stream, CRC32C-checked per compression frame
                    (absent for a constant-collapsed column)
```

An incompletely written part (no manifest) is not openable — that is the commit discipline every
writer (flush, merge, partsync mirroring) relies on. `PartPresent` probes that same manifest, which is
how a holder of an already-open part asks whether the part still exists without reopening it.

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

- The **shared-dictionary build** (below) otherwise hashes every row once, into a granule-local
  set that yields the join decision and each row's local id, and each distinct value once more, to
  find it in the column dictionary. Given entry indices both become array work over `int32`s: a
  per-entry generation stamp counts distinct ids without a clear between granules, and a persistent
  source-entry → shared-id remap replaces the interning map.
- The **per-granule chunk encode** (`chunk.EncodeBytesDictRange`, for granules that decline the
  shared dictionary) otherwise probes a hash map per row; from the split form it renumbers indices
  through an array.

Measured on a 64 Ki-row block-framed column, blob input against split, same object either way:

| column shape | blob | split | |
|---|---:|---:|---|
| 512 distinct attribute blobs — every granule joins the shared dictionary | 1.45 ms | 217 µs | 6.7x |
| near-unique message bodies — every granule declines and self-encodes | 8.95 ms | 2.76 ms | 3.2x |

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
Unblocked columns keep the single-stream layout byte-for-byte. Metric parts are block-framed
throughout; record parts frame every per-record column too, leaving only the stream-id column
unframed, which a fetch resolves through the part's row-range index rather than decoding.

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

### Shared-dictionary layout

A dictionary bytes column carries one dictionary D for the whole column; each granule either holds
ids into D or declines it and self-encodes. The writer emits the **trailer** layout:

```
[frames][dictionary region][directory][u32le dirLen]      DictOff/DictLen locate the region
region: [uvarint entries][uvarint packedLen][packed][u32le CRC32C(packed)]
```

The region is byte-identical to the **leading** layout's header (`[region][block-framed
container]`), which parts written before manifest version 3 use and which stays readable. The
dictionary moved behind the frames so a writer can seal granules before D is final — the precondition
for streaming a bytes column — and so `[DictOff, size)` is one ranged read holding both the dictionary
and the directory, against two to four reads for the leading layout.

| mode byte | layout | payload | reader check |
|---|---|---|---|
| 0 shared | leading only | ids at the width the final D implies | `id < len(D)` |
| 1 self | both | a chunk bytes stream | — |
| 2 narrow | trailer only | 1-byte ids | `id < len(D)` |
| 3 wide | trailer only | 2-byte big-endian ids | `id < len(D)`, `len(D) > 256` |

A trailer granule's width is fixed when it joins: narrow while D holds at most 256 entries, wide
after. A whole-column decode widens narrow granules when D ends past 256, so it returns the same
`DictColumn` the leading layout decodes to.

The join decision is today's (`distinct*2 ≤ rows`, `len(D)+distinct ≤ 65536`) plus a **byte cap**
(`WithSharedDictBytes`, 32 MiB default, clamped to the 64 MiB format ceiling `maxSharedDictRaw`). The
cap is charged only for values a granule would *add* to D, each at its serialized size plus 147 B of
resident overhead (slice header, count, index share), so `DictRaw ≤ charge ≤ cap` and the cap changes a
decision only when D would really outgrow it; below it the decisions are the uncapped ones. The
builder works a granule at a time: the flat-value path hashes each row once and each distinct value
once more (the prior build hashed every row twice), the split path hashes nothing.

A column where no granule joins is written unframed, as before — by `PartWriter` and a buffered
`StreamWriter`, which see the whole column. A streamed column has handed its frames out by then, so it
keeps the trailer layout with an empty dictionary; readers take either.

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
`AppendFloat64` / `AppendU128Run` / `AppendBytes` / `AppendBytesBlob` / a `Binding`, and each column
encodes a granule as soon as one fills. Only one
granule of raw rows per column is ever resident, so the writer's working set is the *encoded* part
rather than its uncompressed rows. Output is byte-identical to `PartWriter`'s from the same rows,
tested case-by-case and by fuzz.

`NewStreamWriter` still holds **the whole encoded part**, and `build` then serializes each column's
frames into one buffer, so its peak is about twice the part it is producing (`blockAccum.finish`
therefore allocates at the exact final
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
  can never become one) and, for real data, the second row — or until it holds one frame of sealed
  output (`constRetainBytes`), whichever comes first. A constant stream compresses to almost nothing,
  so one frame covers a very long prefix; past it the column attaches anyway and is aborted if it
  ends constant, costing one wasted object write instead of an unbounded buffer.
`StreamWriter.ResidentBytes()` reports the footprint directly — for a bytes column including G, D and
its bindings' caches, which stay flat in rows once D stops growing — so a caller bounded by memory rather
than by disk seals on the thing it is actually bounded by (`engine/ARCH.md`).

Only encodings that restart per granule can stream, which is the same property block framing needs:
blocked `Int64`/`Float64`/`Bytes`, plus `Int128` whose RLE codec is fed runs directly and never
materializes its rows. An unblocked column of the other kinds is rejected rather than silently
buffered — its single codec stream cannot resume across appends.

### Streaming a bytes column

A `CodecBytesRaw` column stages one granule of values and encodes it at the boundary. A `CodecDict`
column stages the output granule G — its distinct values in first-occurrence order, their counts, each
row's index into them — and at the boundary decides G against the column dictionary D with the same
`sharedDictBuilder` rule and byte cap `PartWriter` uses, so the two write the same granule streams and
a trailer column object is byte-identical from either writer, buffered or streamed. D copies what
joins it into chunked arenas that never reallocate, so an entry stays put while D grows; at close D is
serialized into the region, then the directory and its length follow the frames.

Rows reach G three ways. None hashes a row more than twice (G's index, then D's), plus one insert
per value new to the granule:

- **Values** (`AppendBytes`, `AppendBytesBlob`, a flat granule): a probe of G's index, which holds only
  values absent from D, then of D. Values are copied into G's arena.
- **A `Binding`** to a source's dictionary table caches per entry a stamp, its G index and its D id,
  so a repeated entry costs an array lookup and a D entry already in G (`gOfD`) no probe at all. A D
  id stays valid for the writer's life; the stamps die with each granule.
- **`BindStable`** is a binding over entries that are never overwritten — a decoder's shared
  dictionary — which G references instead of copying. A granule's own table lives in the decoder's
  reused frame buffer, so it is bound with `Bind`, which copies.

Streamed at 8192-row granules, a 512-value attribute column encodes at 1.7 GB/s from values and
4.5 GB/s through a binding, a near-unique body column at 0.38 and 0.43 GB/s (`PartWriter`: 2.3 and
0.35 GB/s from a blob); `ResidentBytes` stays at 0.41 MiB and 2.65 MiB respectively from 64 Ki to
1 Mi rows (`BenchmarkStreamWriterBytes`, `BenchmarkStreamWriterToBytesResident`).

A table is named by a `DictGen` token: a pointer to its owner plus the owner's epoch, comparable and
allocation-free, with no process-wide counter — two decoders over one part never issue equal tokens.
A decoder owns two: the shared dictionary's, whose epoch never moves, so `SharedEntries` and every
shared granule hand back one token valid for the decoder's life; and the self granules', which every
decode retires before it may overwrite the frame buffer a self table aliases. `AppendDict` requires
the bound token *and* a live one, so a stale self table is an error even while the binding still
holds its token; `BindStable` refuses a self token outright, since a reference would outlive the
frame. A shared granule's ids alias the frame all the same — the token names entries, not ids — so
they are the caller's to use before the next decode. `NewDictGen` gives a caller-built table its own
owner. Bindings belong to one writer and die when it finishes.

`Column.Observer` (`BytesObserver`) is the seam for per-value side structures such as blooms. Both
writers report every declined granule's distinct values with counts, then D with its counts once at
close, before the object commits; counts sum to the column's rows. The streaming writer reports a
granule when it seals, so a column that later collapses to a constant may have reported granules but
gets no `Dictionary` call.

Two things the batch writer settles by looking at a finished column, a streaming one cannot:

- **`AutoCodec`** picks between Gorilla and scaled-decimal by trial-encoding. `StreamWriter` runs
  *both* candidates as it streams and keeps the denser at the end, so the choice is still made over
  the whole column, not a prefix. The two run as two writers over one key — the denser commits, the
  loser aborts — which the backend seam makes affordable, the loser's bytes never being in RAM
  either. It compares block-framed sizes where `PartWriter` compares
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

A whole-column read makes cost independent of selectivity — a selector matching 16 of 210k series
still transfers every column byte — so **part size bounds process memory rather than disk**. The
engine decodes only the granules it needs (`engine/ARCH.md`, the block-sliced fetch); the ranged form
is what lets it fetch only those bytes.

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
- A **trailer-dictionary column opens with one read**: `[DictOff, size)` is the dictionary region
  and the directory, both located by the manifest. A column with sizing stats (below) reads a
  footer or leading directory in exactly one read of `DirLen` too.
- A **leading-dictionary column's container does not start at the object's head**: the dictionary
  sits ahead of it, so both directory layouts are located relative to where the container begins
  rather than to the object. The dictionary is read at open, exactly — its two header uvarints give
  its extent. Reading the directory out of the raw object instead parses dictionary bytes as a
  directory and fails as corruption on healthy data, which on a load path is fatal rather than
  degrading.
- `Decoder.DecodeBytes` takes a **block set**, not a block: a granule that declined the shared
  dictionary carries its own, so ids only become comparable once `chunk.DictMerger` has remapped
  them into one. That is also why it cannot decode into a caller's buffer as the numeric paths do.

### Scanning a column forward

`PartReader.ColumnScan(ctx, name, window)` is the same decoder with **read-ahead**: a miss fetches
whole frames up to `window` bytes instead of one frame. It exists because the two access shapes want
opposite things, not because one is a tuned version of the other.

A query touches a handful of frames out of thousands, scattered, so reading ahead fetches bytes it
never decodes. A merge touches every frame exactly once in order — and a frame is 64 KiB
*uncompressed*, so a 256 MiB column is ~4000 frames, 8 sources × 6 columns is ~200k serialized
ranged reads, and `backend.Cache` deliberately does not cache ranged reads, so nothing amortizes
them. At S3 latencies that is the difference between a merge finishing and not.

- The window is **the read side's memory budget**, one buffer per open column: 8 parts × 6 columns
  × 1 MiB is 48 MiB. Streaming bounds the merge's resident set; it does not make it constant, and
  this is the term that stays.
- A window covering the whole column collapses to a single request — `PartReader.Column` minus the
  cache write, which is the right path for the memory backend and for small parts.
- A **frame is indivisible**: one larger than the window is served alone rather than refused, so the
  window is a target, not a cap on what a single fetch may hold.
- A column the ranged path cannot serve is **read whole, once**: the legacy unframed layout, and any
  column object `backend.RangesNatively` denies. There every ranged read — the directory probe, each
  window — is itself a whole-object read, so a windowed walk would read the column once per window.
  The question is per object, not per backend: an EC tenant's backend ranges a hot full copy but not
  a converted one, which it can only reconstruct whole. A backend without the `RangeHinter` answer is
  judged by type, so an `s3.ObjectStore` without `RangeObjectStore` still pays per window.
- `Decoder.TsCursor` / `Decoder.FloatCursor` are `ColumnReader`'s forward cursors over the decoder's
  frames, which is how the metrics merge reads its sources. The record merge decodes granules instead
  (`DecodeInt64Into`, `DecodeBytesBlock`), since it filters rows and remaps dictionaries per granule.
- `Decoder.DecodeBytesBlock` decodes one granule against the cached frame. The result **aliases that
  frame buffer** and dies at the next decode crossing a frame — deliberately: a merge reads a
  granule's ids and appends them, and copying every value back would be the per-row cost this path
  exists to remove. A granule on the shared dictionary yields the *column's* entry table unchanged,
  with the `DictGen` token `SharedEntries` returns — so ids stay comparable across granules and
  nothing is rehashed per granule; any other granule gets a token retired by the next decode. Its `IDWidth` is the granule's own, so a trailer column hands back 1-byte
  ids for granules written before the dictionary passed 256 entries.

## At-rest checksums

Every byte a part stores is covered by a CRC32C, so a column that comes back from the store altered
fails the read instead of decoding into plausible values. Corruption presenting as *absence* — a
damaged column that yields no rows and no error — is worse than a failed query, because a caller
cannot tell it from "there was no data in that range".

The unit is the unit the reader already fetches, which is what keeps verification free of extra I/O
and keeps sub-part seek intact:

| what | checksum |
|---|---|
| manifest, marks | one CRC32C over the object |
| framed column: directory | one CRC32C over the directory fields, immediately after them |
| framed column: each compression frame | one CRC32C in the directory, over the frame's compressed bytes |
| shared bytes dictionary | one CRC32C after the compressed dictionary blob |
| unblocked column (single stream) | one CRC32C trailing the object |

A single checksum over a column object would be the wrong unit: a ranged read decodes only the
granules its row range touches, so verifying one would mean reading the whole object — exactly the
cost the framing exists to avoid. The frame is the floor anyway (nothing below it is separable), and
the directory already lists a `compressedLen` per frame and is already fetched on the ranged path,
so a 4-byte checksum per frame rides along at no extra round trip.

The directory carries its own checksum rather than relying on the frames'. Its frame lengths *are*
covered indirectly — wrong spans slice wrong bytes, whose frame checksums then fail — but its
granule lengths and `blockRows` are never compared against frame bytes, so a flipped bit there would
place decoded rows at wrong offsets with every frame checksum still passing.

It is not a per-column flag: all eight descriptor flag bits are spent, and a checksum is not a
per-column choice. The **manifest version** carries it. Version 2 and later mean "every column object
is checked"; version 1 parts carry no column checksums and are read unverified, so no migration is
forced. The reader accepts the range 1..3 and rejects anything outside it, so an older binary refuses
a newer part outright with `unsupported version N` wrapping `ErrCorrupt`, rather than misreading it. A
version above the reader's range wraps `ErrUnsupportedVersion`, itself an `ErrCorrupt`: callers that
only fail keep failing, and the engines can tell an intact part written by a newer release from a
damaged one, which they hand to repair after repeated loads (`engine/ARCH.md`) and must never do for
a newer one.

Cost: 4 bytes per compression frame (0.006% of a 64 KiB frame), 4 per column directory, 4 per
unblocked object. The hashing itself is hardware CRC32C on both paths and did not move any `block/`
encode or decode benchmark out of the noise.

`block` reports corruption; choosing another copy is not its concern. Turning an `ErrCorrupt` into
the `cluster.ErrShardAbsent` failover that `cluster_completeness.go` runs for a node which cannot
answer a window belongs to the cluster layer.

## Bounded decompression

Every decompression is held to a size the part records, through `compress.DecompressLimit`, so a
CRC-valid object that decompresses past it fails as `ErrCorrupt` (wrapping `compress.ErrLimit`) having
allocated no more than the bound plus the decoder workspace:

| what | bound |
|---|---|
| a trailer dictionary | `DictRaw`, exactly |
| a frame, any version | its granules' lengths from the directory, exactly |
| an unframed stream with sizing | `StreamRaw`, exactly |
| an unframed stream without, or a legacy frame | `RawBytes + 16·rows + 1 KiB` |
| a framed column's frames together, without sizing | `RawBytes + 16·rows + 1 KiB·granules` |
| a leading dictionary | `RawBytes + 5·65536` |

Without sizing the part's `RawBytes` bounds any one column: a chunk stream is its values plus at most
16 B per row (ids, length prefixes, a Gorilla value's worst case) plus a fixed overhead per stream,
which T64 makes large — it writes its last 64-row block's bit planes whole, up to 538 bytes for one
row. A part recording no `RawBytes` decodes unbounded, as before; it is never merged (`ColumnInputSize`).

A bounded zstd decode goes into a destination of the content size plus 128 KiB + 16: the decoder
appends a block before checking it, so without the slack the append would grow the buffer past the
bound. The slack lives as long as the buffer, so nothing kept past a decode carries it:

- a **kept buffer** — a dictionary, whose entries alias it; a decoder's frame buffer, which lives as
  long as the decoder; an unframed stream — decodes into a scratch buffer and is copied out at its
  exact size. Keeping the slack would cost 128 KiB per open zstd dictionary and per open decoder,
  over a gigabyte for 1000 parts × 5 open columns. The scratch buffers come from a fixed set of four
  of at most 1 MiB that survives collections: a `sync.Pool` empties on every other collection, and
  rebuilding a scratch per miss put a ranged open 16% over its allocation before the slack;
- a **whole-column bytes walk** keeps every frame it decoded (the merged column aliases them), so the
  frames decode back to back into one arena sized to their recorded total plus one slack, instead of
  a slack per frame — about three times the column at 64 KiB frames;
- a walk that copies out what it decodes (the numeric decodes, the shared-id fast path) reuses a
  pooled frame buffer, so it neither keeps nor re-zeroes the slack per call.

The zstd decoder workspace is measured at 298 KiB with a cold pool (block buffers, tables, and the
slack); `compress.DecodeWorkspace` rounds it to 512 KiB.

## Manifest & marks

- **Manifest** — versioned binary record (magic `OTPM`, row count, time range, granule size,
  per-column descriptors, then the two sizes) + trailing CRC32C. The version is the one thing that
  is *not* additive: it gates whether column objects carry checksums (above), and the reader accepts
  a range of versions rather than one. `DiskBytes` is the encoded size of
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
  golden churn); `flagBlocked`/`flagFramed`/`flagFooter`/`flagBytes`/`flagSharedDict` are additive the same way.
  `flagBytes` carries the column object's own byte size, so a ranged open needs no size round trip.
  Decode bounds every uvarint as `uint64` before converting it — lengths by the unread remainder,
  row count and granule size by `maxPartRows`, byte sizes by `MaxInt64` — so a CRC-valid manifest
  with an out-of-range field is `ErrCorrupt`, never a wrapped value or a panic (fuzzed).
- **Version 3** adds a per-column `xflags` byte after `flags`; an unknown bit is
  `ErrUnsupportedVersion`, since each gates fields a reader must parse. A writer emits version 3
  only when some column sets a bit and version 2 otherwise, so metric parts stay readable by a
  version-2 reader.
  - `xTrailerDict` → `DictOff, DictLen, DictRaw, DictEntries`. Bounded by subtraction only against
    `Bytes`, and `DictLen` by the largest region a writer produces for `DictRaw` bytes (compression
    never adds more than its flag byte), so no manifest makes a reader request more than ≈64 MiB for
    a dictionary. It requires a blocked, framed, footer, shared-dictionary `CodecDict` bytes column
    with an object; in version 3 a footer shared dictionary without it is corrupt.
  - `xSizing` → `ColumnSizing` (`WithSizingStats`, which the record engine passes): the directory's
    length and counts, the largest compressed and decompressed frame and granule, and the stream
    total. Readers check the directory against it — counts before any index array is allocated,
    maxima and totals exactly — so a manifest cannot understate the column it sizes.
- **`ColumnInputSize`** turns a descriptor into what reading the column holds, from the manifest
  alone. With sizing it is per column: the dictionary plus its decoded headers, the frame and
  directory sizes, the bytes an open reads (the trailer tail, or `DirLen`), and the whole-read
  footprint. Without sizing a column is bounded only through its part's `RawBytes`,
  so it is charged as a whole read of the whole part (`SourceWide`): `RawBytes` twice — the decoded
  values and the decompressed stream of the column being decoded, a second copy for a numeric one —
  plus each column's per-row stream slack and decoded headers, per-granule stream overhead and zstd
  slack. A part without `RawBytes` or without an object size cannot be bounded and saturates.
- **Marks** — sparse granule index over the sort-key column (per-granule first row + min/max,
  delta-encoded, CRC-checked). `Overlapping(lo,hi)` prunes granules for a time window.

Sidecars written *next to* a part (series index, stats, blooms, keys, symbol tables, EC meta) are
owned by the engines, not by `block` — see [`../engine/ARCH.md`](../engine/ARCH.md) and
[`../recordengine/ARCH.md`](../recordengine/ARCH.md).
