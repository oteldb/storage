# `encoding/` — codec foundation

Turns typed value slices into compact byte streams and back. Everything above builds on it.
See [`../ARCHITECTURE.md`](../ARCHITECTURE.md) for the layer map.

## `bitstream`

MSB-first bit `Writer`/`Reader` over caller-owned `[]byte` — the primitive every codec writes
through. Invariants that hold throughout the codecs:

- bulk reads/writes stay on a byte-aligned fast path; flags are **full bytes**, not single bits,
  so subsequent bulk ops stay aligned.
- reads can return **views into the source** (`ReadBytesView`) and writes a window **into the
  output buffer** (`AppendBytes`) — no copy.

## `chunk` — column codecs

Each codec is a pair of append-style functions over caller-owned buffers. Every stream starts
with a uvarint row count. The `Codec` enum names an encoding in column metadata: values are
**persisted and wire-stable — never reorder them**.

| `Codec` | For | Technique |
|---|---|---|
| `CodecDoD` | timestamps | delta-of-delta |
| `CodecGorilla` | float64 | Gorilla XOR |
| `CodecT64` | low-range int64 | ClickHouse T64 bit-transpose + crop |
| `CodecDict` | low-cardinality bytes | dictionary (1 B/row ≤256 distinct, 2 B ≤65536, flat above) |
| `CodecBytesRaw` | high-cardinality byte ids | fixed-width block when all values share a length, else length-prefixed inline |
| `CodecDecimal` | float64 | scaled-decimal + nearest-delta, optionally lossy |
| `CodecID128` | 128-bit ids | run-length — optimal for a sorted SeriesID sort key |

Rules that matter beyond the code:

- The three byte-column forms share one **self-describing** header, so decode selects the form
  from the stream, not from the column's declared `Codec`.
- Every length/count/dictionary id read from the stream is bounds-checked before allocating —
  decode never panics on corrupt input (fuzzed). Layouts pinned by golden files (`_golden/`).
- **Adaptive float codec:** the part writer trial-encodes both float codecs and keeps the
  smaller. Lossless mode takes scaled-decimal only if a verification decode reproduces the values
  (so NaN/±Inf and any precision loss stay on Gorilla, the lossless floor). Lossy mode
  (`FloatPrecisionBits`, set per age tier by the merge engine) retains N mantissa bits but still
  competes against Gorilla, so a lossy tier is never worse than lossless. Lossy error lives in the
  **delta domain**, so it accumulates mildly along a long series.
- **Split dictionary form** (`DictColumn`): unique entries + raw per-row ids, deferring the
  per-row gather so a caller can filter on ids first. All returned slices alias the source.
- **Split-form encode** (`EncodeBytesDict`/`EncodeBytesDictRange`): a caller that already holds a
  deduplicated entry table plus one entry index per row skips the per-row hashing — indices are
  remapped to output dictionary ids through an array. Output is byte-identical to `EncodeBytes`
  over the materialized rows, flat fallback included: both walk the rows in order and both append
  a newly-seen value to the dictionary on first occurrence, so entry order, id width, and the row
  the 65537th distinct value trips the fallback all coincide. The identity holds only if the
  entry table is distinct by value — deduplicating by index is not deduplicating by value. One
  writer (`appendDictPayload`) emits the stream for both entry points, so the two cannot drift.
  The remap is invalidated by a generation stamp rather than a refill: a granule's row range is
  small against a whole column's entry table, so a per-call fill would dominate the encode.

## `compress`

1-byte-flagged frame around a column/block: raw or compressed, automatically falling back to raw
when compression does not shrink. zstd and lz4 both compress (lz4 framed as `[uvarint origLen][lz4
block]`, the block format carrying no length of its own); none = identity. Encoders/decoders pooled.

`DecompressLimit` is the bounded decode every part read uses: it fails with `ErrLimit` rather than
produce more than a limit, allocating at most the bound plus `DecodeWorkspace`. Raw and lz4 check
their recorded length before allocating. zstd cannot be bounded by capping the decoder: its stream
decoder's history is window-sized (512 MiB by default), and its whole-buffer decode checks a cap only
after appending each block, through plain appends that grow the buffer first. So the frame is
validated before decoding — exactly one frame, block headers walked to the last, no block over
128 KiB, a content size within the limit — and decoded into a buffer of the content size plus one
block of slack, which the decode can then never outgrow. A frame without a content size is accepted
only as one block (klauspost omits it only below 256 bytes, and every writer here emits one frame
per call); several blocks would each append unchecked. Both builds decode through klauspost, since
the cgo decoder takes no cap.

## `pool` (sibling package)

`ByteIntMap`: open-addressing `[]byte → int` map (xxh3 + `bytes.Equal`) for the dictionary hot
path — beats `map[string]int` by avoiding string conversion and hashing. Poolable and resettable.
