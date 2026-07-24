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

## `compress`

1-byte-flagged frame around a column/block: raw or compressed, automatically falling back to raw
when compression does not shrink. zstd implemented, none = identity, lz4 currently takes the raw
path. Encoders/decoders pooled.

## `pool` (sibling package)

`ByteIntMap`: open-addressing `[]byte → int` map (xxh3 + `bytes.Equal`) for the dictionary hot
path — beats `map[string]int` by avoiding string conversion and hashing. Poolable and resettable.
