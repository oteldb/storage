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
each compressed on its own, flagged `flagBlocked` in the descriptor (additive, no version bump).
This buys the sub-part primitives the engines need:

- `RangeInt64`/`RangeFloat64` — decode only the blocks spanning a row range (seek).
- `DecodeBlocksInt64/Float64` — decode a chosen *set* of blocks into a full-length slice (the
  series-skip primitive).
- `TsCursor`/`FloatCursor` — forward streaming cursors that span block boundaries transparently,
  so the merge reads blocked parts unchanged.

Block boundaries align with marks granules, so marks already carry each block's time bounds.
Unblocked columns keep the prior single-stream layout byte-for-byte. Metric parts are blocked by
default.

## Manifest & marks

- **Manifest** — versioned binary record (magic `OTPM`, row count, time range, granule size,
  per-column descriptors) + trailing CRC32C. A descriptor is `[name][kind][codec][compress][flags]`,
  then a `FloatPrecisionBits` byte **only when `flagLossy` is set**, then per-kind stats/const. The
  flag-gating is what keeps lossless and pre-existing parts byte-identical (no version bump, no
  golden churn); `flagBlocked` is additive the same way. Decode bounds-checks every field (fuzzed).
- **Marks** — sparse granule index over the sort-key column (per-granule first row + min/max,
  delta-encoded, CRC-checked). `Overlapping(lo,hi)` prunes granules for a time window.

Sidecars written *next to* a part (series index, stats, blooms, keys, symbol tables, EC meta) are
owned by the engines, not by `block` — see [`../engine/ARCH.md`](../engine/ARCH.md) and
[`../recordengine/ARCH.md`](../recordengine/ARCH.md).
