# Immutable part series index

The index belongs to its sample part and uses the same backend and catalog publication.
It owns no WAL, coordinator, worker or mutable global catalog. Writers retain one output part's
compact interned identities and label-reference lists; callers charge `Builder.Bytes` to build
admission. Legacy identity objects remain available to earlier readers.

## Table framing

Keys are strictly ordered and prefix-compressed within independently LZ4-compressed pages.
Each packed page has CRC32C. Its sparse directory entry contains the first-key offset/length/CRC,
page offset, packed/raw lengths, record count, and an entry CRC (40 bytes). The 40-byte footer
contains magic `OSIX`, version 1, directory offset, page/key/series counts, maximum decoded page
size, reserved bytes, and CRC32C. Opening retains only this footer; seeks range-read directory
entries and one selected page. Oversized records are bounded at 64 MiB and require corresponding
caller admission. Opaque backends retain their existing whole-object fallback.

## Key spaces

| Prefix | Key suffix | Value |
|---|---|---|
| `c` | empty | distinct series count |
| `h` | escaped label name, chunk's last uint32 reference | label-presence references |
| `i` | canonical 128-bit SeriesID | uint32 part-local reference |
| `n` | escaped label name | non-string-value flag |
| `p` | escaped name, escaped typed value, chunk's last reference | matching references |
| `s` | uint32 reference | canonical ID, coverage timestamp, optional raw stats, canonical identity |

Local references follow canonical-ID order. Posting chunks contain at most 1,024 delta-varint
references; typed values preserve the existing missing-label predicate semantics. Finite string
hints seek directly only when that label contains no non-string values. Bitmap unions bound work
over many matching values by part cardinality; the existing postings iterators intersect the
result without intermediate ID lists. Selected identity payloads can be copied after caller
admission; unselected identities are not materialized. Coverage metadata supports timestamp
admission without loading attributes.

The sorted prefix-key/cursor model follows VictoriaMetrics'
[mergeset searches](https://github.com/VictoriaMetrics/VictoriaMetrics/blob/master/lib/mergeset/table_search.go).
Part-local references and iterator set operations follow the
[Prometheus block index](https://github.com/prometheus/prometheus/blob/main/tsdb/index/postings.go).
The backend range/admission boundary is shared with the existing recordengine lazy-projection path.
