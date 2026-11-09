# Immutable part series index

The index belongs to its sample part and uses the same backend and catalog publication.
It owns no WAL, coordinator, worker or mutable global catalog. Writers retain one output part's
compact interned identities and label-reference lists; callers charge `Builder.Bytes` to build
admission. `EncodedBound` also prices buffered output using cumulative canonical identity bytes,
including repeated shared attributes; one reusable serialization buffer belongs to `Bytes`.
Legacy identity objects remain available to earlier readers.

## Table framing

Keys are strictly ordered and prefix-compressed within independently LZ4-compressed pages.
Each packed page has CRC32C. Its sparse directory entry contains the first-key offset/length/CRC,
page offset, packed/raw lengths, record count, and an entry CRC (40 bytes). The 72-byte footer
contains magic `OSIX`, version 1, directory offset, page/key/series counts, maximum decoded page
size, a SHA256 digest of the parent manifest, reserved bytes, and CRC32C. Opening retains only this footer; seeks range-read directory
entries and one selected page. Oversized records are bounded at 64 MiB and require corresponding
caller admission. Budgeted reads require native object sizes and ranged reads before opening.
Unbudgeted opaque backends retain their whole-object fallback.

The parent digest covers raw manifest bytes and excludes object paths, so part copies and renames
preserve the binding. Engines reject missing or mismatched bindings before publishing a handle.
Generic tables may carry a zero digest. A valid page is fully checked for strictly ordered keys
before exposing records; writer errors and finalization are terminal.

`MaxPageBytes` covers packed/raw data, current/first/last keys, directory scratch and buffer growth.
Buffers grow to explicit capacities bounded by the largest raw page; cancellation joins page I/O.
`EncodedBound` covers raw record framing, a directory first-key copy and worst-case one-page-per-record
framing, independently of compression ratio or identity interning.

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
