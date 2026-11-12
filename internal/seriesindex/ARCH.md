# Immutable part series index

The index belongs to its sample part and uses the same backend and catalog publication.
It owns no WAL, coordinator, worker or mutable global catalog. Writers retain one output part's
compact interned identities and label-reference lists; callers charge `Builder.Bytes` to build
admission. `EncodedBound` also prices buffered output using cumulative canonical identity bytes,
including repeated shared attributes; one reusable serialization buffer belongs to `Bytes`.
Legacy identity objects remain available to earlier readers.
`Builder.AddView` copies from an immutable compact registry using its established canonical ID and
encoded point attributes. Public `Add` still checks externally supplied identities against their
IDs through one checked registry insertion, reusing its canonical length for bounds. Postings consume typed encoded bytes; descriptor and legacy identity writers read compact
views directly, avoiding repeated materialization and rehashing. Each output registry owns its
symbols and resource/scope context; source-local ordinals are remapped, never persisted or borrowed.

## Table framing

Keys are strictly ordered and prefix-compressed within independently LZ4-compressed pages.
Each packed page has CRC32C. Its sparse directory entry contains the first-key offset/length/CRC,
page offset, packed/raw lengths, record count, and an entry CRC (40 bytes). The 72-byte footer
contains magic `OSIX`, version 1, directory offset, page/key/series counts, maximum decoded page
size, a SHA256 digest of the parent manifest, reserved bytes, and CRC32C. Opening retains only this
footer; seeks range-read directory entries and one selected page. Forward seeks inside a validated
page avoid directory I/O. Oversized records are bounded at 64 MiB and require corresponding
caller admission. Budgeted reads require native object sizes and ranged reads before opening.
Unbudgeted opaque backends retain their whole-object fallback.
Directory entries, seek keys and compressed pages use the backend's immutable-range cache when
available, sharing its configured byte budget with data columns. Validated decoded pages retain
full keys/values
in one immutable byte arena with uint32 offsets for binary search and ordinal iteration. Expansion
is capped at three times the raw page size; larger prefix expansions retain validated raw bytes
and the final key instead. Misses coalesce before decompression/validation. Cursor reset drops
borrowed slices before any writable reuse. Seeks enable decoded-page admission; `Next` reuses
the current page, then loads further pages through streaming scratch. One-pass scans avoid
expanding/admitting every page. Bare backends keep reusable ranged reads.

The parent digest covers raw manifest bytes, including a persistent per-part nonce, and excludes
object paths, so part copies and renames preserve the binding. The nonce distinguishes otherwise
identical manifests whose per-series sample assignments differ. Engines reject missing or mismatched
bindings before publishing a handle.
Generic tables may carry a zero digest. A valid page is fully checked for strictly ordered keys
before exposing records; writer errors and finalization are terminal.

`MaxPageBytes` covers packed/raw data, current/first/last keys, directory scratch, buffer growth
and a pinned decoded page during a cold load, independently of cache eviction.
Buffers grow to explicit capacities bounded by the largest raw page; cancellation joins page I/O.
`EncodedBound` covers raw record framing, a directory first-key copy and worst-case one-page-per-record
framing, independently of compression ratio or identity interning.

## Key spaces

| Prefix | Key suffix | Value |
|---|---|---|
| `c` | empty | distinct series count |
| `h` | escaped label name, chunk's last uint32 reference | label-presence references |
| `i` | canonical 128-bit SeriesID | uint32 reference, coverage timestamp, stats flag, optional raw stats |
| `n` | escaped label name | non-string-value flag |
| `p` | escaped name, escaped typed value, chunk's last reference | matching references |
| `s` | uint32 reference | canonical ID, coverage timestamp, optional raw stats, canonical identity |

Local references follow canonical-ID order. Posting chunks contain at most 1,024 delta-varint
references; typed values preserve the existing missing-label predicate semantics. Finite string
hints seek directly only when that label contains no non-string values. Bitmap unions bound work
over many matching values by part cardinality; the existing postings iterators intersect the
result without intermediate ID lists. Selected identity payloads can be copied after caller
admission; unselected identities are not materialized. New ID values are 13 bytes without stats
and 45 bytes with stats. Four-byte legacy values remain readable through their descriptor fallback.
A complete ID traversal validates canonical reference ordinals and the exact footer count, retaining
only a success flag; engines already perform that scan against SIDX before publication. Metadata
probes then avoid descriptor pages. Full identity lookups check agreement with inline metadata.

Historical append probes sort pending IDs and prune each part by its validated ID bounds. One
operation-local ID cursor serves the whole batch; legacy values lazily add a descriptor cursor.
Identity-free counts read selected canonical IDs from the corresponding SIDX ordinals, while
label-bearing queries still load the selected identities. Decoded pages share the backend cache; complete materialized identities are not cached.


The sorted prefix-key/cursor model follows VictoriaMetrics'
[mergeset searches](https://github.com/VictoriaMetrics/VictoriaMetrics/blob/master/lib/mergeset/table_search.go).
Part-local references and iterator set operations follow the
[Prometheus block index](https://github.com/prometheus/prometheus/blob/main/tsdb/index/postings.go).
The backend range/admission boundary is shared with the existing recordengine lazy-projection path.

Historical admission sorts only its current batch's unknown IDs and reuses one metadata reader
across parts. Reset drops the previous owner, cursor state and errors while retaining owned
page scratch. Owned scratch follows the largest visited page and dies at batch return; shared decoded pages
remain under the backend cache budget. Legacy descriptor scratch remains lazy.

Compaction checks every contributing descriptor and combines coverage before decoding one canonical
identity per output ID. Equal-ID new-format payloads must agree byte-for-byte; legacy registries
already contain validated canonical identities. The selected descriptor is consumed into owned
compact output state before any source cursor advances. Source hash checks and checked output
insertion remain independent validation boundaries.

Compaction can register checked identities without rebuilding their label maps. `PostingMerge`
retains sorted source/output reference pairs and streams the source presence/name/value key spaces
into the output table. Equal keys merge sorted references, dropping duplicates and excluded series.

A source mapping every output reference supplies all memberships because source descriptors already
agree with the checked canonical output identities. The smallest covering index alone is read.
Otherwise broad unions use the merged path only when at most two source references map to each
output series on average. Sparse or duplicate-heavy outputs rebuild from owned compact identities.

Source-local ordinals never escape their parent index. Dense remapping windows are used only when
at most four slots cover each selected reference; sparse mappings stay compact. Source pages,
owned grouping keys, chunk buffers, remapping windows and sparse-fallback construction are admitted.
Outputs selecting less than a quarter of the chosen sources' references avoid rescanning their
postings and rebuild from compact identities instead.

Reuse trusts the same posting membership semantics as query resolution. Every visited source page,
escaped key, typed value, reference range, chunk order and name flag is checked, even for excluded
references. Unused membership indexes need no reads; all contributing identity descriptors retain
their independent validation. CRC-valid missing postings are not reconstructed on this path.

Forward seeks retain the last requested key and its successful lower-bound position. Increasing
probes inside the same absent-key gap reuse that position; `Next` and reset invalidate the proof.
The seek-key buffer is included in the per-cursor memory bound. Cold pages still undergo full CRC/order validation before cache admission. Warm expanded pages
support backward seeks without decompression or reconstruction; compact fallback pages rewind
through validated prefix records.


Decoded-page misses reuse separate cursor-owned decompression and key scratch. Cache results own
their bytes; borrowed raw/cache views never become decoder destinations. Scratch remains inside
MaxPageBytes and is dropped when resetting to a bare backend's smaller bound. Ordered canonical
identity checks select StreamPages, retaining raw page buffers instead of expanding each crossed
page. Random readers retain decoded-cache seeks. Descriptor hashes and canonical-byte comparisons
remain mandatory in both modes.
