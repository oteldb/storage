# `index/` — identity & inverted index (L3)

Maps query matchers to the series/streams that satisfy them. Identity itself lives in `signal`.

## Identity (`signal`)

`Value` is the OTel AnyValue sum (scalars inline, `[]byte` for zero-alloc projection), grouped
into a sorted `Attributes` set. A **`Series`** is the full OTel identity backbone — Resource
(schema_url + attrs) + Scope (name, version, schema_url, attrs) + the data-point attributes — so
series differing only in resource or scope stay distinct.

`SeriesID` is a **128-bit** xxh3 over a canonical, type-tagged, length-delimited pre-image (maps
hash order-independently, arrays keep order, `int 5`/`"5"`/`5.0`/empty are distinct). 128 bits
because content addressing has no allocator to resolve a collision.
`Series.AppendHashInput`/`DecodeSeries` — and `AppendValue`/`DecodeValue` for a single value — are
the reversible binary codec (WAL + interning). Signal-specific identity (metric name/unit/
temporality, profile type) folds into the pre-image as reserved labels at the signal layer.

## `symbols`

Owned symbol tables initialize a small private byte map instead of borrowing temporary encoder
capacity from the shared pool. Pruning therefore sizes a rebuilt index for its surviving symbols;
unrelated dictionary workloads cannot inflate the resident identity state.

`[]byte → uint32` interning table over `pool.ByteIntMap` (no string conversion), CRC32C-framed
(`OTSY`). Names and typed-value encodings intern to small ids.

## `series`

[`internal/seriesindex`](../internal/seriesindex/ARCH.md) supplies optional immutable per-part
identity/postings indexes, with bounded ranged reads and typed local-reference postings.

`SeriesID ↔ Series`. Canonical hashes and durable formats use the full typed identity.
`Index.AddChecked` verifies an expected canonical hash before changing identity state and returns
the encoded length, so checked output builders do not encode/hash the identity twice. The ordinary
`Index.Add` computes the canonical hash; `Index.AddEntry` accepts the existing `Entry` (ID plus
identity) from trusted projection or validated readers, avoiding that encoding and hash. It still
owns all retained payloads. Untrusted identities use `AddChecked`.
Resident lookup keys use the low 64 bits, then verify the high half against the stored row.
Low-half collisions use a full-ID overflow map. Canonical IDs and durable formats remain 128-bit.

The resident registry uses 32-byte rows: hash, value-arena offset, shared resource/scope
context, and shared attribute-name schema. Values occupy four-byte typed-symbol references;
attribute-name sequences are stored once per schema rather than repeated for every series.

`View` captures immutable append-only arenas for off-lock flush and pruning. `GetInto` and
`View.At` materialize only requested identities into reusable caller buffers. The compatibility
`Snapshot` API materializes entries; pruning uses compact views. `View.AppendCanonical` emits the
canonical identity from shared context and encoded point symbols without constructing attribute
values. `EncodedAttributes` borrows those point symbols for encoders and postings builders.
`Index.AddView` copies them into an owned registry, preserving their established canonical ID and
remapping source symbols; source references never escape into the destination's durable data.
Metrics flush and merge encode identities one at a time, so their snapshots do not expand the
resident registry.

`series.Contexts` optionally shares immutable Resource/Scope payloads across registries. The
64-shard pool compares canonical keys and charges retained keys and expanded payloads before
publishing entries, including slack for mostly empty map groups. It never evicts or recycles borrowed storage; full shards use ordinary local
interning. The caller budgets the pool separately; index sizes exclude shared attribute arrays.
Point symbols, local references, postings, admission watermarks and canonical IDs remain local.
Each registry caches its empty resource/scope references, skipping canonical encoding and map
lookups after first sight; scope name, version and schema URL still distinguish nonempty contexts.
An originating index can disappear while pool entries or other index views still retain payloads.

`AttributeTemplates` optionally shares fixed point-attribute combinations across registries.
Templates own preencoded canonical segments and pointer-free scalar descriptors; rows retain
only positional variable-value symbols. Field offsets refer directly to the canonical storage.
A bounded evidence table requires repeated new registrations before retaining a combination;
the heuristic does not guarantee resident break-even, and earlier flat rows stay immutable.
Low-recurrence input temporarily bypasses candidate encoding,
with periodic retries so workload changes can restore sharing. Duplicate IDs add no evidence.
The caller charges the pool separately; full shards fall back to ordinary local tuples. Templates
are immutable and never recycled while views can reference them. Canonical IDs and disk bytes do
not change; resource/scope contexts and template pools remain independent.
Registration first checks the previous local template by exact keys and typed fixed values,
retaining only variable deltas on a match. A mismatch uses ordinary canonical-key lookup;
signed zero, NaN payloads, raw booleans and nested values keep their exact identity encoding.
Eight consecutive mismatches back off to one comparison per 64 registrations until locality returns.

## `postings`

The inverted index is keyed on interned name/typed-value symbols. `MemPostings` stores canonical
128-bit IDs; `Dense` stores four-byte local ordinals. The metrics engine resolves its local ordinals
back to canonical hashes before accessing parts. Local ordinals never appear in durable data or RPCs.

Sorted, unique local-ordinal lists that span a contiguous interval use arithmetic iterators with
constant-time seeks. Gapped lists retain the array iterator and binary-search seek.

- Lazy set-op iterators (`Intersect`/`Merge`/`Without`, monotone `Seek`), property-tested against
  a naive reference. `Merge` is a binary-min-heap k-way union (O(N·log k)), so a high-cardinality
  matcher resolving across thousands of value buckets stays fast.
- Matching is **callback-based**: `Select(nameID, func(valueID) bool)` hands the predicate a
  candidate id which the caller decodes to a typed `signal.Value` and tests. Storage imports no
  query-language operator; negation/equality compose from `Get`/`Without`/`WithoutName`.
- Sorting is lazy and caller-synchronized (`Sorted()`/`EnsureSorted()`), so a reader can upgrade to
  the write lock exactly once after a write.

## `identity`

The **on-disk** form of a part's identity set — the `{part}/identity` object — encoded against a
symbol table private to the object (measured ~3.8× smaller than repeating the label bytes per
series; ~40 B/series on a churn-shaped set). Sections are addressed by a trailing TOC located from a
fixed 8-byte trailer — its offset plus the TOC's own CRC32C — so a section can be fetched by byte
range without reading the
object and a new section kind (the postings and offset tables a per-part *index* would need) can be
added without breaking a reader. Deliberately uncompressed: interning already removes the repetition
a compressor would find, and a compressed body would defeat that range-addressability.

Scoping identity to the part is what makes retention self-cleaning and gives each node a live
identity set derived from its own parts — the shape Prometheus/Mimir blocks and VictoriaMetrics'
per-partition indexDB have. Fuzzed (decode never panics; encode∘decode is the identity and the
decoded identity still hashes to its id) and golden-tested.

`identity.WriteSorted` streams the same v1 object through a caller-supplied writer, emitting
series before symbols and recording their locations in the existing TOC. Existing readers
resolve sections by kind, so both orders are readable; `Encode`/`EncodeSorted` retain their
symbols-first bytes. Streaming retains one record's scratch and the part-local symbol table,
without materializing the full series section or final object. `WriteViewSorted` produces the same
streaming bytes from compact views: typed point-value encodings are re-interned by content into
the output symbol table, without materializing values or copying source-local symbol IDs.
`symbols.Table.WriteTo` streams
the exact bytes of `Table.Encode`; the destination owns buffering.

## Metering

`series.EncodingBudget` estimates one selected identity run's encoder workspace from its immutable
view. Symbol and context IDs deduplicate payload charges; unused registry entries add no workspace.
The accumulator clears and reuses its maps between runs while retaining their capacity charge.
`View.IdentityWorkspace` preflights an indivisible large identity without allocating the estimator.

Each structure reports its resident footprint (`SizeBytes`), accumulated on insert rather than
walked, so the engine can meter identity state — which no sample/record byte counter sees and which
a flush does not drain (see `ADMIN.md`'s `IdentityBytes`). Every byte is charged once, where it is
owned: interned payloads to the symbol table, so an identity referencing them is not double-counted.
Estimates come from `internal/memsize`, the one place that needs `unsafe.Sizeof`; they are
structural (map slots, backing-array capacity, interned bytes) and ignore size-class rounding and
map-slot padding — measured ~10 % under the heap delta at 200k series.

## `bloom`

Token bloom filter (bit array, k xxh3-128 double-hash probes, versioned + CRC'd, **no false
negatives**), one per bloom-bearing record column. The mode comes from the record schema
(`recordengine.BloomFullText`, tokenized value; `BloomAttrs`, key-scoped `key‖value` and `key‖word`
tokens; `BloomEquality`, verbatim values — the trace-by-id path).

Sizing is by **distinct** tokens, not occurrences (occurrences inflate the filter by the column's
repetition factor): a filter that would be small anyway keeps the cheap occurrence count, a larger
one is sized from a constant-space HyperLogLog estimate. FP targets differ per mode (`1e-6`
equality, `1e-2` full-text/attrs). The sidecar is self-describing (`k`, `m` encoded), so parts
written under any sizing stay readable.

`Decode` bounds `k` to `[1, 64]` and `New` clamps to the same range, so the two agree for every
rate the engine uses (`k = log2(1/p)`). Both ends of an unbounded `k` are silent: `0` — or a value
that truncates to a negative `int` — is a filter that probes nothing and tests present for
everything, and a large one is per-probe CPU spent on every condition of every part.
