# `index/` — identity & inverted index (L3)

Maps query matchers to the series/streams that satisfy them. Identity itself lives in `signal`.

## Identity (`signal`)

`Value` is the OTel AnyValue sum (scalars inline, `[]byte` for zero-alloc projection), grouped
into a sorted `Attributes` set. A **`Series`** is the full OTel identity backbone — Resource
(schema_url + attrs) + Scope (name, version, schema_url, attrs) + the data-point attributes — so
series differing only in resource or scope stay distinct.

`SeriesID` is a **128-bit** xxh3 over a canonical, type-tagged, length-delimited pre-image (maps
hash order-independently, arrays keep order, `int 5`/`"5"`/`5.0`/empty are distinct). 128 bits
because content addressing has no allocator to resolve a collision. `AppendValue`/`DecodeSeries`
are the reversible binary codec (WAL + interning). Signal-specific identity (metric name/unit/
temporality, profile type) folds into the pre-image as reserved labels at the signal layer.

## `symbols`

`[]byte → uint32` interning table over `pool.ByteIntMap` (no string conversion), CRC32C-framed
(`OTSY`). Names and typed-value encodings intern to small ids.

## `series`

`SeriesID ↔ Series`. `Add` is idempotent (the id *is* the identity hash) and interns key/value
bytes through the per-index symbol table. It also **deduplicates whole Resource and Scope sets by
content**, so series sharing a resource/scope point at one owned `[]KeyValue` rather than a
private clone of the structure that byte interning alone leaves behind. Point attributes are
near-unique per series, so they are byte-interned only.

## `postings`

The inverted index, keyed on **interned symbol ids** (`nameID → valueID → sorted []SeriesID`) — so
it is zero-alloc and **type-preserving** (the value id comes from the value's typed encoding).

- Lazy set-op iterators (`Intersect`/`Merge`/`Without`, galloping `Seek`), property-tested against
  a naive reference. `Merge` is a binary-min-heap k-way union (O(N·log k)), so a high-cardinality
  matcher resolving across thousands of value buckets stays fast.
- Matching is **callback-based**: `Select(nameID, func(valueID) bool)` hands the predicate a
  candidate id which the caller decodes to a typed `signal.Value` and tests. Storage imports no
  query-language operator; negation/equality compose from `Get`/`Without`/`WithoutName`.
- Sorting is lazy and caller-synchronized (`Sorted()`/`EnsureSorted()`), so a reader can upgrade to
  the write lock exactly once after a write.

## `bloom`

Token bloom filter (bit array, k xxh3-128 double-hash probes, versioned + CRC'd, **no false
negatives**), one per bloom-bearing record column. Modes: `FullText` (tokenized value),
`Attrs` (key-scoped `key‖value` and `key‖word` tokens), `Equality` (verbatim values — the
trace-by-id path).

Sizing is by **distinct** tokens, not occurrences (occurrences inflate the filter by the column's
repetition factor): a filter that would be small anyway keeps the cheap occurrence count, a larger
one is sized from a constant-space HyperLogLog estimate. FP targets differ per mode (`1e-6`
equality, `1e-2` full-text/attrs). The sidecar is self-describing (`k`, `m` encoded), so parts
written under any sizing stay readable.
