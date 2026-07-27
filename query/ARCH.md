# `query/` — the read seam (L4) and what sits just above it

The library **stops at the fetch contract**. Query languages and planners are the embedder's.

## `fetch` — the dual-shape contract

`Request{Tenant, Signal, Start, End, Matchers, Conditions, AllConditions, Projection, SecondPass,
Limit, Reverse, Recycle}` carries two **operator-free** predicate families:

- **Matchers** — `Matcher{Name, Match func(signal.Value) bool}` resolve **identity** over postings
  (a metric series, a log stream).
- **Conditions** — `Condition{Column, Match, Tokens, Equal}` filter the **per-record columns**
  within that identity.

Neither is an operator enum, so equality/regex/negation and condition extraction live in the
language layer and storage stays operator-free. `Fetch` returns an `Iterator` of
`*Batch{ID, Series, Timestamps, Values, Columns, ScaleFactors}` — metrics populate `Values`,
record signals the named `Columns` (`Projection` narrows, `SecondPass` post-filters); the other
signal's fields stay zero-valued.

Optional capabilities, discovered by walking the `Unwraper` decorator chain:

- **`Limit`/`Reverse`** — ordered top-N. Filtering runs first, the limit selects over survivors.
  The result is a deliberate **superset** (rows tying at the boundary timestamp are kept), so the
  caller's own exact ordering never loses a boundary row. Honored by the record engine; the metric
  engine ignores it (PromQL needs every sample).
- **`Counter`/`GroupCounter`** — count-shaped reads answered without materializing samples or
  labels: a lightweight existence plan over the live buffers plus, for flushed data, a sorted
  intersection against the part index (a fully-covered part decodes **nothing**; only window-edge
  parts decode, and only their timestamp column). `CountBy` groups by a label's canonical text over
  the same flattened key space postings sees.
- **`Recycle` + `Batch.Release`** — opt-in buffer reuse through a shared release hook.
  Pass-through decorators forward it; a decorator that retains or clones a batch emits hookless
  copies and releases its inputs.

The iterator is **streaming by contract**: a batch is produced per `Next`, so a consumer that folds
and releases stays O(1) in matched series. The flip side is that the producer's resources live until
`Close` — the metric engine's iterator pins its parts and holds its decode-memory reservation for the
whole iteration — so **every caller must Close** (`Drain` does it for you, at the cost of
materializing the result set it was meant to avoid).

`Merge(fetchers...)` is the fan-out combinator (union by series id, timestamp-ordered, later child
wins a duplicate) backing multi-tenant and cluster reads; `MergeBatches` is its batch-level form (a
materializing merge over already-drained groups, used by `SplitFetcher`). Children are opened
concurrently under a bound into per-index slots — child order decides the duplicate-timestamp winner
— then merged **lazily**: a k-way merge holds one pending batch per child and emits the smallest id,
so peak resident is O(children), not O(children × series). The ordering is a **min-heap** keyed on
`(pending id, child index)` — O(log children) a step, since a cross-tenant or wide-shard fan-out has
one child per engine — and the index tie-break is what preserves the later-child-wins rule. It relies
on each child yielding ascending series ids — every producer here does (postings resolution is
sorted, and `MergeBatches` sorts too), and a child that breaks it fails the iteration with an error
rather than being silently mis-merged.
A series only one child carries is passed through untouched, hook and columns intact; a federated one
is cloned and its contributors released.

## `scale` — scale-out decorators

The part of an L5 query frontend expressible **purely over the contract**, so an embedder composes
it without the library owning a language:

- **`SplitFetcher`** splits a window into sub-windows **aligned to multiples of Interval** — grid
  alignment (not request-relative) is what makes overlapping queries share sub-windows — fetches
  them concurrently and merges. A narrow window is a transparent pass-through.
- **`CacheFetcher`** memoizes only **fully-pushable** requests: every matcher must carry a
  serializable equality `Spec`, so the key (tenant ‖ window ‖ sorted specs) is exact and a hit can
  never drop a matching series. An opaque matcher bypasses. There is no invalidation, so a
  **`Freshness`** guard keeps the recent window uncached.

Nested (`Split` over `Cache`), settled sub-windows cache while the most recent is always re-fetched
— the standard query-frontend behavior.

## `promql` — optional adapter

The library implements **no** query language. This package bridges the fetch seam to the Prometheus
`storage.Queryable` for embedders using the Prometheus engine; it contains **no engine** and is the
**only package importing prometheus** (importing it is opt-in).

- **Matcher lowering — condition extraction lives here, never in storage.** Only **index-safe**
  matchers (those that do not match the empty string) are pushed down; a negated/absent matcher
  would wrongly drop series lacking the label via postings, so every fetched series is
  **re-checked against the full matcher set** (absent label = empty string).
- **Label projection.** `signal.Series` → `labels.Labels`: attributes flatten, scope name/version
  under `otel.scope.*`, internal reserved labels hidden, `__name__` kept. It is a pure function of
  the content-addressed id, so a `Queryable` memoizes it per id.
- **Zero-copy samples.** Series iterators read the batch's slices directly (ns→ms on the fly), no
  per-sample copy or interface boxing; Select sets `Recycle` and `querier.Close` releases.
- `PushableMatchers`/`MatchesAll`/`PromLabels` are exported as the single source of truth for the
  Prom↔storage projection, so an embedder building its own pushdown reuses them.

The embedder owns evaluation and result types — the library defines no query-result type.

## `profile` — EXPLAIN ANALYZE

Opt-in per query (`profile.WithCollector(ctx)`; nil collector ⇒ no-op, the default). Operators push
timed nodes onto a concurrency-safe tree. **Distributed:** the read RPC sets a header when a
collector is active, the peer runs under its own collector and prepends its encoded tree
(bounds-checked, fuzzed) to the response, which the requester grafts as a `remote {addr}` subtree.
