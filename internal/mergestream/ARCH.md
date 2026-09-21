# `internal/mergestream` — the seam two merge engines share

Both verticals merge immutable parts, but they do not merge the same way: the metric engine dedups
samples by timestamp with later-part-wins and then downsamples, over three fixed columns; the record
engine concatenates without dedup and re-sorts by timestamp, over `ts` plus n int and m byte columns
from a *runtime* schema, with a per-column dictionary carry. A generic row cursor over that costs an
indirect call per row, on a path where a branch per row is measurable.

So this package is a **seam, not a merge**. Each engine keeps its own driver; what lives here is
only what both would otherwise each invent, plus the conformance suite that holds them to the same
invariants.

## `Keys` — the ascending union of the sources' keys

Both merges visit every distinct series/stream of the selected parts in ascending order. Both used
to compute that by inserting every id of every part into a `map[SeriesID]struct{}` and sorting the
result: O(distinct keys) resident, one hash per row of the index, and a sort on top. Every part's id
list is *already* ascending — the metric engine's `partIndex` by construction, the record engine's
`part.ranges` because `buildRanges` sorts them — so the set was rebuilding order that was already
there.

`Keys` is a k-way heap over those lists. State is O(parts); nothing is hashed and nothing is sorted.
A reused `Keys` allocates nothing per traversal, so the union costs no heap at all inside a merge
(measured over 8 sources × 16384 keys: 16.08 MB and 1044 allocations down to 3 B and 0, and 25.8 ms
down to 2.3 ms).

Sources are read by index through the `Source` interface rather than as a materialized slice, which
is what lets the metric engine's *paged* index — fixed-width entries in the on-disk sidecar — feed a
merge without materializing its ids at all.

Repeats are collapsed, both across sources and *within* one: a part whose stream column arrived
unsorted leaves `buildRanges` with two runs of the same id, which the map used to swallow silently.

## `Budget` — the two units a merge seals on

A merge stops filling an output part on two numbers that are not interchangeable and neither of
which bounds the other:

- **disk** — what the writer has actually encoded, the part's size on the backend;
- **resident** — what the merge holds in RAM, which grows with *distinct keys* (id runs, series
  index, aggregate sidecar) rather than with encoded bytes, so a merge of very short series reaches
  it long before the disk cap.

Naming both here is the point: the record engine today has only one number, denominated in decoded
bytes — the resident unit — and the metric engine has both. Giving them one type keeps the record
engine from inventing a second unit of its own when it grows one, and makes the asymmetry visible at
the call site instead of buried in a helper's prose. `Budget` changes no threshold; it only names the
ones the engines already compute.

## `CheckForward` / `ErrNotForward`

Both merges consume each source part in sequence order: the output's keys ascend, so the row ranges
each part is asked for ascend too, and a forward-only column cursor can serve them by skipping
forward and never seeking back.

That holds only while the part's rows are grouped by key in key order. They are as written — but
`buildRanges` re-sorts the ranges of a part whose stream column arrived unsorted, and the ranges then
point backwards. `CheckForward` is where a streaming reader detects that and falls back to a reader
that can seek; a gap is fine (the cursor discards it), a step backwards is not.

## `mergestreamtest`

The part that actually pays. Four invariants are identical in both engines and neither tested them
the same way:

- the output row multiset is what a naive merge of the same sources would produce (each engine
  supplies that reference — they differ, and that difference is the point);
- no source object is read twice by one merge;
- a merge that fails at *any* of its writes commits no part and loses no row — checked at every
  write index, not at a representative one;
- `Keys` matches sort-and-dedup, under fuzz.

An engine instantiates it by adapting its own ingest/merge/read to `Store`. The suite's `Backend`
counts reads per key and injects the write failure; it embeds the `backend.Backend` *interface*, so
it implements neither `Viewer` nor `Sizer` and every read funnels through `Read` where it is seen.
