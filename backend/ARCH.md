# `backend/` — the L1 storage seam

One interface over whole-object, slash-delimited keys: `Read`/`Write`/`List`/`Delete`/
`PutIfAbsent`, plus `IsEphemeral`. Absent keys return `backend.ErrNotExist`.
**`PutIfAbsent` is the CAS primitive** every atomic manifest/index commit builds on
(single-writer-wins, no Raft): guarded map insert (memory), exclusive `os.Link` (file),
`If-None-Match: *` (s3).

Implementations are **interchangeable** — `backend/backendtest.Run(t, factory)` is the shared
conformance suite all of them pass under `-race`.

**`backend/faultbackend`** is the fault-injection wrapper for tests: rules match an operation by
kind and key and either fail it, rewrite the bytes a read returns, or run a hook before it. The
rewrite models the failure an error cannot — a store handing back data that is not what was written,
and saying nothing. The hook is the point of the package — a `Gate` suspends the matching operation
*inside* the backend until the test releases it, so a test states a distributed interleaving instead
of racing for one with sleeps, and the code under test needs no seams of its own. It forwards none
of the optional capabilities below: each has a mandatory fallback, so a wrapped backend runs the
same code, only slower.

The tests it drives that describe an *unfixed* defect are gated by `internal/reproduce`: they skip
unless `OTELDB_STORAGE_REPRODUCE=1`, so CI stays green while the defect stays one command away from
a demonstration.

- **`backend.Memory()`** — ephemeral reference backend; copies on both read and write so stored
  objects never alias a caller's buffer. The default in tests.
- **`backend/file`** — directory tree with a `..` traversal guard; atomic write via temp+fsync+
  rename, `PutIfAbsent` via temp + `os.Link`. **The key prefix bounds the traversal, not just the
  result**: `List` walks only the prefix's subtree, and `Delete` rmdirs the directories its object
  leaves empty (`New` sweeps pre-existing ones once). Otherwise a listing costs a full-tree walk
  whose size grows with parts *ever created* — maintenance lists per tenant/signal every tick.
- **`backend/s3`** — store-specific calls sit behind a small `ObjectStore` interface so the
  contract logic (root prefixing, sorted listing, 404→`ErrNotExist`, conditional put, idempotent
  delete) is testable over a fake. `NewAWS` adapts aws-sdk-go-v2 — **the only package importing
  the AWS SDK**. An always-on integration test runs the suite over a real S3 protocol server
  (embeddable `go-faster/fs` on `httptest`, no Docker).
- **`backend.Cached(inner, maxBytes)`** — byte-bounded read cache for the cold tier. Correct by
  construction: part objects are write-once immutable, so a hit is never stale; a write/delete of
  the same key updates/drops the entry. Wrapped **outermost** (a hit skips metering and the
  backend), skipped for ephemeral backends. It is otter's weight-bounded **loading** cache, not a
  strict LRU: concurrent misses on one key collapse into a single inner read (one object-store GET
  per cold object, not one per in-flight query), and W-TinyLFU admission keeps a cold historical
  scan from flushing the hot working set — the pattern a strict LRU handles worst. An object
  larger than the whole budget is never retained.
- **`backend.WriteUncached` / `backend.ReadUncached`** — the escape hatch from that cache for the
  few *mutable* objects rewritten far more often than they are read: the engines' identity sets,
  written when identity changes and read only on recovery. Caching one is pure eviction pressure (at
  real cardinality a single identity object is a large fraction of the budget), and it is the one
  object class where the write-once-immutable premise above does not hold. `WriteUncached` still
  invalidates the key, so a reader never sees a superseded value.
- **`backend.Viewer`** — opt-in `ReadView(ctx,key)` returning a **read-only view** instead of a
  copy (a stored value is never mutated in place, so a view survives overwrite/eviction). This
  removes the clone-per-hit that dominated the query-path allocation profile; `backend.ReadView`
  falls back to `Read` on backends without it, and `block.PartReader` reads through it.
- **`backend/bucketindex`** — compact versioned index (part list + per-part time bounds + the WAL
  flushed-epoch watermark) in one object, so a stateless reader enumerates and time-prunes a
  tenant's parts without a full `List`. Fuzzed + golden-tested.
- **`backend.ReaderAt`** — optional `ReadAt(ctx,key,off,n)`, the read counterpart of
  `ObjectCreator` and the reason a query touching a few granules no longer pays for the whole
  column (`block/ARCH.md`, "Reading a column by range"). The range is **clamped to the object's
  end**, so a reader takes a trailer without first learning the size — one round trip, not two, and
  a short result is not an error. `file` uses `pread`, `s3` a `Range` header (via the optional
  `RangeObjectStore`, so a fake need not), `Memory` a copy of the range rather than of the object.
  `backend.ReadAt` falls back to reading the object and slicing, so callers range unconditionally
  and a wrapper that forgets to forward costs bytes rather than correctness. The read cache serves a
  range from a resident whole-object entry and otherwise forwards **without caching** — going
  through the loading path would fetch the whole object to answer a range, which is the cost being
  avoided.
- **`backend.Sizer`** — optional `Size(ctx,key)` for byte accounting without reading (used by
  `PartsDetailed`); `backend.SizeOf` falls back to a full read. **That fallback is a trap on the
  ranged read path**: a wrapper that hides `Sizer` would make opening a column read the whole
  column to learn its length, silently undoing the ranging. The manifest records each column's size
  (`flagBytes`) so the common path never asks.
- **`backend.ObjectCreator`** — optional `CreateObject(ctx,key) → ObjectWriter`, an object built
  incrementally: `Write` appends, `Commit` publishes atomically (nothing is visible under the key
  before it), `Abort` discards and is a no-op after a commit. `file` implements it with the same
  temp+fsync+rename it writes whole objects with, so the bytes reach the filesystem as they are
  produced; `Memory` and `s3` do not, and `backend.CreateObject` buffers into a single `Write` for
  them, so callers stream unconditionally. Several writers may target one key — that is how a
  part's rival codecs race, only the winner committing. It exists for the one object class far
  larger than the writer wants resident: a merged part's column (`block/ARCH.md`, "Two writers").
  `backend.StreamsWrites(b)` answers whether the fallback would be taken, which is a *sizing*
  question — the merge cap stops pricing part size against memory when it would not be
  (`engine/ARCH.md`). **A wrapper must forward it**, and must not claim it over an inner backend
  that lacks it: `Cached` returns a second type that carries `CreateObject` only in that case,
  rather than a method that would always answer yes.
- **`backend.NodeLocal`** — optional `IsNodeLocal()`, implemented by `Memory` (a process heap) and
  `file` (a directory tree), not by object stores. It reports the *medium*, and is exact only in the
  false direction: a `file` root on a network mount is a shared store and still answers true. Its
  one consumer is the `Open` diagnostic for a cluster node whose backend looks private while
  `cluster.Config.PrivateBackend` is unset — the configuration in which no flushed part ever
  replicates and the gap reads as real absence. Being a heuristic is why that is a warning and not a
  refusal. **A wrapper must forward it** (`Cached` does), or a wrapped local disk looks shared and
  the diagnostic goes quiet on exactly the deployment it is for.
- **`backend.SpaceReporter`** — optional `FreeSpace(ctx)`, implemented by `file` (statfs, and
  reporting the *unprivileged* figure so the root reserve is never counted as usable). The merge
  engine sizes its output parts against it instead of a constant, so part size tracks the disk the
  data lands on. `backend.FreeSpace` returns `ErrSpaceUnknown` for a backend without it — `Memory`
  and object stores, where local free space has no meaning — and the caller then falls back to its
  configured ceiling. **A wrapper must forward it**: `cachedBackend` does, or every cached backend
  would silently lose the capability.

## Stateless read path

A fresh process serves what a previous one flushed, from the backend alone:
`Engine.LoadParts` rebuilds the part set (bucket index) and the identity index (durable series
object); `Storage.Open` → `recover` discovers tenants by their bucket-index objects and loads each
(no-op when ephemeral). The **unflushed head** comes from the WAL instead — see
[`../wal/ARCH.md`](../wal/ARCH.md).
