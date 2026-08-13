# `backend/` — the L1 storage seam

One interface over whole-object, slash-delimited keys: `Read`/`Write`/`List`/`Delete`/
`PutIfAbsent`, plus `IsEphemeral`. Absent keys return `backend.ErrNotExist`.
**`PutIfAbsent` is the CAS primitive** every atomic manifest/index commit builds on
(single-writer-wins, no Raft): guarded map insert (memory), exclusive `os.Link` (file),
`If-None-Match: *` (s3).

Implementations are **interchangeable** — `backend/backendtest.Run(t, factory)` is the shared
conformance suite all of them pass under `-race`.

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
- **`backend.Sizer`** — optional `Size(ctx,key)` for byte accounting without reading (used by
  `PartsDetailed`); `backend.SizeOf` falls back to a full read.
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
