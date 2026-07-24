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
  rename, `PutIfAbsent` via temp + `os.Link`.
- **`backend/s3`** — store-specific calls sit behind a small `ObjectStore` interface so the
  contract logic (root prefixing, sorted listing, 404→`ErrNotExist`, conditional put, idempotent
  delete) is testable over a fake. `NewAWS` adapts aws-sdk-go-v2 — **the only package importing
  the AWS SDK**. An always-on integration test runs the suite over a real S3 protocol server
  (embeddable `go-faster/fs` on `httptest`, no Docker).
- **`backend.Cached(inner, maxBytes)`** — byte-bounded LRU read cache for the cold tier. Correct by
  construction: part objects are write-once immutable, so a hit is never stale; a write/delete of
  the same key updates/drops the entry. Wrapped **outermost** (a hit skips metering and the
  backend), skipped for ephemeral backends.
- **`backend.Viewer`** — opt-in `ReadView(ctx,key)` returning a **read-only view** instead of a
  copy (a stored value is never mutated in place, so a view survives overwrite/eviction). This
  removes the clone-per-hit that dominated the query-path allocation profile; `backend.ReadView`
  falls back to `Read` on backends without it, and `block.PartReader` reads through it.
- **`backend/bucketindex`** — compact versioned index (part list + per-part time bounds + the WAL
  flushed-epoch watermark) in one object, so a stateless reader enumerates and time-prunes a
  tenant's parts without a full `List`. Fuzzed + golden-tested.
- **`backend.Sizer`** — optional `Size(ctx,key)` for byte accounting without reading (used by
  `PartsDetailed`); `backend.SizeOf` falls back to a full read.

## Stateless read path

A fresh process serves what a previous one flushed, from the backend alone:
`Engine.LoadParts` rebuilds the part set (bucket index) and the identity index (durable series
object); `Storage.Open` → `recover` discovers tenants by their bucket-index objects and loads each
(no-op when ephemeral). The **unflushed head** comes from the WAL instead — see
[`../wal/ARCH.md`](../wal/ARCH.md).
