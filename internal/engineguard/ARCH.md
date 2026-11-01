# Engine state ownership

`Mutex` separates catalog ownership, writer ordering, and reader-visible state. It is not
reentrant. The lock order is catalog → writer → state; ingestion starts at writer.

| API | Contract |
|---|---|
| `Lock` / `Unlock` | Catalog and maintenance mutations; also excludes writers and readers. |
| `TryLock` | Attempts catalog and writer ownership without waiting for those gates. |
| `LockWriter` / `UnlockWriter` | Admission, apply, replica application and WAL serialization. |
| `TryLockWriter` | Tests writer ownership without waiting; success still waits for state exclusion. |
| `RLock` / `RUnlock` | Coherent read planning and state inspection. |
| `LockState` / `UnlockState` | Derived read caches only; cannot change admission or WAL state. |
| `WithReaders` | WAL I/O releases state exclusion while retaining writer ownership. |
| `WithWriters` | Catalog I/O releases writer/state ownership while retaining the catalog gate. |

Standalone durable metrics enable an additional writer-lifecycle gate before concurrent use.
The order becomes catalog → writer lifecycle → writer → state. Warm batches hold only a shared
lifecycle lease while preparing, logging and publishing immutable runs. Exclusive writer operations
drain those leases; query state readers never join this gate. `WithReaders` retains exclusive
lifecycle ownership during WAL seal/sync, and `WithWriters` releases it during catalog I/O.

I/O callbacks mutate no engine state and acquire none of these locks. Both yield methods
restore ownership on errors and panics. Lazy postings sorting uses `LockState`, so a fetch
does not accidentally wait for WAL I/O merely because the index needs its first sort.

Outside this primitive, engine build slots precede the shared maintenance barrier. Reset,
repair and pruning take the barrier exclusively. Identity pruning also excludes public
reloads through the identity lifecycle gate. Catalog operations never wait for a build slot.
