# Engine state ownership

`Mutex` separates catalog ownership, writer ordering, and reader-visible state. It is not
reentrant. The lock order is catalog → writer → state; ingestion starts at writer.

| API | Contract |
|---|---|
| `Lock` / `Unlock` | Catalog and maintenance mutations; also excludes writers and readers. |
| `LockWriter` / `UnlockWriter` | Admission, apply, replica application and WAL serialization. |
| `TryLockWriter` | Tests writer ownership without waiting; success still waits for state exclusion. |
| `RLock` / `RUnlock` | Coherent read planning and state inspection. |
| `LockState` / `UnlockState` | Derived read caches only; cannot change admission or WAL state. |
| `WithReaders` | WAL I/O releases state exclusion while retaining writer ownership. |
| `WithWriters` | Catalog I/O releases writer/state ownership while retaining the catalog gate. |
| `EnableSharedWriters` | Installs an optional lifecycle barrier before concurrent use. |
| `RLockWriter` / `RUnlockWriter` | Pins a registry/head generation while independent series prepare. |
| `Publish` | Under a shared writer lease, installs one complete batch and advances revision under state exclusion. |

I/O callbacks mutate no engine state and acquire none of these locks. Both yield methods
restore ownership on errors and panics. Lazy postings sorting uses `LockState`, so a fetch
does not accidentally wait for WAL I/O merely because the index needs its first sort.

Outside this primitive, engine build slots precede the shared maintenance barrier. Reset,
repair and pruning take the barrier exclusively. Identity pruning also excludes public
reloads through the identity lifecycle gate. Catalog operations never wait for a build slot.

Every writer unlock advances a monotone read revision under state exclusion, including yielded
catalog stages. Derived `LockState` updates leave it unchanged. Saturation disables token reuse.

Standalone WAL-backed metric engines enable shared writers. Exclusive writer operations drain
all shared leases before acquiring writer/state ownership. Shared writers coordinate their own
series reservations and physical WAL ownership; they never upgrade a lease to exclusive ownership.
Queries still use only the state read lock. Per-batch publication and exclusive writer unlock
both advance the same revision; releasing a shared lease alone changes no reader-visible state.
