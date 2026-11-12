# Immutable file readers

The pool retains at most 256 raw file handles process-wide. Entries are keyed by immutable
owner, operation generation, filesystem-root identity and normalized object name. Its mutex
protects lookup/LRU/reservations only; filesystem open, read and close happen outside it.
A reserved cold open or a fully pinned pool declines retention without waiting, so a query
holding readers cannot deadlock while requesting another. Ranged fallbacks hold no long-lived
extra handles. An explicit opened reader has an independent cursor and an idempotent lease.

Engine fetch references pin their part's owner. Retiring/removing a part releases its scope
only after the last fetch; storage shutdown, read-only close, reset and adopted-part refresh
also release ownership. A scope cleanup releases forgotten owners without relying on it for
the descriptor bound. Pool registrations retain separately allocated counters, never the scope
or a query context. Fresh wrappers meter each operation with its own context.

An operation captures the scope's generation. File mutations announce before publication and
settle afterwards, rotating registered owners and rejecting new opens during publication.
Registrations survive descriptor eviction so repairing a column also invalidates any scoped
byte-cache entries. New registrations during publication cannot open the previous inode.
Old active operations may keep an existing opened generation; a cold/fallback read crossing
a mutation fails rather than using metadata and frames from different generations. Whole-object
and page-cache keys include the operation generation for backends supporting opened readers.

Descriptor exhaustion (`EMFILE`/`ENFILE`) permits one bounded idle-eviction retry:
current-generation handles with no actual reader lease may be evicted even while their
part is active. Actual opened leases remain protected. If a repair races a later reopen,
the generation fence reports a changed-object error instead of mixing metadata and bytes.

An operation-local Batch retains at most eight acquired entries or declined-retention results.
Ranged hits use the held entry under the batch's read lock, bypassing the global directory,
reference-count and LRU mutex. Reset releases the previous part's leases; Close waits for active
reads and makes subsequent batch reads fail. Batches retain no query context or scope owner.

A declined key is not retried until reset, avoiding repeated full-pool scans while the caller uses
its ordinary ranged fallback. Its scope generation is checked on every declined hit. Held leases
preserve existing opened-generation semantics across mutation; new generations use distinct keys.
Reservations still pass through the unchanged process-wide 256-handle budget. Filesystem reads and
closes never run under the global pool mutex; simultaneous readers in one batch share a local read
lock, while reset and cold-open setup use its local write lock.
