# `engine/` — the metrics vertical

One `Engine` per tenant (or metric shard) ties index, parts and WAL into an ingest+query path. Its
twin for logs/traces/profiles is [`../recordengine/ARCH.md`](../recordengine/ARCH.md), which shares
the locking discipline below.

## Locking discipline (shared with `recordengine`)

**Invariant: the engine lock is never held across object-store I/O.** Parts are immutable, so every
phase is `plan under lock → I/O off lock → publish under lock`.

| element | rule |
|---|---|
| `parts` slice | copy-on-write; a snapshot stays valid after unlock |
| published part | never written: a snapshot's parts are read lock-free (see "A published part is never written") |
| fetch | plans under RLock (matchers, `acquire()`, head seed), then reads lock-free |
| flush/merge | parts swap, index commit and WAL checkpoint publish atomically |
| writers | only the maintenance loop mutates `parts` |
| retired parts | refcounted, reclaimed deferred, so a fetch never races a delete |
| head detach | detached buffers stay readable via `flushing` until the part lands |

Publishing atomically keeps the durable watermark in step with part discoverability: exactly-once
crash consistency.

**Invariant:** a record is visible in exactly one of head / `flushing` / part. Never neither
(visibility gap), never both (double count).

**A flush that fails before publishing unwinds** (`abortFlush`): every pre-publish error path — the
WAL seal, the part write, the read-back — folds the detached buffers back into the head and clears
`flushing` in one critical section. Leaving them in `flushing` would strand them: the next flush
overwrites it, and its checkpoint then discards the WAL segments that were the samples' last copy.
The fold merges — appends kept landing in the fresh buffers during the flush, and the detached
samples precede them — and restores the byte measure and `head.since`, so the retry is scheduled
from when the data actually started waiting. Past the publish there is nothing to unwind: the parts
are already in the live set and the samples are durable.

## Head

The index (`symbols`+`series`+`postings`) plus per-series `(ts, value)` buffers.

**`OOOWindow` is per-series** (`head.seriesNewest`): a sample more than `OOOWindow` behind *that
series'* newest admitted sample is rejected, so a fast or clock-skewed series cannot shed the slower
ones sharing the head, and a first sample is never late. Watermarks outlive a flush.

**The series index outlives a flush** — only sample buffers drain, so flushed series stay queryable
and re-appends do not re-index.

**`AppendBatch`** takes a precomputed `SeriesID`, columns, and a `materialize` callback used only on
first sight, under one lock. `appendByID` does one map probe: a present buffer means the series is
known, so no `signal.Series` is built or hashed. WAL frames group by series, one write a batch.

**A logged write decides, logs, then applies.** With a WAL, `Append` and `AppendBatch` —
and `ApplyPrimary` always — stage every sample in `walBatch` — admitted exactly as `appendByID` would admit it had
the samples before it been applied — log the staged batch as a single WAL write, and only then apply
it. Applying first left a failed log write's samples in the head, and worse for a series seen for the
first time: its buffer already existed, so the retry skipped the identity record and logged samples
that replay drops, losing an acknowledged write at the next crash. One write per batch is what makes
the log side all-or-nothing: a failed write lands nothing a retry would log again (a part-way failure
is healed by the WAL itself, `../wal/ARCH.md`). An engine with no WAL keeps the one-pass `appendByID`,
which nothing can interrupt; `FuzzStagedAdmissionMatchesDirect` pins the two to the same outcome and
the same head. The head's age starts at the first staged sample, not when the log write returns, so a
slow disk does not delay an age-triggered flush.

## Flush

Drains the head into one flat part, one row per sample:

```
{tenant}/metrics/{partid}/
  columns    [series:int128, ts:int64, value:float64]   sorted by (series, ts)
  sidx       series index sidecar
  stats      aggregate stats sidecar        (optional, Config.AggregateStats)
  identity   this part's series identities  (format in index/identity)
bucket index (part list + time bounds)      ← written last: the commit point
```

Merge writes the same shape, committing the new part set before deleting sources.

### Identity is part-scoped

Each part carries its own series' identities, written and deleted with its other objects. Three
properties follow, and they are what a whole-set identity object cannot give:

| property | consequence |
|---|---|
| retention self-cleans | dropping a part drops the identities naming its rows |
| a flush persists only what it wrote | 88 B to add one series to a 20k-series tenant |
| every node derives its own live set | a replica prunes its own parts, no ownership rule |

A whole-set object has to be re-serialized in full on any change, and costs ~218 B/series by repeating
the label bytes, against ~40 B/series interned here. A prefix carrying one (`series.bin`) is read at
open and deleted once every live part carries its own identities, so recovery cannot resurrect
identities whose data is gone. Identity is read on recovery and by a replica adopting a part, never by
a query.

**The WAL carries its own identities.** A flush checkpoints it, discarding the series records written
when identities were first seen, so a record is logged whenever a series starts a **new sample
buffer** — once per actively-appending series per flush window, not only when its identity is new.
Otherwise a sample logged after a checkpoint names a series the log no longer describes, and identity
now lives in parts retention can drop at any time.

**Metered apart.** The resident half — symbol table, series index, postings, OOO watermarks — is
`Stats.IdentityBytes`, not `HeadBytes`: a flush drains samples, not identities, so folding them would
have the size-triggered flush chase a number it cannot lower.

**One wall clock.** `head.since` is stamped when the head takes its first bytes after a flush and
cleared by `head.detach`, giving `Stats.HeadAge` — how long the buffered data has been waiting. It
is wall time and not the timestamps in the buffers: backfill puts arbitrarily old data in a
brand-new head, so data time cannot say how long a flush has been stalled.

### Publish order: the part's objects first, the bucket index last

The bucket index makes a part durably visible, so writing it is the commit point and a readable part
always carries the identities its rows resolve through. Every part object, sidecars included, is
written deferred and the part is made durable by one `backend.SyncPrefix` after its last sidecar
(`backend/ARCH.md`, `DeferredSyncer`), so the index never names a part a power cut could still take. A crash in between leaves an orphan — objects
and identity together — swept by a later open, stranding nothing.

`CheckpointThrough` runs last and is the WAL's commit point, so replay recovers a part that failed
to publish. It discards only through the sequence the flush sealed at detach, leaving the segments
that hold samples appended during the part write (`wal/ARCH.md`, "Epochs"). The index also carries the flush watermark (as `recordengine` does) and replay skips
segments at or below it: a checkpoint only reaches segments this node wrote, and it can miss them —
a node that stops being the shard's compaction owner stops checkpointing while its parts keep
arriving — so the watermark is what keeps recovery exactly-once.

**The watermark is per writer.** It counts *this* node's flushes and indexes *this* node's WAL
segments, while the index holding it is shared by every replica of the shard, so it is stored in a
slot keyed by `Config.WriterID` and each node recovers only its own. A commit that rebases onto a
rival's index carries the rival's slots through untouched and stamps only its own; stamping one
number over another node's is data loss in one direction and duplicate replay in the other
(`wal/ARCH.md`, "Epochs").

### Adopted parts

**Invariant: the index an engine commits and the part set it serves are the same set.** A commit
that loses the conditional write rebases — it carries the winner's entries forward so its retry
does not drop them — and it opens them, identities included, in the same step. Publishing an index
that names a part this engine will not answer for is a query silently missing rows until the next
`LoadParts`.

They are held apart from `parts`, in `foreignParts`, because the two sets differ in *ownership*, not
in readability: an adopted part is not this engine's to merge, remove or delete, and counting it as
its own would let the next commit's diff read the rival's later removal as a part this engine lost —
or resurrect one it had removed. So `parts` is what flush/merge/retention operate on, `foreignParts`
is added to it for reads (`Parts`, fetch, the identity prune's live set), and `LoadParts` collapses
the two: everything the index names is opened as this engine's view of the prefix.

The opens sit on the commit path, which is where the cost is. It is bounded: handles are reused
across the retry loop, and an entry that cannot be opened — a rival merged it away in between — is
left out of the readable set but kept in the index, since only its writer knows whether it is live.

### Block identity is allocated by the commit that publishes the part

A flush output commits `{n}` at level 0, `n` from `bucketindex.Index.NextBlock`. A merge that writes
**one** part commits the union of the block sets its inputs covered, at `max(input level) + 1`, and
allocates nothing: the merged part covers exactly the blocks its inputs covered — a set, so a run
that straddles a gap claims no block inside it — and that is what makes `Entry.Supersedes`, and so a
repair that terminates on a successor, decidable from identity alone (`backend/ARCH.md`, "Part
identity is a block *set* plus a level").

A merge that **splits** its output allocates a fresh contiguous run instead, one block per fragment,
and hands every fragment the same `bucketindex.Claim`: the consumed set as `Blocks`, the run as
`Group`. No fragment supersedes an input on its own — it holds a fraction of one — but the group
jointly covers what the inputs did, and a later merge that consumes the whole group folds the claim
back into an ordinary interval, which every merge after that inherits. Nothing is severed. The run is
allocated **as a whole, before any fragment is numbered**, so each fragment can name its siblings;
that is what lets a repair fetch a group member by member.

One limit is deliberate: an output carries **at most one unrealized claim**, the widest. A merge
consuming fragments of two groups without completing either keeps the lineage of one, and the other
falls back to exact-prefix matching — where every split left it before claims existed, so never
worse than the status quo, and rare enough not to justify a list per entry.

Inputs that all predate format v5 carry no interval to inherit; a fresh `{n}` is how such a part
migrates, a merge being the only thing that rewrites it, and that output supersedes nothing. A
**mixed** merge inherits the union of the inputs that do carry an interval and allocates nothing on
top: a want naming a pre-v5 part records that part's unset interval, so no claim the output could
make would contain it, and a fresh block would name blocks the output does not cover.

**Allocation runs once per CAS attempt, inside `nextIndexLocked`, and is applied only after the
commit lands.** `NextBlock` is taken over the state that attempt publishes — the persisted
high-water mark, the entries a rebase adopted from a rival writer included (its blocks are real
claims), this engine's own parts, its holes, and every want outstanding or pending, whose part may
yet be repaired back in. The attempt's own allocations raise the mark it commits, so the mark only
ever rises and the commit that retires the top-numbered parts cannot renumber over them. A number
written onto the part before its CAS succeeds survives the rebase, so the retry would keep a block
the winner just took and never re-allocate; the assignments are therefore held in `pendingBlocks`
and written through in the `err == nil` branch, the same discipline `pendingWants` and
`pendingHoles` follow, and for the same reason.

## Identity prune

`PruneIdentities` drops the identities retention leaves behind, which otherwise accumulate for the
process' lifetime under series churn.

| | |
|---|---|
| live set | the live parts' id sets ∪ head ∪ mid-flush detachment ∪ recent |
| when | after a merge, or a replica's refresh — and only if parts went away |
| ownership | none needed: the live set is what this node can still serve |

Each `sidx` already lists its part's ids, so no new on-disk format. Identities die no other way, so an
engine whose data only grew skips even the live-set walk.

Symbol ids are dense and referenced by the postings, so nothing is removed in place. Survivors are
**rebuilt** off-lock against `series.Index.Snapshot()` — the append-only entry log, immutable without
the lock because registration only extends it — then swapped in under the lock.

| step | 200k identities | 1M |
|---|---|---|
| rebuild, off-lock, far too long to hold `e.mu` | ~0.6 s | ~3.4 s |
| swap, under lock, allocation-free | ~5 ms per 200k pruned | ~40 ms |

**Two sets must survive a rebuild that decided without them:** entries registered past the snapshot's
end, and dead-found series that **regained samples** — not re-registered on a new sample, so leaving no
log entry, and dropping them would strand samples the next flush writes into a part with no identity
naming them. A dead series' OOO watermark is deleted by key, so cost tracks what died, not cardinality;
a live one keeps its own, or a late sample would be re-admitted.

The WAL needs no `walExpiries` equivalent: checkpointed every flush and re-logging a series record when
a series starts a buffer, its live records name only series it describes. The prune writes nothing
durable. `recordengine/prune.go` is the same prune over resident `part.ranges`; **known gap there:**
merged symbol sidecars stay unbounded.

## Lifecycle and part identity

Driven by the facade's single maintenance loop, plus a head-bytes trigger flushing only the
over-threshold engines. `Engine` is exported and `Close`/`Reset` callable from anywhere, so
concurrency is enforced, not assumed: flush and merge hold one `flushMu` across their whole body.
`Reset` takes it too, draining an in-flight flush/merge that would otherwise publish its part into the
emptied engine, dropping detached buffers with the head, and **retiring** live parts rather than
deleting their objects, so a fetch holding one is not read out from under it.

**Invariant: a part id is globally unique, and names one part's content forever.** `{partid}`
(`internal/partid`) is a ULID-shaped 128-bit id — 48-bit unix-millisecond timestamp, 80 random bits,
Crockford base32 — minted per part, never derived from what the node holds. A counter would be unique
only under a single-writer-per-prefix discipline that nothing enforces: over a shared object store
(`cluster.Config.PrivateBackend` false) every replica of a shard writes one bucket under one prefix,
so an ownership handoff, a restore from a stale index, or a rejoin after a lease loss would have two
writers minting the same key for different content — a silent overwrite there, and a permanently
diverged part under `cluster/partsync`, which treats the key as the part's identity.

The id's textual form sorts in creation order (the timestamp leads, and the entropy is incremented
rather than redrawn within a millisecond), so part prefixes still order lexicographically the way the
old zero-padded counter did — `cluster/partsync` ranks pre-generation indexes by their highest part
prefix on that basis.

Uniqueness also makes part prefixes append-only for free: a failed attempt burns its id rather than
handing it to the retry, which matters because a rewrite replaces only the objects it produces, so a
part reusing a prefix would inherit leftovers it never wrote.

`LoadParts` sweeps that residue — a failed attempt's objects, or a retired part whose reclaim delete
failed. It lists the prefix and deletes every object under a part directory the bucket index does not
name; a directory is a part's when its name parses as a part id. A replica's `RefreshReplica` skips
it: the owner's in-flight part is not in the index yet.

**An unnamed part is not yet an orphan.** A flush or merge writes the part's objects before the
commit that names them, so over a shared store another writer's in-flight part looks exactly like
residue — and `LoadPartsUnclaimed`, which every clustered node runs at startup before it holds any
claim, would delete it, leaving that writer to commit an index naming a part with no objects. The
sweep therefore deletes a part only once its id is older than `Config.OrphanGrace`
(`partid.ID.Settled`): the id carries its mint time, so no writer-side bookkeeping is needed. A part
dated after this node's clock was minted by a writer running ahead and is never swept. The grace has
to outlast the longest write-to-commit window plus the clock skew between writers. The long window is
a merge: a streaming merge mints its first output id while it is still reading its inputs and commits
after its last output, so it spans up to 16 GiB read and 16 GiB written at the default
`MergeCeilingBytes`. The
one-hour default covers that above ~10 MiB/s of sustained throughput. What it costs is reclamation
latency: a younger orphan survives the load, is counted in `storage.parts.orphans_deferred`, and
goes at the first load after it ages. The guard applies to private backends too. There only this
node writes the prefix, but a sweep that behaved differently by backend kind would be one more mode
to get wrong, and on a single node the cost is only that latency. It does not cover a part
`cluster/partsync` is copying in: the copy keeps the peer's id, which may be old. A sweep that races
such a copy leaves an index naming a part this node cannot open, which is the want path's to repair
from the peer that still holds it rather than a silent loss.

`LoadPartsReadOnly` is that same sweep-nothing load reached deliberately rather than through the
replica path. It exists because the sweep is the one backend mutation an open performs before the
caller has any say, so without it a store could not be opened for reading only — a backup or a
verifier reclaimed objects from the directory it was pointed at. It backs `storage.WithReadOnly`,
which then keeps the guarantee at the facade for the life of the handle.


## A failed load changes nothing, and fences every commit

A load is all-or-nothing. `readIndexLocked` reads the index and opens every part it names into
locals, touching nothing on the engine, and `adoptLoadLocked` — which cannot fail — installs the
whole set at once: the CAS token, the parts, `foreign`, holes, wants, epochs, generation, removals,
`indexed`. Adopting any of them early is the failure this orders against: a load that takes the new
index version and then fails on a part leaves the engine with the new token and the old part set,
and its next commit passes the compare-and-swap and drops every entry it never opened.

A load that fails before adopting also **fences** the engine (`loadErr`): `updateIndexLocked`, the
single commit path, refuses with `bucketindex.ErrFenced`, so flush, merge, retention drops, repair,
the owner-load want commit, a rebase and `CompactNow` are all covered by one check. Keeping the old
state is not enough on its own: committing on a view that is known to be stale can still race a
peer's backfill. Flush and merge also check before doing their I/O, and flush checks again under
the lock that publishes its part, so a flush fenced mid-write folds its records back into the head
rather than publishing a part the next reload would not keep. Without a cluster, reads keep serving
the old part set, the best answer with no owner to fail over to; a cluster node disclaims the whole
shard instead (`cluster/ARCH.md`), since what its stale set lacks is unknown.

The fence lifts only when a load succeeds. An owner reloads only after a backfill, so the facade's
maintenance calls `ReloadFenced` every cycle, before the flush; a replica's `RefreshReplica` is
already a load each cycle. `ReloadFenced` loads without the replica's head trim, since an owner's
head holds records no part has. Until then the fence is surfaced as `Stats.IndexFenced`, the failing
load's error as `Stats.IndexLoadErr`, and every attempt in `index.fenced_loads` by reason
(`ADMIN.md`). A part that stays corrupt is the one failure a load may stop failing on
(below): left fenced, one damaged object would stop flush, merge and retention for good.

A reload keeps a flushed part whose commit failed (identity still pending, named by no index):
its records already left the head, so dropping the handle would make them unreadable until a
restart replays the WAL. The next commit publishes it, and the flush watermark does not fall below
the flush that wrote it.


## A part the owner cannot read becomes a want, not a removal

A part `LoadParts` cannot open must not fail the whole engine — that turns one lost part into a node
that will not start. It drops the part from `Entries` and records a
`bucketindex.Want` naming it (`backend/ARCH.md`, "`Entries → Removed | Wanted`"), in **one**
compare-and-swap: the drop and the obligation are the same commit, so no crash can land the drop
without the want and a lost race leaves neither — the retry re-reads and re-derives both from the
same evidence.

**A want can also arrive from outside.** `AdoptWants` takes obligations the engine cannot derive
from its own index — parts a peer holds that this index never named, which it therefore could never
report losing (`cluster/ARCH.md`, partsync). They behave like a discovered want from there on, with
one difference: they survive a load, since nothing in the index implies them and a reload would drop
them before any commit could publish them. They are stated at this engine's generation, not the
discovering peer's, and one naming a part already indexed is dropped.

Two things consume them. Repair fetches the missing part back (or acknowledges the loss as a hole),
and the **read policy** refuses to answer over one: `WantOverlaps` reports whether an outstanding
want covers a query window, which is what the read seam disclaims on — a cluster node's and a
single-node store's alike (`cluster/ARCH.md`).
Pending wants count — a want a load discovered but no commit has published yet still names data that
is already unreadable — while a hole does not, since acknowledging a loss discharges its want and
lets reads resume.

**Only `backend.ErrNotExist` becomes a want at once.** Every other failure — a timeout, a canceled
context, a full disk, a denied request, a throttled bucket — leaves the part's existence unknown,
and a want is a statement that it is gone. Recording one on an unknown would drop a live part from
the index and start a repair for data that was never missing; over a shared backend a single
transient fault touches every part at once, so the index would be stripped wholesale. So a
non-absence error fails the load exactly as it did before (`partGone`, the counterpart of
ClickHouse's `isRetryableException` rethrow in `checkDataPart`).

The trigger stays narrow in the other direction too: a part whose objects are **present but
unreadable** — a corrupt manifest, a truncated column — is not a want on the load that meets it.
That load fails and fences the engine: one bad read of an intact object must not start a repair,
and widening the trigger is how a repair path turns into a data-destruction path.

**Corruption that persists is a want.** After `corruptLoadsBeforeWant` (3) consecutive loads that
each found a part corrupt, the next load records it as a want exactly as a gone part, and the
fence lifts. Three is the bar repair sets for absence (`holeConfirmations`): a manifest is written
once, whole, as the part's commit point, so damage repeating over three maintenance cycles is the
object, not the read. The runs are per part and in memory (`corruptLoads`): a load failing for any
other reason clears them all, a part the failing load did not find corrupt starts over, and a
restart forgets. A failing load still opens every entry, so each corrupt part advances its own run;
stopping at the first would let two corrupt parts reset each other forever. Two failures never
exit. A backend error says nothing about the part. `block.ErrUnsupportedVersion` is an intact part
from a newer release: repair would replace a readable part with a hole, when the remedy is a binary
that reads it, so it stays fenced. Without a cluster the want cannot resolve on its own — the sole
owner's repair seam refuses to call a present part absent — so reads over its range fail until an
operator removes the objects or restores the part; the fence it replaces stopped the whole engine.

Two consequences fall out of the entry no longer being there:

- The gone part is absent from `indexed`, so the next commit's diff does not also call it a
  *removal*. A loss restated as a deliberate deletion is exactly the ambiguity wants exist to
  remove.
- `foreignEntries` refuses to adopt it back from a rival's index, and the orphan sweep spares its
  surviving objects. They are the remains of a part repair is owed, not the residue of a failed
  flush, and deleting them destroys the evidence before repair can see it.

Only an owner commits it, because recording a want is committing an index, which is the owner's to
write. Every other load still drops the gone part and keeps it as a *pending* want — counted,
disclaimed over, protected from the sweep — that the engine's first commit as a writer records, which
only an owner ever makes. A cluster node recovering holds no claim yet, so it loads through
`LoadPartsUnclaimed`: the sweep runs, the want stays pending. A replica's `RefreshReplica` does the
same without the sweep; failing the refresh instead would leave the engine serving the handles of the
previous load, a part set that says a part is present while nothing under its prefix can be read, and
the object-level mirror (`cluster/ARCH.md`) is what brings the objects back so the next refresh
clears the want. Committing at recovery or on a replica would let a non-owner publish a want at a
generation above the owner's; a strict backfill then installs that index on the owner and a part the
owner holds intact reads as lost.

## Disk pressure closes the ingest path

A flush that cannot fit is not a retryable failure. It detaches the head, fails part-way, burns a
part sequence, and leaves the head to grow behind it — so a node with a full disk manufactures its
own damage continuously while still acking every write, until it OOMs with everything unflushed in
it. `internal/diskguard` (shared with `recordengine`) closes the loop:

- **Before** the head is detached, a flush asks the backend for free bytes and free inodes and
  compares them against the pending part plus `Config.MinFreeBytes` / `Config.MinFreeInodes`. A
  flush that cannot land never starts, so the samples stay where a later flush can still write them.
- Either verdict **latches**. While it stands, `Append`/`AppendBatch`/`ApplyPrimary` reject with an
  error wrapping `backend.ErrNoSpace`, reads keep answering from what is on disk, and
  `Stats.OutOfSpace` (surfaced as `SignalStats.OutOfSpace` and the `storage.disk.out_of_space`
  gauge) says so.
- An ENOSPC from the write itself latches the same way — the disk can fill between the check and
  the write — while any other backend error does not: a transient fault must not make a node
  read-only.
- The latch clears on the next flush that finds room, which the maintenance loop runs on its own
  cadence. Recovery needs no operator action. On a backend that reports **neither** axis (an object
  store), a latch set by an ENOSPC clears at the next flush attempt and re-latches if that write
  fails too: nothing can say when such a medium has room again except writing to it, so the flush
  cadence is the retry.

**Reject, not block.** A full disk is not a queue that drains: a writer parked waiting for one would
only convert an unbounded head into unbounded blocked callers, and still lose the data. The error is
retryable, so an embedder can shed or throttle at the protocol edge, where it has a client to tell.
The opt-in *blocking* backpressure for a head that cannot drain fast enough is a different valve for
a different, transient condition; it must treat `ErrNoSpace` as a reason to stop waiting, not to
wait longer.

**The reserve is for compaction.** A merge writes its output before it can retire the inputs that
would free the space, so a disk at 100% cannot compact its way out. The headroom is deliberately
small — the guard catches a medium that is actually out, and is not a capacity policy — and both
axes can be waived with a negative value.

**The wrapper hazard is the whole guard's weak point.** `FreeSpace`/`FreeInodes` are type
assertions, so a `Backend` decorator that does not forward them silently turns a bounded disk into
"unbounded" and the guard never fires.

## Flush and merge are bounded separately

| phase | splits at | measured in | why |
|---|---|---|---|
| flush | `MaxPartBytes` | approximate uncompressed bytes | it sizes rows already in the head |
| merge | `mergeCapBytes` (`mergecap.go`) | bytes on disk | what the writer actually encoded |

The merge's two seal numbers — that disk cap and `mergeMemoryBudgetBytes` — are paired as
`mergestream.Budget` (`mergeBudget`), so the record engine cannot invent a second unit for the same
pair; `internal/mergestream/ARCH.md` says why they are separate. The series union a merge walks is
`mergestream.Keys` over each part's index (`mergeKeys`), a k-way heap rather than a set of every
distinct series — a paged index feeds it straight out of the sidecar entries.

Splitting at row boundaries is safe: parts are independent, and a series spanning two is merged back
by the read seam.

```
concurrency   = clamp( mergeMemory / 64 MiB, 1, MergeConcurrency )   ← memory decides, cores cap
mergeCapBytes = min( MergeCeilingBytes,
                     free        / MergeConcurrency / 2,   ← merges that can *run*
                     mergeMemory / concurrency      / 2 )  ← merges the budget *admits*
                                                              (whole-object backends only)
```

**Why not a constant:** cardinality spends the budget in *breadth*, so a fixed cap shrinks a part's
time span as active series grow, and a fixed-range query opens proportionally more parts. Both
references size against storage instead (VictoriaMetrics `getMaxOutBytes`, ClickHouse
`max_bytes_to_merge_at_max_space_in_pool` lowered by free space).

**Why divided:** by the concurrency, so concurrent merges cannot collectively fill the disk; halved
again to let a merge's output coexist with inputs it has not yet retired. `MergeConcurrency` is a
**callback**, since fan-out is bounded by the node's engine count as much as by the worker limit and
engines appear lazily; fixed at engine creation it would divide a single-tenant node's disk by its core
count, leaving a 32-core box a thirty-second of it.

**The divisor is a memory quantity, not a core count.** `MergeConcurrency` is the *ceiling*;
`memlimit.MergeConcurrency` lowers it to however many merges the budget can give a usable allowance,
64 MiB each — the read side of a streaming merge (k sources × columns × the read-ahead window).
Dividing by the core count instead prices memory in CPUs: a 16-core pod with a 4 GiB limit would hand
each merge 32 MiB and produce a great many merges too small to keep up with ingest. VictoriaMetrics
makes the same split from the other side — its merge workers are CPU-bound and CPU-counted, while
`memory.Allowed()` sizes the *parts* (`getMaxInmemoryPartSize`, `getMaxSmallPartSize`).

**The two divisors differ on purpose.** Free space is divided by `MergeConcurrency` — the merges
that can *run* — because without an admission pool that is how many may be writing at once. Memory
is divided by the smaller, budget-derived number, which is how many are admitted. Dividing the disk
by the memory-derived count would size each output against a share more merges than that could be
producing.

**And the memory division is enforced, not assumed.** `internal/memlimit.Pool` is a process-wide
byte semaphore that every engine of every signal draws on through `Config.MergeAdmission`; a merge
reserves its resident share once it has selected sources and holds it until it ends. Without it the
share is arithmetic nobody keeps: the facade fans merges out on its own schedule, so the division
understated what a merge may hold *and* overstated how many may hold it, at once. Note the bound is
the *request*, not measured residency: below the cap floors (`minMergeCapBytes` here,
`MaxPartBytes` in `recordengine`) a merge can hold more than it asked for.

**A `MergeOptions.Background` merge does not wait for its budget; it defers.** Admission is taken
after selection, so a cycle with nothing to compact never consults the budget — and when the budget
is committed the merge declines and the next cycle retries. Parking would be worse than it looks:
the facade runs maintenance on *one* goroutine that also services size-triggered flushes, so one
waiting merge would hold back every engine's memory relief for the rest of the cycle — to bound the
memory merges take.

Waiting is the **default**, and `Background` is opt-in, set only by that loop: a merge someone asked
for — an operator command, a test, an embedder driving the engine — must produce one, not a silent
no-op. A waiting merge does hold its own engine's `flushMu`, which `merge` takes before reaching
admission, so that engine's flush waits with it. The pool admits no new holders while anyone is
queued, so the wait is bounded by the merges already running plus whatever is queued ahead.

**A deferral is visible.** It increments `storage.merge.deferred` (`ADMIN.md`), sets
`Engine.MergeDeferred`, and does *not* log "nothing to compact" — which is what it would otherwise
look like. The facade sorts a deferred engine ahead of head-bytes pressure on its next pass — which rotates
the budget only when there are more engines than maintenance workers; below that every task starts
at once and the order decides nothing (#646). The idle-waiver counter is zeroed only once a merge is admitted: a
deferral has not broken the fixed point the waiver exists to escape.

**The trade is throughput.** A 16-core node with a 2 GiB limit runs 4 concurrent merges rather than
16 — the same total resident bytes, a longer compaction cycle in wall-clock.

**The pool is sized once, at `Open`; a merge's request is computed per merge.** Both read the same
`MergeMemoryBytes`, and a configured value cannot drift. A *derived* one re-reads `GOMEMLIMIT` per
request (`internal/memlimit.Bytes`), so an embedder that changes the limit at runtime moves the
requests without moving the pool: lowering it under-uses the pool, raising it makes each request
larger than the pool, which clamps to the total and serializes merges.

**Degenerate cases.** Backends that cannot report free space (`Memory`, object stores) keep the
ceiling. A nearly full disk falls to `minMergeCapBytes` rather than sealing everything, since stranding
the part count high is worst when compaction matters most; that floor binds derived bounds only, so a
configured ceiling below it is honored.

**Memory bounds the cap only over a backend that takes objects whole.** There the output accumulates in
RAM until sealed and `build` serializes it into one buffer per column, so peak resident ≈ 2× the part.
Free space says nothing about that: a 4 GiB pod on a 464 GiB volume derives 232 GiB, clamps to the
16 GiB ceiling, and OOMs. Hence the third term, split across concurrent merges and halved for the
serialize step; the allowance defaults to an eighth of the process budget (`GOMEMLIMIT`, else the
cgroup limit, else host memory — `internal/memlimit`).

Over a `backend.ObjectCreator` (`file`) the term drops: the writer hands each column's frames over as
they seal (`block/ARCH.md`, "Two writers") and never holds the part. A streamed merge still holds
**per-series** state — id runs, series index, aggregate sidecar — which is O(distinct series), not a
function of part bytes; a merge of very short series holds far more per encoded byte. So the loop also
seals on `partStreamWriter.residentBytes()` against `mergeMemoryBudgetBytes()`, the same allowance at
face value, bounding memory directly.

## Merge — one pass, five modes

```go
MergeWith(MergeOptions{RetainFrom, Downsample, Recompress, Precision})
```

Compacts a bounded, size-tiered group of parts, never the whole set. All five modes are the one merge
engine; no parallel subsystem.

| mode | effect |
|---|---|
| compact | merge per series by timestamp, freshest wins |
| retention | drop samples past `RetainFrom` |
| downsample | roll up by tier |
| recompress | size-graduated level; the age tier only on a fully cold part |
| precision | re-encode at a lossy budget, fully cold parts only |

**Determinism:** absolute timestamps, never clock reads — the caller resolves policy against one `now`
per pass — and grid-aligned downsample buckets, so a rollup does not depend on when the merge runs.

**Fixed points:** repeated merges are stable for last/first/min/max/sum/avg, count being the documented
exception. Recompression checks the part's recorded algorithm *and* level, precision the manifest's
recorded budget; only an upgrade rewrites, and a part denser than the target is left alone.

**Weight-aware:** compaction and rollup honor the lossy-sampling scale factor, keeping a sampled series
unbiased.

### Graduated compression (`recompress.go`)

| tier | level |
|---|---|
| row-count ladder, every merge output | zstd 1 ≤ 64k rows, 2 ≤ 1M, else 3 |
| age tier `RecompressSpec` | at most one, above the ladder, fully cold only |
| flush | codec-only framing; a hot flush is unaffected |

The ladder is VictoriaMetrics', capped the same way. A merge rewrites the data regardless, so the only
cost is the level's CPU, and a bigger part — older, read less per byte, merged less often — earns a
denser level. **Recompression is decode-transparent:** the reader keys off the manifest's per-column
algorithm, a pure ratio/CPU trade with no format change. The level is recorded only so a merge can tell
a part at the target from one below it.

### Retention drops whole parts first

A part whose `maxTime` is past the cutoff holds no row retention would keep, so `dropExpired` retires
it on the manifest alone: no decode, no output part, only a straddler rewritten. Retention is then O(1)
in the expired data rather than O(bytes), as in Prometheus (whole blocks) and VictoriaMetrics (whole
partitions). The drop publishes like any merge, so a failed commit rolls back and the parts stay live.
Disk-pressure eviction shares `RetainFrom`, so it drops whole parts too.

### Selection is confined to an aligned time bucket (`timebucket.go`)

Selection is bucketed by time, not by size alone: size tiers have no notion of time, so an unbucketed
selector folds an hour-wide part into a day-wide one until every part overlaps every query window.

```
mergeLadder:  1h  →  6h  →  24h      each level divides the next, so buckets nest
              ↑ flushes land here    ↑ widest part built; the coarsest locality a query can rely on
```

Parts are grouped by aligned bucket, and the size-tiered selector runs unchanged inside one group — as
in ClickHouse, where the size selector is only ever fed one partition. The ladder is walked
narrowest-first, so each part is rewritten once per level rather than repeatedly at the widest.

**The still-filling newest bucket is skipped**, since merging it now guarantees merging it again. The
finest level is exempt: flushes land there, and letting them accumulate is the part-count growth
sealing exists to bound.

**Forced rewrites are confined too, and win the cycle.** The oldest forced part picks the bucket, the
rest of that bucket rides along if it fits the cap, and the size-tiered run waits — unioning the two
would merge parts from opposite ends of the store into one spanning both. Merging inside a bucket
cannot widen.

**A straddler belongs to no bucket** and cannot join one without widening the output, so it is left out
of ladder groups, and a store can hold wide parts the ladder never narrows. A forced straddler is
rewritten *alone* rather than skipped: retention correctness does not depend on splitting it.

### Run selection (`compact.go`)

Picks only what is worth merging: any part a forced rewrite must touch, so age-driven work is never
starved, plus the best run of *unsealed* parts.

| rule | effect |
|---|---|
| a part at the merge cap is sealed | part count ≈ dataset / cap, not growing per flush |
| runs score `m = output / largest input` | the inverse of write amplification |
| escape: total under `smallRunBytes` | skips both guards |
| escape: `mergeIdleRounds` fruitless merges | best run taken regardless of score |
| `MergeOptions.Force` | the operator's version of the idle escape |

Re-merging a sealed part would only re-split it. Sealing and the run budget are in on-disk bytes
(`part.sizeBytes`), and `maxMergeParts` bounds a run too, so a large disk-derived budget cannot make
one merge unbounded. Scoring follows VictoriaMetrics' `appendPartsToMerge`.

**Ordered by size, not bucketed into size tiers:** a tier scheme strands a part alone in its
power-of-two tier, where it never reaches the two-per-tier threshold and is carried by every query for
the engine's lifetime. Ordering alone is not enough, because such strays sit geometrically apart and
every run over them fails the balance test or scores under `minMergeMultiplier` — hence the two
escapes. The guards reason about the proportion of bytes rewritten, which is meaningless when the whole
rewrite is cheap, and rewriting a large part once to absorb a stray beats carrying it forever. `Force`
supplies an idle count already at `mergeIdleRounds`, so it is the same selection the engine reaches on
its own — bypassing the heuristic, never the memory bound.

The selector works in size order but returns the run in engine part order, so the merge visits sources
oldest → newest and a later part's value wins a duplicate timestamp.

### Merge shape (`mergeshape.go`)

`Engine.MergeShape` reports the selector's inputs off the merge path (fields in `ADMIN.md`). A no-op
merge is otherwise indistinguishable from an idle engine, and a store can sit at a part count it will
never reduce for thousands of cycles with nothing saying so. The cap comes from the last merge rather
than being derived on demand: deriving it reads free space, and introspection does no I/O.
`MergeShape.Bytes` sums the parts' manifest sizes (no backend stat calls), so the same snapshot says
whether a rising part count is parts that grew or a merge that stopped taking them.

### Streaming both ways (`compactStream`)

```
source part ─ ColumnScan cursors ┐
source part ─ ColumnScan cursors ┼→ merge per series → partStreamWriter → block.StreamWriter
source part ─ ColumnScan cursors ┘  (one series range   (streampart.go)    encodes a full granule
 (one read-ahead window per column)   at a time)
```

Working set is O(parts × (columns × read window + one series range)) plus the *encoded* output — not
O(dataset), not the sources' encoded columns, and not the output's uncompressed rows.

**The read side.** Each source column is read through `block.PartReader.ColumnScan` with a
`defaultMergeReadWindow` (1 MiB) read-ahead, so a merge holds one window per source column rather than
its object: 8 sources × 3 columns is 24 MiB at most, whatever the sources' size. That is a bound, not a
constant, and it is not charged against `mergeMemoryBudgetBytes`, which prices only the output side.
Measured (`TestMergeResidentFlatInPartSize`, file backend, 64 KiB window): growing the sources 7.6×
(2.5 → 18.9 MiB) moves the merge's peak live heap from 1.6 to 1.8 MiB; reading whole objects instead
moves it from 2.4 to 18.9 MiB. The file and s3 backends have no zero-copy view, so a whole-object read
is a heap copy the merge holds for its whole duration — which is what this replaced.

Ranged reads bypass `backend.Cache`, so a merge no longer evicts the query read cache with its sources.
Two cases still read a column whole, once: a constant or unblocked column (no frames; the first costs
no I/O, the second is one small stream), and a backend offering neither `ReaderAt` nor `ViewerAt` —
the EC wrapper, a minimal embedder backend — where each ranged read would itself be a whole read. An
`s3.ObjectStore` without `RangeObjectStore` claims `ReaderAt` yet answers every range with a whole GET,
so a merge over it re-reads each column once per window.

**The merge unit is still one whole series.** `decodeRange` holds a series' range per source and
`collect` materializes the merged series before downsampling, so a part of few series with many samples
each holds O(one series × parts), and one hot series can approach a whole part.

**The write side.** Streaming the output decouples granularity from memory: the row cap sets part
size only, where buffering the output would also make it the peak-memory knob (`capRows × 32 B`, 512
MiB at the default), so a cap raised to widen parts would raise merge RSS with it. On a 2M-row merge
streaming costs 3.7–5.4× less total allocation and 2.9–3.8× less peak heap than buffering, the wider
margin on counter-shaped data.

Sidecars stream with it: series index, identities and aggregate stats come from the same per-series
calls, each run- or series-shaped, so they cost O(distinct series), not O(rows). Encoding decisions
must be fixed before the first row — compression profile, precision budget, whether a weight column
exists — so `mergeEncoding` derives them from the sources up front; its doc has why that is equivalent.

The single-part forced-rewrite path (`writeColumns`) buffers whole columns: its fixed-point check needs
the post-downsample row count before deciding to write at all. It is bounded by one part, and is not
the routine compaction tick.

### Publish ordering

A merge retires its sources only after the bucket index naming their replacement is committed. The
index is what a restart and every replica read, so a part it still names must never become reclaimable:
retiring first would let the next reclaim delete referenced objects, and every later load would drop
the part into a want repair can never satisfy. A failed commit rolls the in-memory swap back, so the uncommitted output is
never observable as published; its objects are orphans, swept by a later open.

### Repair — a want is discharged by committing a part

A part this engine's index names but cannot read is recorded as a `bucketindex.Want`, carried
across every commit alongside the removals. Repair runs at the head of each merge cycle
(`repair.go`), so a part pulled back joins the same compaction, and it **never fails the merge**: a
shard that cannot be repaired must still compact.

Satisfaction follows `Index.Satisfying` — the exact part, **or the largest live part whose block
set contains the want's at a higher level**, or, when no single part does, **a split group whose
members are all present and whose joint claim covers it**. A group is answered one member at a time,
so repair runs a second fetch round in the same cycle: the first round's answer names the group, and
`Index.Missing` names the members still to fetch, by block rather than by prefix (no prefix is known
for them). The whole group therefore lands in **one commit**, which matters — a fragment committed
beside the ancestors it partly duplicates, with nothing yet able to retire them, would have those
rows read twice. A round that cannot complete a group inside the per-cycle fetch budget commits none
of it (`dropIncompleteGroups`) and the next cycle asks again. Completing a group retires the parts
its claim covers, through `bucketindex.Subsumed`, the same swap a merge publishes.

A group with a member no peer can supply therefore **stays outstanding rather than becoming a hole**:
each cycle answers the want with a member it already has, which resets the absence evidence, so the
two gates a hole needs are never both cleared. That is the safe direction — an outstanding want is
visible and recoverable, a hole over live data is neither — but it is a stuck state, and the signal
for it is `RepairStats.Fetched` climbing while the wanted count does not fall.
These targets are **never wants**: nothing about them reaches the index, so the obligation stays the
original want's and `Entries → Removed | Wanted` is untouched. That is what makes repair terminate: by the time
a want is serviced the data may exist only inside a merged successor, and chasing a prefix that no
longer exists anywhere would never converge. The local index is asked first, so a want this
engine's own merges already covered costs no network call at all — and then the local **disk**: a
want whose part still opens from this node's own backend is discharged without a peer
(`RepairStats.Local`). The index that recorded the want may be a peer's copy installed by a
backfill, which knows nothing of this disk; and holding is proven by opening the part, because
surviving objects under a prefix are not a readable part. Pending wants — those a load could not
commit — are serviced alongside the committed ones, since the repair commit is a commit.

The local index is asked **again at commit time**, not only before the network. `Satisfying` is
re-run over what the commit will hold — the live entries plus the parts opened earlier in the same
loop — so a want the commit already covers is skipped rather than opened. That is what separates the
two reasons an open can fail: objects that are present but unreadable are a transient failure and
the want stays for the next cycle (`RepairStats.Failed`), while a want another entry in this very
commit already contains is neither failed nor owed. Counting the second as a failure would report
repair as stuck at the moment it converged, and `RepairStats` is the only view an operator has of
that (`ADMIN.md`). Two wants answered by one merged successor produce exactly this shape whenever a
peer names each want's own prefix rather than the successor twice.

`Config.Repair` (`PartFetcher`) is the whole seam to the cluster: part identities in, the entries of
whatever parts were actually copied out. The engine never learns about peers, addresses or transport —
`cluster/partsync` supplies the implementation; a store without a cluster layer supplies one that
fetches nothing (see "A store without a cluster layer is its own complete owner set"); nil (a shared
backend where every replica reads the same objects, or a read-only store) makes repair a no-op. A `WantAbsent` outcome with a nil error
is definitive absence and leaves the want outstanding, counted in `RepairStats.Unsatisfiable`; an
error is transient and retried next cycle. The two are never merged: an unreachable peer is not
evidence that data is gone.
The seam's types are `backend/bucketindex`'s, aliased here and in `recordengine`, so the facade
hands both engines the same fetcher.

The seam takes the **whole cycle's wants in one call**, capped at `repairFetchesPerCycle`, and gets
one result per want back. The cluster-side cost is per cycle, not per want — one read of each peer's
bucket index answers every want, and one copy of a merged successor discharges every want inside it
— so splitting the cycle into per-want calls would multiply both by the want count. Fetch
concurrency therefore belongs to the implementation, not to the engine.

Publishing a repaired part is the same swap a merge publishes — it is committed into `Entries`, and
the commit's want trim discharges every want it satisfies. A fetched **successor** also retires the
local parts it supersedes, because their rows are inside it and keeping both would count them
twice. A part whose objects arrived but will not open is rolled back to a failure, so the want
stays.

**No count trims an outstanding want.** `bucketindex.MaxWants` (4096) is the horizon past which a
node owes more than part-by-part repair can converge on; the commit logs past it and keeps every
want. The horizon counts *parts gone from this node's disk*, not time away, and compaction holds the
live part set far below it: 35 live parts at 58 MiB of logical ingest, 85 at 234 MiB — one per
~1.67 MiB, four times the data giving 2.4 times the parts, since the ladder's top level is a day
bucket. 4096 wants therefore needs a shard of tens of TiB or years of retention. Absence does not
reach it from the other direction either. The wholesale adoption is `cluster/partsync`, which copies
the objects a superseding peer's current index names and installs that index; a returning node's
wants are what the mirror has not restored *yet*, bounded by that same live part set and cleared by
the next refresh, rather than a count that grows with how long it was away. Truncating the list
would break the one
invariant the read policy stands on — a part leaves `Entries` only into `Removed` or into `Wanted` —
and a want dropped that way is neither a hole nor a `LostParts` increment: the window it covered is
served short with nothing to say so. Refusing the commit instead would keep the invariant trivially
(the entries stay) but wedge the node — flush, merge and the repair commit that discharges wants all
go through the same commit — for the same index bytes, since a want costs what its entry did.

One cycle attempts at most `repairFetchesPerCycle` (4) wants with `repairFetchConcurrency` (2)
copies in flight. A repair fetch copies a whole part, so an unbounded pass on a badly damaged node
would spend the maintenance cycle in the network and never compact; a shard needing more than a
handful of parts back is past what part-by-part repair is for. The *serving* side, where the real
budget belongs (see `cluster/ARCH.md`), is uncapped.

A pass is single-flight, gated by `repairGate` — its own gate, not `flushMu`. `MergeWith` is
callable concurrently (an operator's `Admin.MaintainNow` alongside the maintenance loop), and two
passes snapshotting the same wants copy the same part from a peer twice into one object prefix.
Only one of them commits — the winner's parts already carry the prefix — so the index stays right,
but the loser has paid for a network copy, counted it in `RepairStats.Fetched`, and, once the
winner's merge has retired the prefix it copied into, counts a `Failed` for a part that is fine.
`RepairStats` is the operator's only view of whether repair is progressing, so a counter that
inflates under concurrency misleads during exactly the incident it describes. The gate is repair's
own rather than a widened `flushMu` because the pass it covers is remote I/O: holding the flush
lock across a peer fetch would stall ingest for the length of a network copy. It is a buffered
channel rather than a mutex so a waiter honors the merge's context — a shutdown does not wait out
another pass's fetch. The second pass then observes the first pass's commit and finds nothing to do.

### An unrepairable want becomes a revocable hole

A want no owner can satisfy would otherwise stay outstanding forever, and the shard would be
permanently "not repaired" with no in-band way for an operator to accept the loss. So repair
acknowledges it: it commits `Entry{Hole: true}` at the lost part's identity, which discharges the
want and raises the index's monotone `LostParts` (`backend/ARCH.md`). Converting the want into a
*removal* instead would terminate just as cleanly and destroy the evidence — the part would become
indistinguishable from a deliberate deletion, which is the ambiguity wants exist to remove.

**Two independent conditions gate the acknowledgement, because a hole over live data is worse than
an outstanding want in every way that matters.** An outstanding want is visible and recoverable; a
hole over data that was never lost is neither.

1. **The peer set must be the shard's complete expected owner set.** `PartFetcher` answers with a
   `bucketindex.WantOutcome`, and only `WantAbsent` — every expected owner answered, none had it —
   is evidence. `WantIncomplete` means the peers asked were a strict subset: during a rolling
   restart the deregistered owner drops out of the ring, and the two reachable peers lacking the
   part says nothing about the third that holds it. The cluster layer decides this against the
   *configured* replication factor, not against whatever the ring currently returns, so a cluster
   permanently short of nodes never acknowledges a loss (`cluster/ARCH.md`).
2. **The conclusion must repeat over `holeConfirmations` (3) consecutive attempts.** One pass is a
   snapshot: a peer that is up, in the ring and has not finished loading its bucket index answers
   "no such part" truthfully and prematurely. Any other outcome — a fetch, an error, an incomplete
   owner set — resets the count. The evidence lives only in memory, so a restart forgets it and
   repair earns it again; the bias is deliberately toward leaving the want outstanding.

An error is never evidence at either gate: a peer that could not be reached says nothing about
whether the data exists.

**The hole is revocable and re-attempted.** Because the commit is not cross-replica atomic, an owner
can acknowledge a loss while a peer still holds the part. So every repair pass targets the holes as
well as the wants, and a hole is replaced by the part turning up — at its exact prefix, or inside a
containing successor. Holes are held apart from `parts` (there is nothing to open) and re-read from
the index on load, so an acknowledgement survives a restart. `LostParts` does not fall when a hole
is revoked.

### A store without a cluster layer is its own complete owner set

With no peer to fetch from, a single-node want can never be satisfied, and without a hole it never
ends: every read reaching into it fails for the life of the shard (the read policy,
`cluster/ARCH.md`). So the store acknowledges the loss itself. The facade hands the engine a
`PartFetcher` (`soleOwnerRepairer`) that copies nothing and answers for an owner set of one. Both
gates above stand unchanged — `WantAbsent` only, over `holeConfirmations` consecutive passes, each
re-probing the backend — and so does the rule a want is minted by: absence is the backend saying the
part's manifest does not exist. Any other error is a failed attempt, and a manifest that is present
but will not open is a failure too, never absence.

**Absence is definitive because the backend is the only copy.** The cluster gate exists because a
peer may still hold the part; here nothing else can serve those bytes. What remains is whether
another *writer* shares the prefix and removed the part on purpose — merged it into a successor or
expired it — in a commit this engine has not rebased onto. In a supported deployment none does: a
non-clustered prefix has one writer, and the one supported co-tenant, a read-only handle, never
writes. The seam still re-reads the committed index
on every pass and counts absence only while that index states the loss — a want or a hole at the
prefix, not tombstoned, not contained in a committed part — so a rival's merge or expiry makes the
pass inconclusive (`WantIncomplete`) rather than a loss. A rival committing between the last pass and
the hole commit is not covered: the CAS retry lands the hole beside the rival's successor, the next
commit revokes it, and `LostParts` over-counts by one — only in a deployment that is already
unsupported.

**A read-only store never concludes.** Acknowledging a loss is an index commit, so its seam is nil.
It keeps the pending want its load found and the read policy fails reads overlapping it: it neither
answers short nor writes.


## Read path

`Fetch` resolves matchers over the index, then merges each series' head buffer ∪ every part by
timestamp — **one series per `Next`**, so a consumer that folds and releases each batch never holds
more than one series' samples, whatever the matched count.

The plan (acquired parts, decode reservation, head snapshots) and the fetch's span, profile and metrics
span the whole iteration, settled by `Close` — **which the caller owes even when it stops early**. What
stays O(matched series) is the plan: one identity per matched series, plus head and mid-flush
snapshots, copied under the lock because a concurrent flush moves a series' buffer into a part the plan
did not acquire.

Layered optimizations, each opt-in:

**Series index sidecar** (`{prefix}/sidx`) — sorted distinct SeriesIDs and run-start rows, fixed-width,
binary-searched in the raw bytes and held only while a fetch reads the part, re-fetched through
`backend.ReadView` as a zero-copy cache hit. Resident index memory then follows the read cache budget
rather than series count, and opening a part reads no series column. Derived: a missing or corrupt
sidecar scans the series column once instead, so no migration burden.

**Block slicing + decode cache** (`Config.DecodeCacheBytes`) — column blocks are sliced from a
byte-bounded LRU keyed by `(part, column, block)` and added to the merge as **views**, never
materializing a decoded part. Entries are immutable and refcounted, released per series after copy-out;
an evicted, unpinned buffer recirculates through a bounded freelist, cutting miss-path allocation
without enlarging the resident set. Cache-off, or constant/unblocked columns, decodes per fetch,
series-skipped to the blocks the matched row ranges touch. With the cache on a fetch also prefetches
the parts it will touch.

Every part decode enters through one seam (`Engine.decodeOf`), which owns the whole-part convention:
`nil` row ranges mean *decode the whole part* and resolve to a whole-column decode there, above both
the cached and uncached implementations. A block-selecting implementation must never see `nil` — it
returns part-sized buffers with only the selected blocks written, so an empty block set yields buffers
the caller reads as data (a pooled buffer then surfaces its previous occupant's rows). Non-nil ranges
carry the same contract in the other direction: rows outside the selected blocks are undefined, and a
caller reads only its matched series' rows.

**Granule time pruning** — block boundaries align with the part's marks granules, so the marks index
already carries each block's `[MinKey, MaxKey]` sample times (`block/ARCH.md`). A block-sliced fetch
drops blocks that cannot intersect the window before reading them, and sizes the decode reservation
over the survivors. Rows are `(series, ts)`-sorted, so granule bounds are not monotonic across a part,
but they are inside one series' row range — where the test applies. Without it a narrow window costs a
full part scan, a long series being decoded whole and discarded row by row. Derived and advisory —
absent, corrupt or mismatched marks prune nothing.

**Recent tier** (`Config.RecentWindow`) — mirrors the most recent flush window in RAM across flushes,
so a query inside the window acquires no part at all; overlap is deduped by the freshest-wins merge.

The tier is one of three in-memory sample sources — recent tier, mid-flush detached buffers, head —
and every read path enumerates them through `enginePlan.memSources`, never by naming the maps. That
is an invariant, not a convenience: a plan inside the tier's window holds *no parts*, so a path that
enumerates the tiers by hand does not merely miss the tier, it sees nothing at all. The aggregate
folds, the pushdown-safety spans and the step-grid sizing all read the same accessor as the merge.

**Buffer recycling** (`Request.Recycle` + `Batch.Release`) — default-off. Result buffers come from a
GC-stable doubly-bounded freelist, not `sync.Pool`, which empties at every GC and lost the capacity
under allocation-driven collections.

### Decode-memory budget (`Config.DecodeMemoryBytes`)

A shared byte semaphore over in-flight decoded column bytes, bounding the query-concurrency RSS cliff.
Reserved once per *fetch* off the lock — never incrementally per part, so two queries cannot deadlock
holding partial reservations — with a fetch larger than the whole budget admitted alone. The facade
builds one budget for all tenants, so the cap is process-wide.

**Per-fetch is only deadlock-free while a caller keeps one fetch open at a time.** True of a
drain-then-close consumer, false of a streaming one holding several iterators across an evaluation:
there the second acquire is hold-and-wait against the query's own reservation, and admit-alone cannot
fire, `used` being non-zero precisely because this query holds it.

**`Request.Scope`** (a `fetch.Scope`) names the logical query — the read-side analogue of Prometheus'
`Storage.Querier`, one per request, passed on every read under it. Its first read blocks as usual;
while it holds, later reads sharing the scope are charged without queueing. Accounting stays exact,
only the waiting is skipped, so such a query may overshoot the ceiling by its own later estimates — the
same latitude a single over-budget read has. A nil scope is unchanged; `fetch.WithScope` installs one
on the request context where a call path cannot thread it through every `Request`.

**A scope is an optimization, never a correctness requirement.** The library cannot verify "one fetch
at a time", so the wait is bounded twice and a missing scope costs latency and memory, never liveness:

- **Cancellable.** Admission takes the fetch's `ctx`; a done context aborts and reserves nothing, so a
  deadline or disconnect always recovers the goroutine and an unscoped hold-and-wait degrades to one
  failed query, not a wedged engine. `Fetch`/`Count`/`Aggregate*` can therefore fail before reading
  anything, releasing the plan on that path.
- **Force-admitted.** A queue head starved for `DefaultDecodeBudgetForceAfter` goes over the ceiling,
  counted and logged (`ADMIN.md`). The ceiling is already soft, so trading an RSS overshoot for
  liveness is the same trade.
- **FIFO with hand-off** — the releaser reserves on the waiter's behalf before waking it, since a
  cancellable waiter must not have to hand a turn back and a barging arrival must not starve a large
  query. `used == 0` still admits unconditionally.

## Count and series enumeration

`Count`/`CountBy`/`Series` share one **existence plan**: matched ids from the head index, which
outlives a flush, and in-window existence from the live buffers. No batch, no value column, no label
projection. They differ in what they ask of the parts:

| call | parts contribute by | decode |
|---|---|---|
| `Count`/`CountBy`, exact | sorted intersection, or binary search at a window edge | the two edge parts, ts only |
| `Series`, series-only | the matched ids the part's series index holds | never |

The intersection applies to a part fully inside the window. `Series` therefore costs matched
cardinality alone, not the window's depth, and its window filter is **part-granular**: a series in an
overlapping part is listed even if its samples sit just outside — the granularity `recordengine.Series`
and Prometheus' label endpoints also have. It is the primitive the metrics label endpoints need, whose
fetch-based answer would decode and discard every sample of every match.

## Label metadata

`LabelNames`/`LabelValues` answer from the index, not from series. With no matchers the walk is over
the postings' (name → values) map, O(distinct values), materializing no identity; with matchers it
narrows to the matched ids and reads only the requested name off each identity.

The head's series index is **all-time**: it outlives flushes, and retention prunes samples and parts
but never identities, so the index alone would keep offering labels of series whose data is long gone.
Every value is therefore **liveness-probed** — an in-window in-memory sample, or membership in a part
overlapping the window — stopping at the first live series, so a live value costs one probe rather than
a scan of its postings list. Part indexes are warmed before the engine lock is taken, keeping the probe
I/O-free under it.

## Aggregate pushdown

`AggregateRange`/`AggregateStep` return per-series count/sum/min/max (→avg) over a window or a
step-aligned grid. With `Config.AggregateStats` each part writes a small stats sidecar
(`{prefix}/stats`), and a range **fully covering** a part folds it without decoding the value column.

**Count and sum are weighted by the lossy-sampling scale factor**, so a sampled tenant's aggregate
estimates the originals rather than the rows that survived; min/max are not, an extremum being
independent of multiplicity. `SeriesAgg.Count` is therefore a float (Σw) and carries `Rows`, the
unweighted row count, beside it — an operator asking how much data is behind a number wants Rows,
a query asking how many events happened wants Count. Rows is also the aggregate's emptiness test:
the sliding-window accumulator adds and subtracts entries as windows move, and only an integer
survives that exactly, where a float count could leave an epsilon behind and report a window the
data has already left.

The stats sidecar stays integral and unversioned because **both writers gate it on the part having
no weight column**: every weight in a sidecar is 1, so Count equals Rows there, and a sampled part
has no sidecar and takes the weighted decode path.

Taken only when provably exact: in-window parts fully covered *and* pairwise time-disjoint, else it
falls back to decode+merge, which dedups. Each in-memory source is a span of its own in that
disjointness test, not one merged span: the recent tier holds already-flushed samples that a
re-append duplicates into the head, and merging the spans would hide that from the test while the raw
fetch went on deduping it. Derived, so absent or corrupt means decode. In cluster mode
it survives the network — each owner aggregates locally and ships per-series identity plus buckets, so
only aggregates cross the wire.

**The rejection is reported, not just taken.** `aggPushdownCheck` returns a reason and the number of
sources that tripped it, on every aggregate span. The three causes — query shape, store layout,
boundary mismatch — are unrelated and need separating; `ADMIN.md` reads them operationally.

**Trade-off worth knowing about.** A whole-part stat is exact only for a part lying entirely inside the
query range, so eligibility falls as parts grow: compaction — otherwise strictly good, giving fewer
parts, less per-part overhead and better compression — works against this pushdown. 71 small parts give
a 6h dashboard window many parts it wholly contains; the same data compacted to 3 gives it none, and
every such query drops to decoding every sample. The engine deliberately does not arbitrate: a merge
policy keeping parts small enough for a *particular* window would guess at the query mix and give up
the compaction wins for every other read. Reporting the reason instead lets an operator weigh it.

**Buckets accumulate in a `stepGrid`,** allocated once per call and reused across every series in it: a
dense array indexed by arithmetic on the timestamp, so filling costs no hashing and draining no sort of
aggregate structs. It spans the plan's *data* — parts' ranges ∪ the in-memory sources' span, clipped
to the request — not the request, routinely unbounded on one side. A grid too wide to index densely,
a fine step over a long span, falls back to a map sized by the samples.

### Overlapping windows

`AggregateWindow`/`AggregateWindowNamed` answer the *overlapping* range-vector shape: one aggregate per
step-aligned evaluation timestamp `t` over the half-open window `(t-W, t]`, where `W` may be many steps
wide — a 1h range at a 5m step is a 12× overlap. Cost stays proportional to the data in the request,
not to the overlap factor:

1. Samples fold **once** into disjoint fine buckets on the same `stepGrid`, so the sidecar pushdown
   still applies and a part inside one fine bucket never decodes.
2. A **sliding accumulator** walks each series' buckets once, adding the bucket entering a window and
   subtracting the one leaving.

Count and sum slide by arithmetic. An extremum cannot be subtracted back out — dropping the current
minimum would force a rescan — so min/max ride **monotonic deques** of entry indices: an arrival pops
every tail entry it dominates, such an entry being no better *and* expiring no later, leaving the front
as the answer, each entry pushed and popped once for O(1) amortized steps.

**The fine grid is left-open** (`(b, b+step]`, unlike `AggregateStep`'s `[b, b+step)`), the only
convention a half-open window edge never splits. The decomposition is exact only when `W` is a multiple
of the step; otherwise an edge falls inside a bucket and the call slides over merged raw samples.

**The grid is anchored.** `WindowSpec.Anchor` names a timestamp on it and windows end at
`Anchor + k*Step`. An evaluation grid belongs to the query, not the clock — PromQL anchors at the
query's start, a multiple of the step only by coincidence — so the fine buckets are phased to match, a
window edge never falling inside one. The zero value is the absolute grid.

Both forms drain one internal `iter.Seq2`, so only one series' windows are resident rather than
series × steps. That also shapes the instrumentation: decode and fold alternate per series, so they are
accumulated durations on one span plus a planning child span, not sub-spans (`ADMIN.md`).

## Cluster surface

| call | contract |
|---|---|
| `ApplyPrimary(walBytes)` | the shard's single authoritative accept/reject decision, logged to the WAL |
| `ApplyReplicated` | applies a payload verbatim, like WAL replay |
| `RefreshReplica` | reloads parts from the store and trims the head against each series' own flushed watermark |

`ApplyPrimary` OOO-checks and admission-checks each sample, returning the accepted set re-framed plus a
per-reason reject breakdown. The accepted frames are encoded from the staged batch, so a sample routed
to an overflow series replicates under that series, and every series in them carries its identity
record: a replica that joined or restarted since the series' last write does not hold it, and would
drop samples without it. A payload that fails to decode or to log applies nothing, not its valid
prefix.

**The replica trim watermark is per series, never one figure across the head.** A series absent from
every part keeps its whole head, and a series present keeps every sample past *its own* newest flushed
timestamp — the part set's newest time belongs to whichever series flushed last and says nothing about
the durability of any other. Trimming against it deletes late samples that are legal under their own
out-of-order bound and durable nowhere yet: invisible on replica reads until the owner's next flush,
and lost outright if the replica is promoted first.

Those watermarks come from the parts, since nothing per series is recorded in the bucket index. Flush
and merge both write them beside the part as the `{prefix}/smax` sidecar (`internal/watermark`: magic,
uvarint count, then `(u128 id, i64 max)` big-endian per series, trailing CRC32C), so a cold replica —
or one that has just mirrored a part with `cluster/partsync` — resolves them without touching the
timestamp column at all. A part written before the sidecar existed, or one whose sidecar is corrupt or
does not cover its series, falls back to decoding the column; absence is not an error. Either way the
result is held on the part handle at ~24 bytes per series, on top of the 20 the resident index costs.

**A reloaded index reuses the part handles the engine already holds** (`loadPartsLocked`) when the
handle already carries everything its entry records — time bounds, block identity, claim, level — and
has no identity pending (`part.matchesEntry`). Reopening throws away the watermarks, the paged series
index, the stats and granule sidecars and the registered identities, which a replica's maintenance
tick would then rebuild every tick over the whole part set. A replica's entries do not change between
ticks, so reuse is the steady state. The part's identity object is registered once per handle for the
same reason: it is immutable, and the identity prune only ever drops identities no live part holds.
Over four parts of 50 series × 500 samples on the memory backend this is a refresh at 9.7 µs and 5.3 KB
allocated instead of 653 µs and 943 KB.

**A published part is never written.** `PartsDetailed`, the merge selector and every fetch plan read
a part's time bounds after releasing the engine lock: the merge selector from a copy-on-write
snapshot, a fetch after `acquire()` (`Count`'s `activeFlags`, the aggregate pushdown's time-span
checks). `MergeShape` reads the same kind of snapshot but only the part's immutable size. A reload whose entry disagrees with the handle therefore opens a fresh handle
rather than updating the old one in place. A shallow copy is not an option, because the handle holds a
`sync.Once` and atomics. The rare changed entry costs one open.

The owner's commit is the one writer of a published part's block identity. `updateIndexLocked` stamps
`blocks`/`claim`/`level` onto a part only once its CAS lands, and a flush whose commit failed leaves
its part published with the identity still pending. That is safe because the one reader of those
fields outside `e.mu`, `planMergeBlocks`, runs under `flushMu`. Every commit that can find a pending
identity holds `flushMu` too: flush, merge, retention drop and repair. A reload stamps none: the only
pending handles it publishes are flushed parts whose commit failed, carried over as they are, and an
owner load that carries one leaves its want commit to the next flush or merge.

Reuse cannot skip noticing that a part's objects have gone away, which is what turns a lost part into
a want. A reused handle is probed with `block.PartPresent` — the manifest, the commit point of a part
write — so a refresh pays one small object per part instead of reopening it.

**The primary logs the accepted set** — the frames it already built to replicate, written to the WAL
verbatim (`wal.WriteFrames`). That is what makes the quorum's "one durable copy at the primary" true:
without it a restarted node recovers only flushed parts and then answers as a ring owner, serving
everything written since its last flush as absent. `ApplyReplicated` does **not** log: a secondary
neither flushes nor checkpoints, so its log would grow without bound: its head is memory-resident,
recovered by catching up rather than by replay.
