# `wal/` — write-ahead log

CRC-framed records (`[uvarint len][type][payload][CRC32C(len+type+payload)]`) appended to numbered
segment files, rotating at a size limit. Replaying a log rebuilds the symbols+series+postings index and the head —
the unflushed state a crash would otherwise lose. Flushed data comes from the backend instead
(see [`../backend/ARCH.md`](../backend/ARCH.md)).

Record types (additive — an old reader **skips** an unknown type):

| Type | Payload |
|---|---|
| series | `SeriesID` + typed attribute encoding |
| samples | metric samples |
| scale-factor samples | metric samples carrying per-sample lossy-sampling weights (written only when sampling occurred) |
| records | opaque record-engine payload (logs/traces/profiles) |
| side | opaque content-addressed side-store delta (the profile symbol store) |

Replay surfaces a bad-CRC *complete* record as corruption and stitches segments in order.

**The checksum covers the length varint**, not just the body. `readFrame` trusts the length to
decide where the frame ends, so leaving it uncovered leaves the one field that steers the reader
unprotected: a payload can hold the checksum of a prefix of itself, and a corrupted length then
carves a shorter, checksum-valid record out of a longer one — a record the writer never wrote,
dispatched without complaint, with the reader parsing from the wrong offset afterwards.

What length coverage does *not* buy: a length inflated past the end of the buffer. The completeness
bound has to run before the checksum (the checksum's span depends on the length), so such a frame is
byte-indistinguishable from a torn tail no matter what the CRC covers. Refusing a short read outside
the last segment is what closes that one, not the checksum.

**This framing is also the node-to-node replication payload** (`ApplyPrimary` → `cluster/replica` →
`Replay`), so widening the checksum input is a wire break as well as a disk break. The break is
loud in both directions — an older directory salvages nothing, every frame reported as damage, and a
mismatched replication payload is rejected rather than half-applied — and both halves need a
coordinated rollout. Carrying a discriminator in the segment name would version only the disk half
and leave the wire half broken anyway, which is why the format is not versioned here.

## Where a torn record is tolerated, and where it is not

Exactly one place may end mid-record: the **last segment of a WAL directory**, which is the one a
crash was appending to. `ReplayDirFrom` accepts a torn tail there and nowhere else — a short stop in an
earlier segment means the rest of that segment, and the hole it leaves in history, would be skipped
while the segments after it papered over the gap. Skipping it silently is loss bounded only by the
segment size, so it is damage: reported, never taken for the end of the log (see *A damaged log is
salvaged*).

`Replay` itself is therefore **strict**: it is handed a complete log — a whole segment, or a
replication payload from `ApplyPrimary` — so a record that does not fit inside the buffer is
truncation, not an end of stream, and an error: decoding it as a short batch instead would let a
replica diverge from its primary in silence. The records applied before the stopping point are kept
either way.

Making the tolerance last-segment-only is only safe because `Create` **repairs on resume**: it cuts
the highest-numbered segment back to its last complete frame before opening the next one (Prometheus
`wal.Repair` semantics), by the same copy-and-rename a running writer's restore uses (below), never by
truncating the segment in place. Without that, one ordinary crash leaves a torn tail that the
next run turns into a permanent *middle* segment, and every later replay fails. The discarded bytes
are an incomplete frame no valid frame follows, unreadable by construction. Everything before them
is left alone: a *complete* frame that fails its CRC, or a hole with whole frames past it, is damage
rather than a torn append, and replay is the one place that reports it — cutting it here would erase
the records past it and the evidence.

### A failed write is healed before the next one

The same tolerance would not survive a running writer that kept appending after a write failed
part-way. A full or failing disk returns a short write, and a torn frame followed by later frames is
exactly the corruption replay refuses, so one `ENOSPC` would make the store unopenable and take every
acknowledged record behind it with it.

So `SegmentWriter` never writes behind a partial frame. A failed write records the segment and its
length before the write; before anything else is written, `heal` restores that segment to the length
if the write left bytes past it, and the writer then moves to a fresh segment, since its open handle
reaches the dropped bytes. The restore writes the kept prefix to a `.repair` file (not a segment name,
so a stray one is never replayed, and `Create` removes it), syncs it, closes the torn segment's handle
without a sync — the copy now holds its whole frames — renames the copy over the segment and syncs the directory — a rename rather than an
in-place truncate, because a crash midway through rewriting the segment would lose the records already
acknowledged from it, where a crash before the rename leaves the tear at the log's tail for `Create` to
repair. The handle is closed before the rename because Windows refuses to rename over an open file, and
on POSIX a handle kept open would write into the replaced file's orphaned bytes.

Until the restore succeeds every write is refused, including the one that would open a new segment, and
`Sync`, `Seal` and `Close` fail: the handle is already gone, so the records written before the failed write
are durable only once the restore is. A failed sync of a segment a rotation closes gets the same
treatment: the write that rotated reports it, and so does the next `Sync`, `Seal` or `Close`, since the
records already in that segment were acknowledged and no later sync reaches them. Success includes the directory sync: a retry that finds the segment already at its kept length cannot
tell whether an earlier attempt's rename is durable, so it syncs the directory again before clearing
the tear. A segment checkpointed away in the meantime needs no restore.

### Proving a tail is a tail

Ending mid-record is not by itself evidence of a torn append. A crash does not always truncate to a
clean prefix: per-block writeback can land a later block while an earlier one is lost, and the
filesystem zero-fills the gap; a preallocated tail reads back as zeros for the same reason. The
result is a **hole** — a zero region followed by frames that did reach the platter — and a reader
stopping at the zeros would mistake committed records for the end of the log.

So a stopping point is a tail only when no frame replay can trust follows it. A trusted frame is
CRC-valid and followed by another valid frame or by the end of the segment: a chance CRC32C match is
2^-32 per candidate offset, two in a row is negligible, and payloads are engine blobs, never nested WAL
frames, so nothing biases that. The cost is a lone frame between a hole and a torn tail, dropped with
the tail. (A strict replay keeps the older single-frame test, which reports such a lone frame as a
hole.)

The scan is byte-aligned, since a hole destroys frame alignment and the resume point can be any
offset. Cost is bounded two ways: candidates whose length varint is zero or overruns the buffer are
rejected without a checksum (a zero-fill is one pass over the gap), and the checksummed bytes carry a
1 GiB budget per segment — roughly 0.3 s — after which nothing more is trusted.

Pebble instead carries a durability watermark (`SyncOffset`) in every chunk header and proves
corruption when a later chunk promises durability past the bad offset. That is exact, and costs a
format major version and a second reader; salvage makes the distinction matter less, since a hole is
skipped either way and only the report differs. The residual: a hole whose trailing region holds no
trusted frame — a gap that swallowed every following record, or one whose survivors are themselves
torn — is indistinguishable from a tail and is accepted as one.

## A damaged log is salvaged

A damaged segment does not keep a store from opening. For telemetry, losing the records in a damaged
region is within the contract; refusing to start is not — and with the default `WALSyncNone` a power
cut that lands a later block before an earlier one is an ordinary event, not a failing disk. So the
engines replay their directories through `Handlers.OnDamage`: each region replay cannot read — a frame
failing its CRC, a hole, a CRC-valid frame whose payload does not decode, a torn record in a non-final
segment — is reported and skipped, and replay resumes at the next trusted frame and goes on through every
later segment. The engines count it as `storage.corruption.detected{component="wal",
disposition="tolerated"}` and log it with the segment, offset and bytes skipped (`ADMIN.md`).

What salvage can lose: the records inside a damaged region; samples whose series record was inside it
(replay drops samples of an unregistered series); and the tail of a batch a crash cut through — a batch
is several frames, so its whole frames before the cut replay while the client, never acknowledged,
retries it. Prometheus's `wlog.Repair` instead cuts the log at the damage and deletes every later
segment, and Loki's replay stops at the first error; both lose everything past the damage to keep going.

The first damage replay meets in a segment copies the segment aside as `{segment}.damaged`, a name
replay never reads, so a flush checkpointing the segment away does not take the evidence with it. Only
the newest four copies are kept, so a disk that keeps corrupting the log cannot fill itself with them.

`Handlers.OnDamage` unset keeps replay strict — damage is an `ErrCorrupt` naming the segment and offset —
and `Replay` of a single payload is always strict: a replica must reject a damaged replication payload
rather than diverge from its primary.

`FuzzCrashDurability` holds all of this to what the writer acknowledges. It drives a writer on `faultfs`
through writes, batched writes, syncs, seals, checkpoints and clean restarts, and injects filesystem
faults, process deaths and power cuts, including partway through a recovery. Every recovery must open
and replay:
- records in write order, intact, none duplicated or invented;
- every record a successful sync covered;
- every acknowledged record when no power cut intervened;
- no failed write once a `Sync` healed it, and nothing a checkpoint or an earlier recovery removed;
- damage only after a crash that kept part of the unsynced state.

A sweep of 400 schedules runs with the tests. Each of eleven known durability regressions — a heal
skipped in `prepare`, `Sync`, `Seal` or `Close`, a handle left open, the directory sync skipped on retry,
an in-place repair, a swallowed rotation sync, a checkpoint or segment creation without a directory
sync, and a `Create` that refuses damage — fails the sweep or the committed fuzz corpus.

## Epochs — exactly-once recovery (record signals)

Segments are named `{seq}-{epoch}.wal`: `seq` orders replay, `epoch` is the flush generation, so a
segment self-describes which generation it holds. The watermark of the last-flushed epoch lives in
the **bucket index**, so it advances *atomically with part discoverability* — the very object
`recover` reads. `ReplayDirFrom(minEpoch, …)` skips segments at or below it, so even a crash in the
window between a part committing and its WAL being deleted re-applies nothing.
(Metrics don't track the epoch — their merge dedup makes that window self-healing.)

**The epoch is a per-node counter, and the index it lives in is shared.** Over a shared object store
every replica of a shard commits one index object under one prefix, and each of them counts its own
flushes over its own WAL directory: two writers' epochs are unrelated integers naming different
sequences of records. The index therefore keeps **one slot per writer**, keyed by
`engine.Config.WriterID` (the cluster node id, empty for a single writer — see
[`../backend/ARCH.md`](../backend/ARCH.md)), and a node recovers only its own. There is no
correct scalar: a foreign number below this node's replays records its parts already hold, and one
above skips records it never persisted, so max-merging on rebase trades duplicates for silent loss.

What that gives up: a node's WAL records are superseded **only by its own flushes**. A record
flushed into a part by a *different* node — the shard's compaction owner — is replayed by the node
that logged it, because nothing relates the two counters. That direction is duplicates, not loss:
the metric merge dedups, and a replica's `RefreshReplica` trims the head below what its parts
cover.

Lifecycle: `Create` **resumes** an existing directory (repairs the highest-numbered segment's torn
tail, then opens lazily beyond the prior run's segments), `SetEpoch` stamps new segments, `Seal` closes the current segment
and stamps the next generation, and `CheckpointThrough` deletes the segments a flush made durable
(truncate-on-flush), so replay stays bounded.

**A flush seals at detach, not at publish.** The part holds exactly the records logged when the head
was detached, and the part write then runs off the engine lock while ingest continues. Sealing there
— atomically with the detach — puts every later record in a segment beyond the sealed sequence, at
the generation past the watermark the flush is about to commit, so `CheckpointThrough` cannot delete
it and `ReplayDirFrom` cannot skip it. Checkpointing through the *current* sequence instead deletes
the segments holding acknowledged records that no part contains: silent loss under ordinary
concurrent ingest.

## Durability policy

`Options.WALDir` attaches one writer per (tenant, signal) engine. `Options.WALSync` picks the fsync
policy: `None` (default — page cache), `Always` (per record), `Interval` (background timer).

**The WAL never shares a namespace with parts.** Segments live at `{WALDir}/{tenant}/{signal}/`, and
an engine owns every key under `{tenant}/{signal}/`: its orphan sweep lists that prefix, retention
and merges delete under it, and the file backend prunes directories they leave empty. Two guards
keep the trees apart. `Open` refuses a `WALDir` equal to, inside, or containing a backend that
reports its directory (`backend.LocalDir`), both resolved through symlinks. And the tenant id `wal`
(any id whose first `/` segment is `wal`) is reserved — rejected at derivation as
`reserved_tenant`, and refused again at engine creation — so a WAL at `<root>/wal` under a backend
that cannot report its root still cannot be aliased by an engine prefix.

Segments go through `internal/vfs`, the rooted filesystem seam, so the crash model is testable
rather than argued: `faultfs` keeps only what was synced *through a synced directory*, and
distinguishes `Crash()` (power loss) from `Kill()` (process death).

The writer holds that rooted directory open for its lifetime, since every rotation and checkpoint
syncs it. `Close` is therefore terminal — it releases the directory as well as the current segment,
and a closed writer cannot open another. Rotation, sealing, and checkpointing use the internal
segment-only close instead. A leaked directory handle would be invisible on unix and fail the
crash-recovery tests on Windows, which cannot remove a directory a live process still holds.

What each policy guarantees, precisely:

| policy | process death (`Kill`) | power loss (`Crash`) |
|---|---|---|
| `None` | every acknowledged record | nothing not already synced by a rotation |
| `Interval` | every acknowledged record | every record synced by the last tick (200 ms window by default) |
| `Always` | every acknowledged record | every acknowledged record |

`Always` earns the second column only because **the directory is synced too**. An fsync commits a
segment's bytes and says nothing about the entry naming them, so a directory sync is required once
per segment (in `openNext`, before any record lands in it) and once per `CheckpointThrough` (so a
power cut cannot resurrect segments a flush already superseded and have replay re-apply them). Not
per record: the name a record needs is already durable by the time it is written.

The claim stops at the filesystem. A drive that lies about its write cache is outside what any
placement of fsync can cover.
