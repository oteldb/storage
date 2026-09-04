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
loud in both directions — an older directory fails `ErrCorrupt` on its first frame at startup, a
mismatched replication payload is rejected rather than half-applied — and both halves need a
coordinated rollout. Carrying a discriminator in the segment name would version only the disk half
and leave the wire half broken anyway, which is why the format is not versioned here.

## Where a torn record is tolerated, and where it is not

Exactly one place may end mid-record: the **last segment of a WAL directory**, which is the one a
crash was appending to. `ReplayDirFrom` tolerates it there and nowhere else — a short stop in an
earlier segment means the rest of that segment, and the hole it leaves in history, would be skipped
while the segments after it papered over the gap. That is silent loss bounded only by the segment
size, so it is an `ErrCorrupt` naming the segment and the offset.

`Replay` itself is therefore **strict**: it is handed a complete log — a whole segment, or a
replication payload from `ApplyPrimary` — so a record that does not fit inside the buffer is
truncation, not an end of stream. A truncated replication payload used to decode as a short batch
and let a replica diverge from its primary in silence; it is now an error. The records applied
before the stopping point are kept either way.

Making the tolerance last-segment-only is only safe because `Create` **repairs on resume**: it
truncates the highest-numbered segment to its last complete frame before opening the next one
(Prometheus `wal.Repair` semantics). Without that, one ordinary crash leaves a torn tail that the
next run turns into a permanent *middle* segment, and every later replay fails. The discarded bytes
are an incomplete frame, unreadable by construction. A *complete* frame that fails its CRC is left
alone — that is corruption rather than a torn append, and replay is the one place that reports it.

### Proving a tail is a tail

Ending mid-record is not by itself evidence of a torn append. A crash does not always truncate to a
clean prefix: per-block writeback can land a later block while an earlier one is lost, and the
filesystem zero-fills the gap; a preallocated tail reads back as zeros for the same reason. The
result is a **hole** — a zero region followed by frames that did reach the platter — and the reader
stops at the zeros, mistaking committed records for the end of the log.

So both places that accept a torn tail (`ReplayDirFrom` on the last segment, `repair` before it
truncates) first scan forward from the stopping point for a complete, CRC-valid frame. One found
means whole records survive past the gap: the segment is `ErrCorrupt`, and `repair` refuses to
truncate rather than destroying both the records and the evidence. None found means an ordinary torn
append, accepted as before.

The scan is byte-aligned, since a hole destroys frame alignment and the resume point can be any
offset. Cost is bounded two ways: candidates whose length varint is zero or overruns the buffer are
rejected without a checksum (a zero-fill is one pass over the gap), and the checksummed bytes carry a
1 GiB budget — roughly 0.3 s — after which the scan gives up and reports a tail. False positives cost
2^-32 per candidate offset, so even a maximal 32 MiB region misclassifies a healthy log with
probability ~0.8%; payloads are engine blobs, never nested WAL frames, so nothing biases that.

Pebble instead carries a durability watermark (`SyncOffset`) in every chunk header and proves
corruption when a later chunk promises durability past the bad offset. That is exact, and costs a
format major version and a second reader; a surviving CRC-valid frame is evidence enough here and
needs no format change. The residual: a hole whose trailing region happens to hold no CRC-valid frame
— a gap that swallowed every following record, or one whose survivors are themselves torn — is still
indistinguishable from a tail and is accepted as one.

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
| `Always` | every acknowledged record | every acknowledged record |

`Always` earns the second column only because **the directory is synced too**. An fsync commits a
segment's bytes and says nothing about the entry naming them, so a directory sync is required once
per segment (in `openNext`, before any record lands in it) and once per `CheckpointThrough` (so a
power cut cannot resurrect segments a flush already superseded and have replay re-apply them). Not
per record: the name a record needs is already durable by the time it is written.

The claim stops at the filesystem. A drive that lies about its write cache is outside what any
placement of fsync can cover.
