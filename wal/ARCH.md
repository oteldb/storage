# `wal/` — write-ahead log

CRC-framed records (`[uvarint len][type][payload][CRC32C]`) appended to numbered segment files,
rotating at a size limit. Replaying a log rebuilds the symbols+series+postings index and the head —
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

Replay tolerates a **torn final record** (crash recovery), surfaces a bad-CRC *complete* record as
corruption, and stitches segments in order.

## Epochs — exactly-once recovery (record signals)

Segments are named `{seq}-{epoch}.wal`: `seq` orders replay, `epoch` is the flush generation, so a
segment self-describes which generation it holds. The watermark of the last-flushed epoch lives in
the **bucket index**, so it advances *atomically with part discoverability* — the very object
`recover` reads. `ReplayDirFrom(minEpoch, …)` skips segments at or below it, so even a crash in the
window between a part committing and its WAL being deleted re-applies nothing.
(Metrics don't track the epoch — their merge dedup makes that window self-healing.)

Lifecycle: `Create` **resumes** an existing directory (opens lazily beyond the prior run's
segments, never truncating them), `SetEpoch` stamps new segments, `Checkpoint` deletes the segments
a flush made durable (truncate-on-flush), so replay stays bounded.

## Durability policy

`Options.WALDir` attaches one writer per (tenant, signal) engine. `Options.WALSync` picks the fsync
policy: `None` (default — page cache, process-crash safe), `Always` (per record, power-loss safe),
`Interval` (background timer).
