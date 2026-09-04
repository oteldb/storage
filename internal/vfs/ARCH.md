# `internal/vfs` — the filesystem seam and its crash model

`vfs.FS` is the lowest seam in the tree: a rooted directory, plus `SyncDir` in the contract. It
exists because durability is a property of syscall *order*, and no seam above it can express that —
`backend.Backend` sees whole objects appear atomically, so a fault injected there cannot describe a
file whose bytes survived a power cut while the name reaching them did not.

Two implementations answer the same conformance suite (`vfstest.Conformance`), so the fake cannot
drift from the real directory it stands in for:

- `vfs.Open`/`vfs.OpenRoot` — an `os.Root` with a real directory fsync (`syncdir_unix.go`; a no-op
  elsewhere).
- `faultfs` — in memory, with a durability model and rule-driven fault injection.

## What `faultfs` tracks

Every file holds two byte slices: `data`, what a reader sees now, and `synced`, what its last
`File.Sync` committed. The namespace is tracked separately — `durable`/`durableDirs` hold the names
a power cut would still resolve, and `links`/`unlinks` hold the name changes waiting for the
directory sync that publishes them. A durability bug is exactly a divergence between the two halves,
which is why they are separate maps rather than one.

## The crash modes

| mode | models | keeps |
|---|---|---|
| `Kill()` | the process died — panic, SIGKILL, OOM — machine still up | everything; the page cache is the kernel's |
| `Crash()` | power loss, worst case POSIX permits | only bytes synced through a synced directory |
| `CrashWith(cfg)` | power loss with writeback partly done | the above, plus a per-unit random draw over what was unsynced |
| `Tear(name, keep)` | a chosen prefix of one file's uncommitted tail reached the platter | scripted, not drawn |

`Crash()` is `CrashWith(CrashConfig{})` and stays the default: deterministic, and the worst legal
outcome, so code that survives it survives any device.

### What the randomized draw models

`CrashConfig.UnsyncedPercent` is the chance one unsynced unit had already been written back.
`CrashConfig.Seed` fixes every draw.

- **Per-block survival.** Writeback is per 4 KiB page, not per file, so each block of the unsynced
  tail is drawn on its own. The result is not a prefix.
- **Zero-filled holes.** If block *n* landed and block *n−1* did not, the file comes back *longer*
  than its synced length with the lost block reading as zeros. This is the state `Crash()` cannot
  reach — losing a prefix is all it can do — and the one that catches a log replayer treating a run
  of zeros as the end of the log.
- **Per-entry survival.** An unsynced create, rename or unlink is drawn independently of the bytes
  it names, so "the name landed and the tail did not" is a state the tests can reach.

Draws only ever *add* to what was synced: no mode takes back a committed byte. So the space between
`Crash()` (nothing extra) and `Kill()` (everything) is what a seed sweep explores, and every point
in it is legal.

### Seeding

Each decision comes from its own generator, keyed by `(seed, domain, path)` — `domain` separating a
path's entry decision from its byte decisions — rather than from one sequential stream. The clone
walks Go maps, and map order changes on every range: a single stream would let ordering pick which
draw a file got, and a seed would reproduce nothing. Path keying also holds a file's outcome fixed
when an unrelated file joins the test, so a reproducer keeps reproducing.

Pebble's `errorfs` keys per-file generators the same way for a different reason (concurrent access
to other files). That reason does not apply here — the clone runs once under the filesystem lock —
so determinism alone is the justification.

Tests take a fixed seed so the suite never flakes, and log it on failure alongside
`OTELDB_STORAGE_CRASH_SEED`, which overrides it for replay or for a sweep.

## What it does not model

- **Reordering or tearing inside a block.** A 4 KiB block lands whole or not at all. Real devices
  can tear at a smaller sector granularity; `Tear` is the scripted stand-in when a test needs a
  partial record, and it works on the file's tail, not on an interior block.
- **Reordering across files.** There is one draw per unit, no notion of a write cache flushing in a
  different order than the writes were issued.
- **Truncation.** Files are treated as append-only. `O_TRUNC` resets the live bytes without shrinking
  what was already synced, so a crash after a truncate returns the longer, older content.
- **Metadata durability.** Permissions and mod times are not part of the crash model: a crashed
  clone gives every file `0600` and every directory `0750`.
- **Directory reachability of published names.** `SyncDir` publishes the names created *in that
  directory*, so syncing `a/b` can make `a/b/c` durable while `a` itself is not. Files are still
  filtered to those whose parent survived, but the directory set itself is not closed upward.
- **Partial or failing syncs.** `Sync` either commits everything written so far or fails outright via
  a `Rule`; it never commits some of it.

## Fault injection

A `Rule` matches an `Op` (and optionally the `Call`), then fails it with `Err` or suspends it in
`Before`. `Gate` is the ready-made `Before` for holding an operation until a test releases it, so an
interleaving is stated rather than raced for. `Calls()` returns the operation log for asserting that
a `SyncDir` actually happened.
