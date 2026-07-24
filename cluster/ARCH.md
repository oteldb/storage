# `cluster/` — L0 distribution

Optional. Single-node must work with this layer absent. Coordination is **external and minimal**:
etcd for membership and compaction claims, backend CAS for commits — no homegrown Raft.

**Exception to the "embedder owns transport" rule:** this layer ships its own node-to-node HTTP
transport (replicate, primary-write, read, enumeration, partsync endpoints). The ingest/query data
plane stays transport-free.

## `ring` — placement

Rendezvous / highest-random-weight hashing: a node's score for a key is `xxh3.HashSeed(key,
seed(nodeID))`, `Lookup(key, rf)` returns the owners (primary first). Three properties:

- **Deterministic, coordinator-free** — every node computes the same owners from the membership
  list alone, so routing needs no lookup table on the hot path.
- **Minimal movement** — an add only ever steals a replica slot *to itself*; a removal only
  redistributes *its* keys (property-tested).
- **Failure-domain spreading** — `Lookup` takes the highest-scoring node of each not-yet-used zone
  first, filling remaining slots in pure score order. With zones unset the result is exactly
  score-ordered top-`rf`, so it costs nothing until an operator sets them. `LookupBalanced`
  generalizes this to a domain **hierarchy** (`Node.Domains`, coarsest first: rack, server, node),
  minimizing shards per domain at each level — what EC needs.

The `Ring` is immutable (`With`/`Without` return a new one).

## `etcd` — membership & ownership

`Join` registers under a **lease** and watches the member prefix; each change rebuilds the ring
into an `atomic.Pointer`, so `Membership.Ring()` is a lock-free read. A crashed node drops out of
every peer's ring within the TTL. etcd distributes membership only — placement stays local.

`Ownership` is the **rebalance executor**: exclusive compaction claims via etcd CAS bound to the
node's lease. `Reconcile` is stateful and minimal-move — it tracks held shards and writes only to
acquire a wanted-unheld or release a held-unwanted shard, so steady state is **zero round-trips**;
retrying the wanted-unheld acquires each pass is what converges a handoff. It records the enacted
plan (`LastPlan`) for operator preview. In cluster mode the maintenance loop flushes/merges **only
owned shards**, so a shard's parts are written by exactly one node even during ring-disagreement
windows — the claim arbitrates.

## `replica` — quorum replication

`Replicate` fans an opaque payload to a key's owners and returns once a quorum has applied it,
erroring early when quorum becomes unreachable; non-quorum owners still receive it so replicas
converge. `ReplicateQuorum` takes an explicit ack count (the primary already holds one durable
copy, so it needs `RF/2` more). The replicator is **decoupled from the ring** — the caller maps
owners→addresses — so routing and quorum logic test against a fake transport.

## Write path — primary-authoritative

A write is framed with its tenant + signal byte and routed to the shard's **ring-primary**, the
single authority. The primary applies it via `ApplyPrimary` (the *only* OOO and admission decision
for the shard), re-frames the **accepted** set, and replicates that verbatim to secondaries
(`ApplyReplicated`, no re-check — the way WAL replay trusts the log). Every replica therefore
receives the same accepted set from one authority: replicas converge even under concurrent writers
and the reject count is exact, flowing back into `Accepted{Accepted, Rejected, RejectedReason}`.

## Read path — owner-aware fan-out

An owner serves locally with full matcher pushdown. A non-owner fans out to owners, **hedged**
(first owner immediately, a second raced once it is slow or errors — a single owner's copy is
complete). Matchers are opaque Go closures and **not serializable**, so the RPC carries the tenant
+ window and the requester **re-applies the matchers** to the returned superset (which the contract
permits). **Equality is the exception**: `fetch.Matcher` may carry a serializable `EqualMatcher`
spec, forwarded and pushed down on the peer, so a non-owner read narrows by `__name__` instead of
pulling the whole window. Enumeration RPCs (series, keys, side store, aggregate) fan out the same
hedged way.

## Sharding

`Config.ShardsPerTenant` splits a tenant into N shards; a series/stream maps to
`hash(id) % N` and the **shard** — key `{tenant}/_s{idx}` — is the ring/storage/compaction unit.
The key **collapses to the bare tenant at N=1**, so the default layout, placement and on-disk
prefixes are byte-identical to the unsharded path, and the shard key is just a tenant-like string
the existing tenant-keyed machinery handles transparently. Writes group by shard key and route per
shard; reads gather across all N and merge. Policy (retention, RF, downsampling) resolves per
**real** tenant via `tenantOfShard`.

Cross-shard reassembly is explicit: trace-by-id runs across every shard (a trace's spans scatter
across service streams), series listings concatenate, key listings union, and the profile symbol
store is unioned (content-addressed ⇒ a plain dedup).

## `rebalance`

`Plan(shards, prev, next, rf)` is a pure diff of two rings: per shard whose owner set changed, the
IDs added and removed. With a **shared** object store a reassignment is an ownership handoff (the
gainer serves the shard's parts from the store, the loser stops), not a copy. `PlanWith` honors
per-tenant RF, so the recorded plan is each shard's full owner-set diff — the replicas that must
backfill under shared-nothing.

## `partsync` — shared-nothing part replication

`Config.PrivateBackend` declares the backend per-node private (a local disk), so peers cannot read
this node's flushed parts and the cluster must replicate them node-to-node. Two read-only HTTP
endpoints serve the node's backend (key listing; one object verbatim with an xxh3 checksum header
the client verifies). A `Syncer` **mirrors an engine prefix from the newest peer copy**: fetch each
peer's bucket index, pick the newest, copy missing objects — **manifest after the part's other
objects, bucket index after everything**, so the local index only ever references fully-copied
parts (the same commit-point discipline as flush; a crashed sync leaves an orphan retried next
pass).

The engine layer is untouched: partsync moves objects, then the ordinary `RefreshReplica`/
`LoadParts` path loads them. Because the head is trimmed only below parts the engine actually
loaded, pull-before-trim can never drop an unflushed sample. A replica mirrors before each refresh;
a compaction owner backfills **strictly** newer copies only, so a stale replica can never regress
the owner's index while a newly-gained owner still adopts the previous owner's parts and sequence
watermark. Stale objects a peer no longer lists are pruned only after **two consecutive absent
passes** (quarantine-by-delay, giving in-flight readers a cycle to drain), and live-part shards are
exempt. Sync is **signal-agnostic** (it mirrors whatever lives under `{tenant}/{signal}`), so every
sidecar replicates identically. Convergence is push-accelerated by a best-effort notify after a
flush/merge; the periodic pull stays the anti-entropy source of truth, and passes are serialized
per prefix so a notify can never install an older index over a newer one.

## `ec` — erasure coding

Per-tenant policy (`tenant.Durability.EC`, an age tier like recompression, so recent data stays
full-copy for fast reads). Systematic Reed-Solomon behind a small surface, plus a per-part `Meta`
sidecar (scheme, per-object sizes, per-shard xxh3 checksums; fuzzed + golden-tested).

**Layout is fixed:** shard slot *i* lives at `{partPrefix}/ecshard/{i}/{object}` on ring-owner *i*
(owner count is exactly Data+Parity — the tenant's RF is ignored under EC), the sidecar on every
owner, and objects under a small floor stay full-copy everywhere (k+m shards of a tiny object cost
more than they save). Slot placement uses `LookupBalanced`, so shards spread across the
rack/server/disk hierarchy; a scheme is rack-safe with at least `ceil(Shards/Parity)` racks.

- **Read is transparent**: an EC tenant's engine is built over an `ecBackend` wrapper, so every
  part-object read hits it — a surviving full copy is a zero-copy view, a converted object is
  reconstructed from valid Data shards (own slot locally, the rest from slot-owning peers).
  Writes/list/delete pass through, so flush, partsync and the converter see the plain layout.
- **Convert** runs on cold parts from the compaction owner's maintenance branch: shard every
  at-or-above-floor object, write the sidecar **as the commit point**, delete the full copies.
  Crash-safe at every step (before the sidecar ⇒ readable full-copy part; mid-delete ⇒ still
  readable; re-run ⇒ only sweeps leftovers).
- **Slot filtering** — a replica mirrors only **its own slot** plus non-shard objects, so each node
  converges to one shard per part. Since EC rewrites objects without changing the bucket index, the
  filtered pull reconciles by **object presence**, not index generation.
- **Owner prune** — the owner stages every shard on conversion (the distribution source) and
  deletes foreign slots only after confirming each peer holds its own, so the last copy of a slot
  is never dropped; skipped when the ring is smaller than Data+Parity.
- **Repair** — a missing own slot is rebuilt by gathering **by content** (list each owner's
  `ecshard/` objects: balanced placement can renumber slots, so position is not identity),
  reconstructing and writing it checksum-verified. A node loss removes an owner but never pushes
  another out, so survivors always hold ≥ Data shards — reads work even before repair runs.
- **Gained-owner bootstrap** — a spare promoted into an owner set has no engine, and the
  maintenance loop iterates local engines, so it would never look at the tenant. Each cycle first
  discovers such shards via one etcd range read over the compaction claims (a live shard has a
  claiming owner), mirrors from peers, then creates the engine by the same backend-driven prefix
  discovery startup recovery uses. A shard whose every owner died has no claim and is not
  discoverable.
