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

Registration is **maintained, not one-shot**. A stall longer than the TTL loses the lease and etcd
deletes the member key, which is indistinguishable from a crash to every peer — so a live node
watches for its own disappearance (the keep-alive channel closing, or its own id in a `DELETE`
event, which is a contradiction) and re-registers under a fresh lease, with backoff. Compaction
claims hang off that lease, so `Membership.OnRejoin` rebinds `Ownership` to the new one
(`SetLease`, which also drops the held set the dead lease took with it).

The **watch** is maintained the same way. etcd cancels a watch of its own accord — a compacted
start revision above all, which is what an outage longer than the compaction interval leaves —
and clientv3 reconnects a broken stream but does not resubscribe a canceled one. A cancellation
is answered with a fresh snapshot (taken whole: a stream that ended left an unknown number of
changes unseen) and a new watch from its revision, with backoff; otherwise the member set would
freeze at its last view and never resynchronize, silently routing to peers that had left. The
snapshot doubles as a second eviction check, since a key deleted while nothing was watching
produces no `DELETE` for anyone to see. This applies equally to `Watch` observers, whose only
membership input is that stream.

An absent node **keeps serving reads**: it still holds its shards, and a secondary's head is
memory-only, so restarting it would trade a routing problem for lost writes and a read gap on every
node at once. The state is reported instead — `ClusterStats.SelfAbsent`/`Rejoins`, the
`storage.cluster.self_absent` gauge, and a warning log — while the routing tier's readiness gate
reports the routing side.

`Ownership` is the **rebalance executor**: exclusive compaction claims via etcd CAS bound to the
node's lease. `Reconcile` is stateful and minimal-move — it tracks held shards and writes only to
acquire a wanted-unheld or release a held-unwanted shard, so steady state is **zero round-trips**;
retrying the wanted-unheld acquires each pass is what converges a handoff. It records the enacted
plan (`LastPlan`) for operator preview. In cluster mode the maintenance loop flushes/merges **only
owned shards**, so a shard's parts are written by exactly one node even during ring-disagreement
windows — the claim arbitrates.

### Lease fencing — the boundary on acting as primary

A node that stops renewing its lease keeps a ring frozen at its last etcd view, so it goes on
resolving itself as the primary of shards etcd has already handed elsewhere. Its `held` set is
local and only cleared on rejoin, so the belief outlives the fact by exactly the window that
matters. Ordering survives this — a takeover acquires at a higher etcd revision, so a displaced
writer's indexes never supersede — but an acknowledged write does not: it is replicated to owners
that are no longer in the real ring, and read by nobody.

So a claim expires **locally, on a deadline the node computes for itself**:
`Membership.FenceDeadline()` is the last keep-alive etcd answered plus the TTL, less `FenceMargin`
(clock error plus the delay in noticing; 5s against the 30s `DefaultTTL`, clamped to `ttl/3` so a
short-TTL cluster is not fenced permanently). Past it `Membership.Fenced()` is true and
`Ownership` disclaims everything: `Term` reports not-held, `Owned` is empty, `Reconcile` is a
no-op. The `held` set itself is kept, so a lease confirmed again — the same lease, no rejoin —
resumes the claims in place.

The gate is deliberately **not "can I reach etcd"**. A node that cannot reach etcd but is still
inside a live lease is not wrong to serve as primary; a node whose lease has lapsed is wrong
whether or not etcd is reachable. The cost is stated plainly: an etcd outage longer than the TTL
does stop ingest cluster-wide, because no node can distinguish "etcd is down for everyone" from
"I am the one partitioned" — and in the second case someone has already taken over. A blip shorter
than the TTL costs nothing.

Fencing suppresses the **primary role only**:

- **Writes** — `primaryWrite` refuses with `cluster.ErrNotPrimary` (HTTP 409, like a read's
  `ErrShardAbsent`, so the origin can tell "ask someone else" from "this node is broken"). The
  write fails at its origin instead of being acknowledged and then withheld.
- **Replicated writes** are still applied: they come from a primary that *can* prove its claim,
  and that primary is the authority.
- **Reads** stay served — they have their own disclaim path (`canAnswer`), and a stale read is a
  lesser fault than a lost write.
- **The unflushed head is kept**, neither dropped nor flushed. Dropping loses writes that were
  properly replicated when they were acked; flushing writes parts under a tenure that has ended,
  which the shard's new owner never tombstones. It flushes when the node can prove the shard is
  its own again — which follows from `Reconcile` being a no-op while fenced, since the maintenance
  loop only flushes owned shards.

The residual is bounded and named: writes acked between the instant the lease was actually lost and
the deadline. That is a parameter (`FenceMargin`), not "until someone notices".

## `replica` — quorum replication

`Replicate` fans an opaque payload to a key's owners and returns once a quorum has applied it,
erroring early when quorum becomes unreachable; non-quorum owners still receive it so replicas
converge. `ReplicateQuorum` takes an explicit ack count (the primary already holds one durable
copy, so it needs `RF/2` more). The replicator is **decoupled from the ring** — the caller maps
owners→addresses — so routing and quorum logic test against a fake transport.

## Write path — primary-authoritative

A write is framed with its tenant + signal byte and routed to the shard's **ring-primary**, the
single authority. The primary first checks it can still prove the shard is its own (see *Lease
fencing*) and otherwise refuses with `ErrNotPrimary`; a write it cannot place must fail, not
succeed and vanish. It then applies it via `ApplyPrimary` (the *only* OOO and admission decision
for the shard), re-frames the **accepted** set, and replicates that verbatim to secondaries
(`ApplyReplicated`, no re-check — the way WAL replay trusts the log). Every replica therefore
receives the same accepted set from one authority: replicas converge even under concurrent writers
and the reject count is exact, flowing back into `Accepted{Accepted, Rejected, RejectedReason}`.

The primary also **logs** the accepted set to its own WAL, which is what the quorum's "one durable
copy at the primary" rests on: a restart replays its unflushed head instead of coming back as a ring
owner that answers with everything since its last flush missing. So a clustered node on a durable
backend requires `WALDir` — `Open` refuses the combination. Secondaries hold the head in memory only
(they neither flush nor checkpoint, so a log there would grow without bound); a secondary that
restarts catches up from the shard's parts.

## Read path — owner-aware fan-out

An owner serves locally with full matcher pushdown. A non-owner fans out to owners, **hedged**
(first owner immediately, a second raced once it is slow or errors — a single owner's copy is
complete).

Ownership alone is not enough to serve: the ring and the data can disagree — a node promoted into
the owner set by a rebalance holds nothing until it backfills, and a node whose membership view lags
the writer's derives a different owner set entirely. So a read resolves to a **holder**: a node that
owns a shard but has no engine for it routes to the shard's other owners instead of serving its own
empty copy, and a peer asked for a shard it does not hold answers `ErrShardAbsent` (HTTP 409, not
404 — an unknown endpoint must stay distinguishable) rather than an empty success. An absent answer
is a failover, not a result; only when *every* owner disclaims the shard does the read report empty.
Both the local skip and the all-owners-absent case are metered (`storage.rpc.shard_absent`) and
logged, since either means committed data is temporarily unreachable from this node.

**Holding a shard is not the same as being able to answer for it.** A node that restarts without
recovering the shard's head — a secondary's head is never logged, so a restart always loses it — or
one a rebalance just handed the shard to, comes back with the flushed parts alone. It is a ring
owner, and one owner's answer is taken as complete, so serving it returns a hole nothing can see.
Such an engine carries a **read gap**: it disclaims (the same `ErrShardAbsent` failover) any query
reaching at or past the newest timestamp its parts held when it came back, and serves everything
older locally. The bound is in the data's own time domain, not the wall clock, so it holds for
backfilled ingest; it is inclusive because the lost head may hold more rows at that same timestamp.
The gap closes when the parts advance past it, which only a flush by the shard's compaction owner
does — a merge preserves its inputs' time range and so cannot close it by accident. Metered as
`storage.rpc.shard_incomplete` and reported per shard in `Inspect` (`ADMIN.md`). The cost is
conservative in one direction only: a node whose head was genuinely empty still fails over for
recent windows until the next flush.

Matchers are opaque Go closures and **not serializable**, so the RPC carries the tenant
+ window and the requester **re-applies the matchers** to the returned superset (which the contract
permits). Re-applying is `fetch.Filter` / `fetch.MatchesSeries` — one implementation on the seam,
shared by the node's fan-out and by `router`, since a superset every consumer must narrow the same
way is a shared obligation, not a per-consumer one. **Equality is the exception**: `fetch.Matcher` may carry a serializable `EqualMatcher`
spec, forwarded and pushed down on the peer, so a non-owner read narrows by `__name__` instead of
pulling the whole window. Enumeration RPCs (series, keys, side store, aggregate) fan out the same
hedged way. The series RPC is **signal-dispatched**: one endpoint enumerates stream identities for
logs/traces/profiles and metric series alike, so the metrics label endpoints answer from identities
in cluster mode too, and the read seam re-exposes that gather as the `fetch.SeriesLister` capability
the shard merge underneath it cannot provide.

The metric **aggregate pushdown** has two endpoints: `/internal/aggregate` returns disjoint step
buckets, `/internal/aggregate/window` the overlapping evaluation windows of a range vector. Both
ship one compact entry per series — identity + aggregates, never raw samples — which the coordinator
re-filters against the full matcher set and unions, merging by bucket start / evaluation timestamp
where a series surfaces from more than one shard (exact, since shards hold disjoint samples). They
are deliberately **separate paths** rather than one widened request: a peer that predates windows
answers 404, which fails over, instead of silently returning disjoint buckets for an overlapping
question.

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
this node's flushed parts and the cluster must replicate them node-to-node. Leaving it unset on a
private backend is silent: writes still reach every replica through the primary path, so the
cluster looks healthy until a handoff or a replica restart serves a shard whose parts live only on
another node's disk — absence the read path cannot distinguish from real absence. `Open` warns when
the backend reports `backend.NodeLocal` and this flag is unset, and `Inspect` keeps that visible as
`ClusterStats.NodeLocalBackendUnshared`; both are advisory, because a `file` backend on a shared
mount is a legitimate shared store that trips the same check. Two read-only HTTP
endpoints serve the node's backend (key listing; one object verbatim with an xxh3 checksum header
the client verifies). A `Syncer` **mirrors an engine prefix from the newest peer copy**: fetch each
peer's bucket index, pick the newest, copy missing objects — **manifest after the part's other
objects, bucket index after everything**, so the local index only ever references fully-copied
parts (the same commit-point discipline as flush; a crashed sync leaves an orphan retried next
pass).

**Absence is not an instruction.** Mirroring a peer, obeying its deletions, and deleting a
particular part are three separate claims. Only an index that *supersedes* the local one may do the
second, and only a part the peer says it **removed** may be deleted at all. Ordering comes from the bucket
index's commit generation — a `(term, counter)` pair the writing engine stamps on every index write,
where the term is the etcd revision the writer's compaction claim was created at
(`Ownership.Acquire`). Neither the part names nor `FlushedEpoch` can play that role: both are
high-water marks of *creation*, so a rewrite that only removes parts moves neither, and a shrink is
indistinguishable from a silent loss — an owner with bit rot, a partial `rm`, or an index rolled
back by a snapshot still holds the newest part. A peer that does not supersede is still copied from
(additive, always safe) but its index is not installed and its absences prune nothing; the
protection set is then the union of both indexes, and because the local index is kept, the part goes
on being protected on every later pass rather than only the one that noticed. The term is what keeps
this live as well as safe: a purely local counter would leave a node restored from an old snapshot
permanently unable to supersede its own replicas, where reacquiring the shard raises its term above
the tenure that intervened. An index predating format v3 carries no generation, sorts below every
one that does, and falls back to the old `maxSeq`/`FlushedEpoch` ranking only when neither side has
one.

Supersession alone is not enough, because a damaged owner goes on writing: it reloads from what it
has left and flushes again, and those writes legitimately supersede while legitimately not naming
the part it lost. So the index also carries **tombstones** — each removed part and the generation
that removed it — recorded by the engine from the diff against the index it last wrote. Pruning
requires one: a part absent from a peer's index that the peer never claimed to have removed is a
peer missing data, and its objects are withheld and counted (`Stats.Withheld`, a repair signal that
is zero in steady state) rather than deleted. This is the shape Mimir gets from `deletion-mark.json`
and ClickHouse from an explicit `DROP_RANGE`; with no shared bucket to hold it, the statement rides
in the index. Tombstones are bounded (`bucketindex.MaxRemovals`, newest kept), so a replica further
behind than that keeps garbage instead of guessing, and a legacy index states no removals at all,
where absence is all there is and the pre-tombstone behavior stands.

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
