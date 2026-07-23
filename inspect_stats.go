package storage

import "sync/atomic"

// ecCounters is the cumulative erasure-coding activity of this node, updated by the EC
// maintenance/read paths and snapshotted (without I/O) into [ECStats] by Inspect.
type ecCounters struct {
	converted      atomic.Int64 // cold parts erasure-coded by this node (as compaction owner)
	convertErrors  atomic.Int64 // failed conversion attempts (retried next tick)
	repairedSlots  atomic.Int64 // shard slots rebuilt from surviving shards
	repairErrors   atomic.Int64 // failed repair attempts (retried next tick)
	prunedStaged   atomic.Int64 // parts whose staged foreign shards were pruned after distribution
	reconstructs   atomic.Int64 // read-path object reconstructions (a full copy was absent)
	reconstructErr atomic.Int64 // failed read-path reconstructions (surfaced to the query)
}

// maintCounters is the cumulative maintenance-loop activity, snapshotted into
// [MaintenanceStats] by Inspect.
type maintCounters struct {
	cycles           atomic.Int64 // completed maintenance cycles
	lastStartNano    atomic.Int64 // wall-clock start of the most recent cycle
	lastDurationNano atomic.Int64 // duration of the most recent completed cycle
	lastTasks        atomic.Int64 // engine tasks (flush/merge or replica refresh) in that cycle
	pressureFlushes  atomic.Int64 // engines flushed by the head-size trigger, not the interval
}

// ECStats is the cumulative erasure-coding activity of this node since process start: the
// converter, the shard repairer, the staged-shard prune, and the read-path reconstruction.
// Counters only — no backend I/O. All zeros on a node whose tenants have no EC policy.
type ECStats struct {
	// Converted is the cold parts this node erasure-coded (as their compaction owner);
	// ConvertErrors the failed attempts (each retried on a later maintenance tick).
	Converted     int64
	ConvertErrors int64
	// RepairedSlots is the shard slots rebuilt from surviving shards (after a disk loss or a
	// slot reassignment); RepairErrors the failed attempts.
	RepairedSlots int64
	RepairErrors  int64
	// PrunedStagedParts is the parts whose staged foreign shards this node (as owner) pruned
	// after confirming distribution — each prune is what converges a part to one shard per node.
	PrunedStagedParts int64
	// Reconstructs is the read-path object reconstructions (no local full copy, gathered from
	// shards); ReconstructErrors the reconstructions that failed and surfaced to the query. A
	// high Reconstructs rate with a cold-read-heavy workload is expected; growing errors are not.
	Reconstructs      int64
	ReconstructErrors int64
}

// MaintenanceStats describes the background maintenance loop: liveness (is it cycling?),
// recency, and the size of the most recent cycle. Counters only — no backend I/O.
type MaintenanceStats struct {
	// Cycles is the completed maintenance cycles since process start (the loop-liveness probe).
	Cycles int64
	// LastCycleStartUnixNano is when the most recent cycle began; LastCycleDurationNano how
	// long the most recent completed cycle took (a growing duration means compaction is
	// falling behind the ingest rate).
	LastCycleStartUnixNano int64
	LastCycleDurationNano  int64
	// LastCycleTasks is the engine tasks (per tenant, per signal: flush+merge on owners,
	// refresh on replicas) dispatched in the most recent cycle.
	LastCycleTasks int64
	// PressureFlushes is the engines flushed by the head-size trigger
	// ([Options.FlushThresholdBytes]) rather than by the interval, since process start. A rising
	// count means ingestion is filling heads faster than the flush cadence drains them.
	PressureFlushes int64
}
