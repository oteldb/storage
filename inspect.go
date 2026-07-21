package storage

import (
	"sort"

	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/signal"
)

// StoreStats is a point-in-time, in-memory snapshot of store state for an embedder's CLI/UI
// dashboard (and for debugging). It is pull-based and does **no backend I/O and no column decode**,
// so it is safe to poll at dashboard cadence — each call takes only a brief per-engine read lock to
// copy counters, never touching the ingest/query hot path. On-disk part byte sizes are deliberately
// omitted (they would require backend stat calls); this is an in-memory view of head, part counts,
// time spans, admission, and cluster state.
type StoreStats struct {
	// Tenants is one entry per tenant that has any engine, sorted by tenant id.
	Tenants []TenantStats
	// Cluster is the cluster-mode view (membership, owned shards, last rebalance); nil single-node.
	Cluster *ClusterStats
	// Caches aggregates the read-path caches.
	Caches CacheStats
}

// TenantStats is one tenant's per-signal breakdown plus its (cross-signal) admission counters.
type TenantStats struct {
	Tenant signal.TenantID
	// Admission is the tenant's cumulative admission tally (shared across signals — the valves are
	// keyed by tenant, not signal).
	Admission AdmissionStats
	// Signals is one entry per signal the tenant has ingested, sorted by signal.
	Signals []SignalStats
}

// SignalStats is one (tenant, signal) engine's in-memory shape.
type SignalStats struct {
	Signal signal.Signal
	// Series is the distinct series/streams ever seen (index span: head ∪ flushed parts).
	Series int64
	// HeadItems is the samples (metrics) or records (logs/traces/profiles) buffered unflushed.
	HeadItems int64
	// HeadBytes is the head's buffered bytes — the in-flight memory measure a flush drains.
	HeadBytes int64
	// Parts is the number of flushed immutable parts (the compaction-backlog proxy: many small
	// parts means merge is behind).
	Parts int
	// MinTimeUnixNano / MaxTimeUnixNano bound the data this engine holds (0 when empty); MinTime is
	// over flushed parts, MaxTime includes the head's newest.
	MinTimeUnixNano int64
	MaxTimeUnixNano int64
	// MergeRunning is true while a compaction/merge is executing on this engine.
	MergeRunning bool
	// MergeBacklog is the count of flushed parts pending compaction — the backlog proxy (currently
	// equal to Parts; a separate field so a dashboard can label it as the merge backlog).
	MergeBacklog int
	// WAL is true when this engine has a write-ahead log (false for the ephemeral in-memory engine);
	// the WAL* fields below are meaningful only when it is true.
	WAL bool
	// WALSegments is the WAL's current segment sequence number (segments opened so far).
	WALSegments int
	// WALBytes is the byte size of the WAL's currently-open segment.
	WALBytes int64
	// WALEpoch is the WAL's active flush generation (the epoch stamped onto new segments). The same
	// generation across both engine families; not the recovery watermark.
	WALEpoch uint64
}

// ClusterStats is the cluster-mode view of this node.
type ClusterStats struct {
	// Self is this node's address.
	Self string
	// Members is the live ring membership (sorted by id).
	Members []MemberStats
	// Owned is the shards this node currently holds a compaction claim on (sorted).
	Owned []string
	// LastRebalance is the primary handoffs enacted at the most recent ring change (empty if none).
	LastRebalance []RebalanceMove
	// PartSync is the shared-nothing part-mirroring activity (cluster/partsync), cumulative since
	// process start; nil unless the cluster runs with a private (per-node) backend.
	PartSync *PartSyncStats
}

// PartSyncStats is the cumulative shared-nothing part-mirroring activity of this node: how the
// flushed parts of shards it replicates (or gained) arrived over the parts endpoints. Counters
// only (a copy of the syncer's totals — no backend I/O).
type PartSyncStats struct {
	// Passes is every sync attempt, including no-ops — the "is the sync loop running?" probe.
	Passes int64
	// Mirrored is the passes that installed a newer peer copy.
	Mirrored int64
	// Copied is the objects fetched from peers; CopiedBytes their total size.
	Copied      int64
	CopiedBytes int64
	// Pruned is the stale local objects deleted after the quarantine delay.
	Pruned int64
	// Errors is the passes that failed part-way (retried next maintenance tick).
	Errors int64
	// LastSyncUnixNano is when the last mirroring pass completed (zero until one has) — the
	// replication-staleness probe.
	LastSyncUnixNano int64
}

// MemberStats is one cluster member's identity.
type MemberStats struct {
	ID   string
	Zone string
	Addr string
}

// RebalanceMove is one shard's primary handoff at a ring change: the node ids that gained it and
// those that lost it.
type RebalanceMove struct {
	Shard   string
	Added   []string
	Removed []string
}

// CacheStats aggregates the read-path caches.
type CacheStats struct {
	// Decode is the decoded-column cache, summed across metric engines (zero when unconfigured).
	Decode engine.DecodeCacheStats
}

// Inspect returns an in-memory snapshot of store state for a dashboard/CLI. It does no backend I/O
// and decodes nothing; it takes a brief read lock per engine to copy counters. Poll it at dashboard
// cadence (seconds), not per request.
func (s *Storage) Inspect() StoreStats {
	byTenant := make(map[signal.TenantID]*TenantStats)

	tenantStats := func(tid signal.TenantID) *TenantStats {
		ts, ok := byTenant[tid]
		if !ok {
			ts = &TenantStats{Tenant: tid}
			byTenant[tid] = ts
		}

		return ts
	}

	// Metrics.
	var decode engine.DecodeCacheStats

	for tid, eng := range s.engineSnapshotByTenant() {
		es := eng.Stats()
		segs, walBytes, epoch, hasWAL := eng.WALState()
		ts := tenantStats(tid)
		ts.Signals = append(ts.Signals, SignalStats{
			Signal: signal.Metric, Series: es.Series, HeadItems: es.HeadSamples, HeadBytes: es.HeadBytes,
			Parts: es.Parts, MinTimeUnixNano: es.MinTime, MaxTimeUnixNano: es.MaxTime,
			MergeRunning: eng.MergeRunning(), MergeBacklog: es.Parts,
			WAL: hasWAL, WALSegments: segs, WALBytes: walBytes, WALEpoch: epoch,
		})

		if dc, ok := eng.DecodeCacheStats(); ok {
			decode.Hits += dc.Hits
			decode.Misses += dc.Misses
			decode.Bytes += dc.Bytes
			decode.Items += dc.Items
		}
	}

	// Record signals (logs, traces, profiles) share the same shape via recordengine.Stats.
	addRecord := func(sig signal.Signal, engines map[signal.TenantID]*recordengine.Engine) {
		for tid, eng := range engines {
			es := eng.Stats()
			segs, walBytes, epoch, hasWAL := eng.WALState()
			ts := tenantStats(tid)
			ts.Signals = append(ts.Signals, SignalStats{
				Signal: sig, Series: es.Streams, HeadItems: es.HeadRecords, HeadBytes: es.HeadBytes,
				Parts: es.Parts, MinTimeUnixNano: es.MinTime, MaxTimeUnixNano: es.MaxTime,
				MergeRunning: eng.MergeRunning(), MergeBacklog: es.Parts,
				WAL: hasWAL, WALSegments: segs, WALBytes: walBytes, WALEpoch: epoch,
			})
		}
	}

	addRecord(signal.Log, s.logEngineSnapshotByTenant())
	addRecord(signal.Trace, s.traceEngineSnapshotByTenant())
	addRecord(signal.Profile, s.profileEngineSnapshotByTenant())

	// Attach per-tenant admission and order each tenant's signals deterministically.
	out := StoreStats{Caches: CacheStats{Decode: decode}}

	for tid, ts := range byTenant {
		ts.Admission = s.AdmissionStats(tid)
		sort.Slice(ts.Signals, func(i, j int) bool { return ts.Signals[i].Signal < ts.Signals[j].Signal })
		out.Tenants = append(out.Tenants, *ts)
	}

	sort.Slice(out.Tenants, func(i, j int) bool { return out.Tenants[i].Tenant < out.Tenants[j].Tenant })

	out.Cluster = s.clusterStats()

	return out
}

// clusterStats builds the cluster-mode section of [StoreStats], or nil in single-node mode.
func (s *Storage) clusterStats() *ClusterStats {
	if s.cluster == nil {
		return nil
	}

	cs := &ClusterStats{Self: s.cluster.self, Owned: s.cluster.ownership.Owned()}

	for _, m := range s.cluster.membership.Members() {
		cs.Members = append(cs.Members, MemberStats{ID: m.ID, Zone: m.Zone, Addr: m.Addr})
	}

	sort.Slice(cs.Members, func(i, j int) bool { return cs.Members[i].ID < cs.Members[j].ID })

	for _, r := range s.cluster.ownership.LastPlan() {
		cs.LastRebalance = append(cs.LastRebalance, RebalanceMove{Shard: r.Shard, Added: r.Added, Removed: r.Removed})
	}

	if s.cluster.private {
		t := s.cluster.psync.Totals()
		cs.PartSync = &PartSyncStats{
			Passes: t.Passes, Mirrored: t.Mirrored,
			Copied: t.Copied, CopiedBytes: t.CopiedBytes,
			Pruned: t.Pruned, Errors: t.Errors,
			LastSyncUnixNano: t.LastSyncUnixNano,
		}
	}

	return cs
}
