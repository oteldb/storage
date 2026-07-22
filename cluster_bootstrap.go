package storage

import (
	"context"

	"go.uber.org/zap"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/signal"
)

// bootstrapGainedTenants brings a node that was promoted into a shard's owner set — without
// ever having held the shard (a spare) — up to serving state. Engines are normally created by
// the write path (the head reaches every owner) or by startup recovery, so a promoted spare
// has neither an engine nor data, and the maintenance loop (which iterates local engines)
// would never even look at the tenant. This closes that loop:
//
//  1. Discover the cluster's shards from the etcd compaction claims (one range read — any live
//     shard has a claiming owner), and keep the ones this node owns per the ring but has no
//     local metric/record engine for.
//  2. In shared-nothing mode, mirror each such shard's data from its peers (partsync, with the
//     EC slot filter when the tenant has an EC policy) into the local backend.
//  3. Create the engine over whichever signal prefixes now have a bucket index locally — the
//     same backend-driven discovery startup recovery uses — and load its parts.
//
// After a pass the spare serves the shard's flushed data; the still-unflushed head converges
// through the normal path (the new owner set receives new writes, and the previous owners'
// head is flushed by the compaction owner and then mirrored). Errors are logged and left for
// the next tick, like the rest of the maintenance loop.
func (s *Storage) bootstrapGainedTenants(ctx context.Context) {
	if s.cluster == nil {
		return
	}

	shards, err := s.cluster.ownership.Claims(ctx)
	if err != nil {
		s.obs.Logger(ctx).Warn("bootstrap: claims discovery failed", zap.Error(err))

		return
	}

	log := s.obs.Logger(ctx)

	for _, shard := range shards {
		tid := signal.TenantID(shard)

		if !s.ownsShard(tid) || s.hasAnyEngine(tid) {
			continue
		}

		log.Info("bootstrap: gained shard with no local engine", zap.String("shard", shard))
		s.bootstrapShard(ctx, tid)
	}
}

// ownsShard reports whether this node is among the shard's ring owners (at the tenant's
// replication factor / EC owner count).
func (s *Storage) ownsShard(tid signal.TenantID) bool {
	for _, n := range s.ownerLookup(tid) {
		if s.cluster.membership.AddrOf(n.ID) == s.cluster.self {
			return true
		}
	}

	return false
}

// hasAnyEngine reports whether this node holds an engine for the tenant in any signal.
func (s *Storage) hasAnyEngine(tid signal.TenantID) bool {
	s.tmu.Lock()
	defer s.tmu.Unlock()

	return s.tenants[tid] != nil ||
		s.logTenants[tid] != nil || s.traceTenants[tid] != nil || s.profileTenants[tid] != nil
}

// bootstrapShard mirrors one gained shard's data from its peers and creates the engines over
// whatever signals actually exist for it.
func (s *Storage) bootstrapShard(ctx context.Context, tid signal.TenantID) {
	log := s.obs.Logger(ctx)

	for _, sp := range []struct {
		prefix string
		sig    signal.Signal
	}{
		{metricsPrefix, signal.Metric},
		{logsPrefix, signal.Log},
		{tracesPrefix, signal.Trace},
		{profilesPrefix, signal.Profile},
	} {
		// Shared-nothing: pull the shard's objects from its peers first. A shared backend
		// already has them (syncParts is a no-op there).
		s.syncParts(ctx, tid, sp.prefix, false)

		// Only create an engine for a signal the shard actually has data in: the bucket index
		// is the signal's existence marker, exactly as in startup recovery.
		indexKey := string(s.normalizeTenant(tid)) + sp.prefix + "/" + bucketindex.Object
		if _, err := backend.ReadView(ctx, s.backend, indexKey); err != nil {
			continue
		}

		if err := s.bootstrapEngine(ctx, tid, sp.sig); err != nil {
			log.Warn("bootstrap: engine load failed",
				zap.String("shard", string(tid)), zap.Stringer("signal", sp.sig), zap.Error(err))

			continue
		}

		log.Info("bootstrap: shard signal loaded",
			zap.String("shard", string(tid)), zap.Stringer("signal", sp.sig))
	}
}

// bootstrapEngine creates the tenant's engine for one signal and loads its flushed parts.
func (s *Storage) bootstrapEngine(ctx context.Context, tid signal.TenantID, sig signal.Signal) error {
	if sig == signal.Metric {
		eng, err := s.engineFor(tid)
		if err != nil {
			return err
		}

		return eng.RefreshReplica(ctx)
	}

	eng, err := s.recordEngineFor(sig, string(tid))
	if err != nil {
		return err
	}

	return eng.RefreshReplica(ctx)
}
