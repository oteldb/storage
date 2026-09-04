package storage

import (
	"context"

	"github.com/go-faster/errors"
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
//     shard has a claiming owner), and keep the ones this node owns per the ring.
//  2. In shared-nothing mode, mirror each such shard's data from its peers (partsync, with the
//     EC slot filter when the tenant has an EC policy) into the local backend.
//  3. Create the engine over whichever signal prefixes now have a bucket index locally — the
//     same backend-driven discovery startup recovery uses — and load its parts.
//
// Steps 2 and 3 run **per signal**, gated on that signal's engine alone. A shard-wide "has any
// engine" gate strands the other signals for good: the write path creates engines lazily per
// signal, so the first replicated write of any one signal lands before the first maintenance tick
// and would otherwise disclaim the shard's other signals on every owner — reads then fail over
// until the owner set has turned over, after which every owner disclaims and the query returns
// empty without an error.
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
	owned := make(map[signal.TenantID]struct{}, len(shards))

	for _, shard := range shards {
		tid := signal.TenantID(shard)

		if !s.ownsShard(tid) {
			continue
		}

		owned[tid] = struct{}{}

		if s.bootstrapDone(tid) {
			continue
		}

		log.Debug("bootstrap: scanning owned shard for unloaded signals", zap.String("shard", shard))

		if s.bootstrapShard(ctx, tid) {
			s.markBootstrapped(tid)
		}
	}

	s.forgetBootstrapped(owned)
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

// hasEngine reports whether this node holds an engine for the tenant in one signal.
func (s *Storage) hasEngine(sig signal.Signal, tid signal.TenantID) bool {
	if sig == signal.Metric {
		_, ok := s.lookupEngine(tid)

		return ok
	}

	_, ok := s.lookupRecordEngine(sig, tid)

	return ok
}

// bootstrapDone reports whether a completed bootstrap pass has already resolved every signal of
// this shard. It is what keeps the per-signal scan off the steady-state path: without it a node
// owning a metrics-only shard would mirror-and-probe the three absent signals on every
// maintenance tick (a peer list per signal per shard in shared-nothing mode, a backend HEAD in
// both). A shard is memoized only after a pass that saw no error, so a transient peer or backend
// failure retries rather than sealing in a stranded signal; a signal that gains data later gets
// its engine from the write path, which reaches every owner.
func (s *Storage) bootstrapDone(tid signal.TenantID) bool {
	s.cluster.bootMu.Lock()
	defer s.cluster.bootMu.Unlock()

	_, ok := s.cluster.booted[tid]

	return ok
}

func (s *Storage) markBootstrapped(tid signal.TenantID) {
	s.cluster.bootMu.Lock()
	defer s.cluster.bootMu.Unlock()

	s.cluster.booted[tid] = struct{}{}
}

// forgetBootstrapped drops memo entries for shards this node no longer owns, so a shard that is
// lost and later regained is bootstrapped again.
func (s *Storage) forgetBootstrapped(owned map[signal.TenantID]struct{}) {
	s.cluster.bootMu.Lock()
	defer s.cluster.bootMu.Unlock()

	for tid := range s.cluster.booted {
		if _, ok := owned[tid]; !ok {
			delete(s.cluster.booted, tid)
		}
	}
}

// bootstrapShard mirrors one gained shard's data from its peers and creates the engines over
// whatever signals actually exist for it, reporting whether the pass resolved every signal
// without error (see bootstrapDone).
func (s *Storage) bootstrapShard(ctx context.Context, tid signal.TenantID) bool {
	log := s.obs.Logger(ctx)
	complete := true

	for _, sp := range []struct {
		prefix string
		sig    signal.Signal
	}{
		{metricsPrefix, signal.Metric},
		{logsPrefix, signal.Log},
		{tracesPrefix, signal.Trace},
		{profilesPrefix, signal.Profile},
	} {
		if s.hasEngine(sp.sig, tid) {
			continue
		}

		// Shared-nothing: pull the shard's objects from its peers first. A shared backend
		// already has them (syncPartsStatus is a no-op there). A sync error leaves the pass
		// incomplete even when the probe below finds nothing: an unreachable peer cannot prove
		// the signal is absent, only that we have not seen it yet.
		if _, err := s.syncPartsStatus(ctx, tid, sp.prefix, false); err != nil {
			complete = false
		}

		// Only create an engine for a signal the shard actually has data in: the bucket index
		// is the signal's existence marker, exactly as in startup recovery.
		indexKey := string(s.normalizeTenant(tid)) + sp.prefix + "/" + bucketindex.Object
		if _, err := backend.SizeOf(ctx, s.backend, indexKey); err != nil {
			if !errors.Is(err, backend.ErrNotExist) {
				complete = false
			}

			continue
		}

		if err := s.bootstrapEngine(ctx, tid, sp.sig); err != nil {
			log.Warn("bootstrap: engine load failed",
				zap.String("shard", string(tid)), zap.Stringer("signal", sp.sig), zap.Error(err))

			complete = false

			continue
		}

		log.Info("bootstrap: shard signal loaded",
			zap.String("shard", string(tid)), zap.Stringer("signal", sp.sig))
	}

	return complete
}

// bootstrapEngine creates the tenant's engine for one signal and loads its flushed parts. The shard
// arrives with its flushed data only, so the engine starts with a read gap over the head the
// previous owners still hold: until a flush covers it, this node disclaims reads into that window
// rather than answering them short (cluster_completeness.go).
func (s *Storage) bootstrapEngine(ctx context.Context, tid signal.TenantID, sig signal.Signal) error {
	if sig == signal.Metric {
		eng, err := s.engineFor(tid)
		if err != nil {
			return err
		}

		if err := eng.RefreshReplica(ctx); err != nil {
			return err
		}

		s.noteReadGap(sig, tid, metricPartsMax(eng))

		return nil
	}

	eng, err := s.recordEngineFor(sig, string(tid))
	if err != nil {
		return err
	}

	if err := eng.RefreshReplica(ctx); err != nil {
		return err
	}

	s.noteReadGap(sig, tid, recordPartsMax(eng))

	return nil
}
