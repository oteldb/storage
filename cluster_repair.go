package storage

import (
	"context"

	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/signal"
)

// partRepairer is the cluster half of the engines' repair seam: it answers "make this want's part
// local" by asking the shard's peer owners for it over the part-sync transport. The engines know
// nothing of peers or HTTP — they hand over a part identity and get back the entry of whatever
// part actually came across.
//
// Peers are resolved per call, not captured: ownership moves, and a repair running a maintenance
// cycle after a rebalance must ask whoever holds the shard now.
type partRepairer struct {
	s      *Storage
	tid    signal.TenantID
	prefix string
}

// FetchWant implements the engines' PartFetcher.
func (r *partRepairer) FetchWant(
	ctx context.Context, w bucketindex.Want,
) (bucketindex.Entry, bool, error) {
	_, remotes := r.s.shardOwners(r.tid)
	if len(remotes) == 0 {
		return bucketindex.Entry{}, false, nil
	}

	return r.s.cluster.psync.FetchWant(ctx, r.prefix, remotes, w)
}

// repairerFor returns the repair seam for one engine, or nil where there is nothing to repair
// from: a shared backend needs no cross-node copy (every replica reads the same objects), and
// single-node mode has no peer at all. A nil seam makes repair a no-op that still counts what it
// could not satisfy.
func (s *Storage) repairerFor(tid signal.TenantID, prefix string) *partRepairer {
	if s.cluster == nil || !s.cluster.private {
		return nil
	}

	return &partRepairer{s: s, tid: tid, prefix: prefix}
}

func (s *Storage) recordRepairerFor(tid signal.TenantID, prefix string) recordengine.PartFetcher {
	r := s.repairerFor(tid, prefix)
	if r == nil {
		return nil
	}

	return r
}

func (s *Storage) metricRepairerFor(tid signal.TenantID, prefix string) engine.PartFetcher {
	r := s.repairerFor(tid, prefix)
	if r == nil {
		return nil
	}

	return r
}
