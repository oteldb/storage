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

// FetchWants implements the engines' PartFetcher.
//
// The owner set is resolved once for the cycle, so every want in it is judged against the same
// view of who holds the shard.
//
// Finding nothing is reported as definitive absence only when this node could ask *every* owner
// the shard is expected to have: the ring named the full replication factor, every one of those
// nodes resolved to an address, and this node is among them. Anything less is
// [bucketindex.WantIncomplete] — during a rolling restart the deregistered node drops out of the
// ring, so the peers that answer are a strict subset of the owners and their not having the part
// says nothing about the one that does.
//
// The bar is deliberately the *configured* replication factor rather than whatever the ring
// currently returns. A cluster permanently running fewer nodes than its RF therefore never
// acknowledges a loss, which is the safe direction: an outstanding want is visible and
// recoverable, a hole over live data is neither.
func (r *partRepairer) FetchWants(ctx context.Context, wants []bucketindex.Want) []engine.FetchResult {
	out := make([]engine.FetchResult, len(wants))

	// An engine recovery built is handed this seam before the cluster layer starts. There is no
	// owner set to ask yet, so nothing may be concluded.
	if r.s.cluster == nil {
		for i := range out {
			out[i].Outcome = bucketindex.WantIncomplete
		}

		return out
	}

	complete, remotes := r.s.completeOwners(r.tid)

	absent := bucketindex.WantIncomplete
	if complete {
		absent = bucketindex.WantAbsent
	}

	if len(remotes) == 0 {
		for i := range out {
			out[i].Outcome = absent
		}

		return out
	}

	fetched := r.s.cluster.psync.FetchWants(ctx, r.prefix, remotes, wants)
	for i := range fetched {
		res := &fetched[i]

		switch {
		case res.Err != nil:
			out[i] = engine.FetchResult{Outcome: bucketindex.WantIncomplete, Err: res.Err}
		case res.OK:
			out[i] = engine.FetchResult{Entry: res.Entry, Outcome: bucketindex.WantSatisfied}
		default:
			out[i] = engine.FetchResult{Outcome: absent}
		}
	}

	return out
}

// recordPartRepairer adapts a metric repair seam to the record engines' identical one; the two
// engines declare their own result type, so the wrapper only re-labels the fields.
type recordPartRepairer struct{ engine.PartFetcher }

// FetchWants implements recordengine.PartFetcher.
func (r recordPartRepairer) FetchWants(
	ctx context.Context, wants []bucketindex.Want,
) []recordengine.FetchResult {
	src := r.PartFetcher.FetchWants(ctx, wants)

	out := make([]recordengine.FetchResult, len(src))
	for i := range src {
		s := &src[i]
		out[i] = recordengine.FetchResult{Entry: s.Entry, Outcome: s.Outcome, Err: s.Err}
	}

	return out
}

// completeOwners reports the shard's remote owners and whether they are the whole expected owner
// set — the ring named as many owners as the replication factor asks for, every one of them
// resolved to an address, and this node is one of them.
func (s *Storage) completeOwners(shardKey signal.TenantID) (complete bool, remotes []string) {
	cn := s.cluster

	var local, unresolved int

	owners := s.ownerLookup(shardKey)
	for _, o := range owners {
		addr := cn.membership.AddrOf(o.ID)

		switch {
		case addr == cn.self:
			local++
		case addr != "":
			remotes = append(remotes, addr)
		default:
			unresolved++
		}
	}

	complete = local > 0 && unresolved == 0 && len(owners) >= s.rfFor(shardKey)

	return complete, remotes
}

// repairerFor returns the cluster repair seam for one engine, or nil where there is nothing to
// repair from: a shared backend needs no cross-node copy (every replica reads the same objects), and
// without a cluster layer there is no peer at all.
//
// It is decided from the options because the seam is fixed at engine creation, and recovery creates
// every engine found on the backend before the cluster layer starts.
func (s *Storage) repairerFor(tid signal.TenantID, prefix string) *partRepairer {
	if s.opts.Cluster == nil || !s.opts.Cluster.PrivateBackend {
		return nil
	}

	return &partRepairer{s: s, tid: tid, prefix: prefix}
}

// repairSeamFor is the seam an engine is built with. A writable store without a cluster layer gets
// [soleOwnerRepairer]; a read-only one gets nil, because it must never commit a hole. A nil seam
// makes repair a no-op that still counts what it could not satisfy.
//
// The mode is read from the options, not from s.cluster: recovery creates engines before the
// cluster layer starts, and a cluster node's engine must never take the single-node evidence rule.
func (s *Storage) repairSeamFor(tid signal.TenantID, prefix string) engine.PartFetcher {
	if s.opts.Cluster == nil && !s.opts.ReadOnly {
		return soleOwnerRepairer{backend: s.backendFor(tid), prefix: prefix}
	}

	if r := s.repairerFor(tid, prefix); r != nil {
		return r
	}

	return nil
}

func (s *Storage) recordRepairerFor(tid signal.TenantID, prefix string) recordengine.PartFetcher {
	r := s.repairSeamFor(tid, prefix)
	if r == nil {
		return nil
	}

	return recordPartRepairer{r}
}

func (s *Storage) metricRepairerFor(tid signal.TenantID, prefix string) engine.PartFetcher {
	return s.repairSeamFor(tid, prefix)
}
