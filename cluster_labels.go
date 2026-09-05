package storage

import (
	"context"
	"slices"

	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// localLabels serves a peer's label-metadata enumeration from the local engine, dispatched by the
// request's signal — the index-only twin of [Storage.localSeries], backing the cluster label
// endpoints.
//
// Only metrics carry a label index. A record signal (log/trace/profile) answers
// [fetch.ErrLabelsUnsupported], which the caller reads as "keep doing what you were doing" rather
// than as a fault: it is deliberately not [cluster.ErrShardAbsent], because failing over to another
// owner would meet the same answer.
func (s *Storage) localLabels(ctx context.Context, r cluster.LabelsRequest) ([]string, error) {
	if r.Signal != signal.Metric {
		return nil, fetch.ErrLabelsUnsupported
	}

	tid := s.normalizeTenant(signal.TenantID(r.Tenant))

	eng, ok := s.lookupEngine(tid)
	if err := s.canAnswer(ctx, rpcOpLabels, signal.Metric, tid, ok, r.Start, r.End); err != nil {
		return nil, err
	}

	req := metricSeriesRequest(tid, r.Matchers(), r.Start, r.End)
	if len(r.Name) == 0 {
		return eng.LabelNames(ctx, req)
	}

	return eng.LabelValues(ctx, req, r.Name)
}

// clusterLabels enumerates a metric tenant's label names (an empty name) or one name's values across
// its shards in cluster mode: locally where this node owns the shard, else from an owner (hedged
// failover). Label metadata is a set, so the shards compose by union — the same merge the local
// fan-out performs.
//
// Every shard must answer. A shard skipped because it could not be reached, or answered from the
// capable subset, would drop labels that exist only there, and the caller cannot tell that apart
// from a complete answer — so a shard's failure fails the whole call and the caller falls back to a
// path that sees every shard.
//
// The request must carry only pushable matchers: the shards return strings, not identities, so a
// matcher that could not be lowered into the index has nowhere to be re-checked. One that was not
// pushable makes the call [fetch.ErrLabelsUnsupported].
func (s *Storage) clusterLabels(
	ctx context.Context, tid signal.TenantID, r fetch.Request, name []byte,
) ([]string, error) {
	eq := fetch.EqualitySpecs(r.Matchers)
	if len(eq) != len(r.Matchers) {
		return nil, fetch.ErrLabelsUnsupported
	}

	tenant := s.normalizeTenant(tid)
	n := s.cluster.shardCount()

	var all []string

	for idx := range n {
		vs, err := s.shardLabels(ctx, cluster.LabelsRequest{
			Signal: signal.Metric,
			Tenant: string(shardKeyOf(tenant, idx, n)),
			Start:  r.Start,
			End:    r.End,
			Name:   name,
			Equal:  eq,
		})
		if err != nil {
			return nil, err
		}

		all = append(all, vs...)
	}

	slices.Sort(all)

	return slices.Compact(all), nil
}

// shardLabels enumerates one metric shard's label metadata: locally if this node holds the shard,
// else hedged across its remote owners. Each owner holds a complete copy of the shard, so the first
// successful answer is authoritative — owners fail over, they do not merge.
func (s *Storage) shardLabels(ctx context.Context, r cluster.LabelsRequest) ([]string, error) {
	shardKey := signal.TenantID(r.Tenant)

	local, remotes := s.shardPlacement(ctx, rpcOpLabels, r.Signal, shardKey)
	if local {
		vs, err := s.localLabels(ctx, r)
		if !disclaimedLocally(err) {
			return vs, err
		}
	}

	return hedgeOwners(ctx, s, rpcOpLabels, remotes, func(ctx context.Context, addr string) ([]string, error) {
		return cluster.FetchLabels(ctx, s.cluster.httpc, addr, r, s.clusterOpts...)
	})
}

// LabelNames implements [fetch.LabelLister] for a tenant's cluster read seam, so the PromQL label
// endpoints reach the index-only path in cluster mode instead of falling back to the O(cardinality)
// identity gather. See [Storage.clusterLabels] for how the shards compose.
func (f clusterSeriesFetcher) LabelNames(ctx context.Context, r fetch.Request) ([]string, error) {
	return f.store.clusterLabels(ctx, f.tenant, r, nil)
}

// LabelValues is [clusterSeriesFetcher.LabelNames] for one name's values. An empty name has no
// values (it is the names discriminator on the wire), so it lists nothing.
func (f clusterSeriesFetcher) LabelValues(ctx context.Context, r fetch.Request, name []byte) ([]string, error) {
	if len(name) == 0 {
		return nil, nil
	}

	return f.store.clusterLabels(ctx, f.tenant, r, name)
}
