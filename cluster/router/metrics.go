package router

import (
	"context"

	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/internal/parallel"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/metric"
)

// writeFanOut bounds how many shard primaries one write routes to at once. Writes are RPC-bound,
// so this sits above the CPU count to overlap round-trips while capping in-flight requests on a
// wide fan-out.
const writeFanOut = 16

// Written is the outcome of a [Router.WriteMetrics]: what the shards accepted, and what each
// primary refused and why.
type Written struct {
	// Accepted is how many points the primaries took.
	Accepted int
	// Rejected is the combined per-reason breakdown across every shard.
	Rejected cluster.Reject
}

// WriteMetrics frames md by shard and routes each shard to its ring primary, in parallel.
//
// It is the write half of a storage node without the node: the same framing, the same shard keys,
// and the same per-shard primary authority — so an ingester's writes are indistinguishable from a
// node's, and land in the same places.
//
// The origin-side ingest-rate valve is not applied here: it is per-tenant policy the router does
// not hold. The cardinality and in-flight-memory valves are head-enforced and still apply, so they
// come back in the returned breakdown.
func (r *Router) WriteMetrics(ctx context.Context, md metric.Metrics, tenantOf cluster.TenantFunc) (Written, error) {
	frames := cluster.FrameMetrics(md, r.shards, tenantOf, nil)

	type route struct {
		key     signal.TenantID
		payload []byte
	}

	routes := make([]route, 0, len(frames.Shards))
	for sk, payload := range frames.Shards {
		routes = append(routes, route{sk, payload})
	}

	rejects := make([]cluster.Reject, len(routes))
	errs := make([]error, len(routes))

	parallel.ForEach(len(routes), writeFanOut, func(i int) {
		rej, err := r.PrimaryWrite(ctx, signal.Metric, routes[i].key, routes[i].payload)
		if err != nil {
			errs[i] = err

			return
		}

		rejects[i] = rej
	})

	var out Written
	for _, rej := range rejects {
		out.Rejected.OOO += rej.OOO
		out.Rejected.Cardinality += rej.Cardinality
		out.Rejected.InFlight += rej.InFlight
	}

	out.Accepted = frames.Emitted - out.Rejected.Total()

	// Surface the first failure deterministically (by route index): a partial write is the
	// caller's problem to retry, and retrying the whole batch is safe — the shards that took it
	// reject the replay out of order.
	for _, err := range errs {
		if err != nil {
			return out, err
		}
	}

	return out, nil
}
