package router

import (
	"context"

	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/internal/parallel"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/log"
	"github.com/oteldb/storage/signal/profile"
	"github.com/oteldb/storage/signal/trace"
)

// WriteLogs frames a logs batch by shard and routes each shard to its ring primary.
func (r *Router) WriteLogs(ctx context.Context, ld log.Logs, tenantOf cluster.TenantFunc) (Written, error) {
	return r.writeRecords(ctx, signal.Log, func(emit func(*recordengine.Batch)) int {
		return log.Project(ld, emit)
	}, tenantOf)
}

// WriteTraces frames a spans batch by shard and routes each shard to its ring primary.
func (r *Router) WriteTraces(ctx context.Context, td trace.Traces, tenantOf cluster.TenantFunc) (Written, error) {
	return r.writeRecords(ctx, signal.Trace, func(emit func(*recordengine.Batch)) int {
		return trace.Project(td, emit)
	}, tenantOf)
}

// WriteProfiles frames a profiles batch by shard and routes each shard to its ring primary.
func (r *Router) WriteProfiles(ctx context.Context, pd *profile.Profiles, tenantOf cluster.TenantFunc) (Written, error) {
	return r.writeRecords(ctx, signal.Profile, func(emit func(*recordengine.Batch)) int {
		return profile.Project(pd, emit)
	}, tenantOf)
}

// writeRecords is the shared body of the three record signals: frame by shard, route each shard to
// its primary in parallel, sum what came back. Only the signal discriminator and the projector
// differ between them.
func (r *Router) writeRecords(
	ctx context.Context, sig signal.Signal, project cluster.RecordProjector, tenantOf cluster.TenantFunc,
) (Written, error) {
	frames := cluster.FrameRecords(project, r.shards, tenantOf, nil)

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
		rej, err := r.PrimaryWrite(ctx, sig, routes[i].key, routes[i].payload)
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

	for _, err := range errs { // first failure by route index, deterministically
		if err != nil {
			return out, err
		}
	}

	return out, nil
}
