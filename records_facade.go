package storage

import (
	"context"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/signal"
)

// The logs and traces facades are structurally identical — both are record signals over
// recordengine — so their write and read paths share these helpers, parameterized by the signal's
// projector and engine accessors. Only the schema (carried by the engine) and the projector differ.

// recordProjector projects a signal's ingest batch, calling emit once per stream and returning the
// total record count (it wraps log.Project / trace.Project).
type recordProjector func(emit func(*recordengine.Batch)) int

// writeRecordsLocal ingests a projected record batch into per-tenant engines (single-node path),
// deriving the tenant from each stream's Resource+Scope and returning OTLP partial-success counts.
func (s *Storage) writeRecordsLocal(project recordProjector, engineFor func(signal.TenantID) *recordengine.Engine) (Accepted, error) {
	var oooRejected int64

	var (
		lastTenant signal.TenantID
		lastEng    *recordengine.Engine
	)

	emitted := project(func(b *recordengine.Batch) {
		id := b.Identity()
		tid := s.tenantFor(id.Resource, id.Scope)
		if lastEng == nil || tid != lastTenant {
			lastTenant, lastEng = tid, engineFor(tid)
		}

		// Ephemeral here (no WAL wired into the facade), so AppendBatch never errors; records
		// beyond the OOO window are not accepted and counted as rejected.
		accepted, _ := lastEng.AppendBatch(b)
		oooRejected += int64(b.Len() - accepted)
	})

	return Accepted{Accepted: int64(emitted) - oooRejected, Rejected: oooRejected}, nil
}

// writeRecordsClustered frames each tenant's streams+records as a WAL payload and routes it to the
// tenant's ring primary (primary-authoritative replication); the reject count flows back.
func (s *Storage) writeRecordsClustered(ctx context.Context, sig signal.Signal, project recordProjector) (Accepted, error) {
	byTenant := make(map[signal.TenantID][]byte)

	emitted := project(func(b *recordengine.Batch) {
		id := b.Identity()
		tid := s.tenantFor(id.Resource, id.Scope)
		byTenant[tid] = append(byTenant[tid], recordengine.EncodeWAL(b)...)
	})

	var rejected int64

	for tid, payload := range byTenant {
		tenant := string(s.normalizeTenant(tid))

		rej, err := s.routeToPrimary(ctx, sig, tenant, payload)
		if err != nil {
			return Accepted{Accepted: int64(emitted) - rejected, Rejected: rejected}, err
		}

		rejected += int64(rej)
	}

	return Accepted{Accepted: int64(emitted) - rejected, Rejected: rejected}, nil
}

// recordFetcher builds a record signal's read seam over the named tenants: owner-aware in cluster
// mode (via clusterFor), else the local engines (snapshot for all tenants, lookup for named ones).
// Multi-tenant reads concatenate (records are append-only and column-shaped, not ts-deduped).
func (s *Storage) recordFetcher(
	tenants []signal.TenantID,
	snapshot func() []*recordengine.Engine,
	lookup func(signal.TenantID) (*recordengine.Engine, bool),
	clusterFor func(signal.TenantID) fetch.Fetcher,
) fetch.Fetcher {
	if s.closed.Load() {
		return fetch.Merge()
	}

	if s.cluster != nil && len(tenants) > 0 {
		fetchers := make([]fetch.Fetcher, 0, len(tenants))
		for _, t := range tenants {
			fetchers = append(fetchers, clusterFor(t))
		}

		return oneOrConcat(fetchers)
	}

	var fetchers []fetch.Fetcher

	if len(tenants) == 0 {
		for _, eng := range snapshot() {
			fetchers = append(fetchers, eng)
		}
	} else {
		for _, t := range tenants {
			if e, ok := lookup(s.normalizeTenant(t)); ok {
				fetchers = append(fetchers, e)
			}
		}
	}

	return oneOrConcat(fetchers)
}

// oneOrConcat returns an empty fetcher, the single child, or a concatenating fetcher.
func oneOrConcat(fetchers []fetch.Fetcher) fetch.Fetcher {
	switch len(fetchers) {
	case 0:
		return fetch.Merge()
	case 1:
		return fetchers[0]
	default:
		return concatFetcher(fetchers)
	}
}
