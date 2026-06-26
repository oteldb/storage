package storage

import (
	"context"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/tenant"
)

// The logs and traces facades are structurally identical — both are record signals over
// recordengine — so their write and read paths share these helpers, parameterized by the signal's
// projector and engine accessors. Only the schema (carried by the engine) and the projector differ.

// recordProjector projects a signal's ingest batch, calling emit once per stream and returning the
// total record count (it wraps log.Project / trace.Project).
type recordProjector func(emit func(*recordengine.Batch)) int

// recordEngineCached returns the cached record engine for a tenant in m, creating it (with a WAL
// when [Options.WALDir] is set, and the optional side store from newSide) on first use. The caller
// holds s.tmu. Shared by the logs/traces/profiles *EngineFor constructors, which differ only in the
// tenant map, key-prefix suffix, schema, and side store.
func (s *Storage) recordEngineCached(
	m map[signal.TenantID]*recordengine.Engine, tid signal.TenantID, suffix string,
	schema *recordengine.Schema, newSide func() recordengine.SideStore,
) (*recordengine.Engine, error) {
	if e := m[tid]; e != nil {
		return e, nil
	}

	prefix := string(s.normalizeTenant(tid)) + suffix

	w, err := s.walFor(prefix)
	if err != nil {
		return nil, err
	}

	var side recordengine.SideStore
	if newSide != nil {
		side = newSide()
	}

	e := recordengine.New(recordengine.Config{
		Schema:    schema,
		OOOWindow: s.opts.OOOWindow,
		Backend:   s.backend,
		Prefix:    prefix,
		SideStore: side,
		WAL:       w,
	})
	m[tid] = e

	return e, nil
}

// recordEngineFunc is the engine accessor passed to [Storage.writeRecordsLocal].
type recordEngineFunc func(signal.TenantID) (*recordengine.Engine, error)

// writeRecordsLocal ingests a projected record batch into per-tenant engines (single-node path),
// deriving the tenant from each stream's Resource+Scope and returning OTLP partial-success counts.
func (s *Storage) writeRecordsLocal(project recordProjector, engineFor recordEngineFunc) (Accepted, error) {
	var (
		rej        rejectTally
		firstErr   error
		lastTenant signal.TenantID
		lastEng    *recordengine.Engine
		lastAdmit  *tenantAdmission
		lastLimits tenant.Limits
	)

	emitted := project(func(b *recordengine.Batch) {
		if firstErr != nil {
			return
		}

		id := b.Identity()
		tid := s.tenantFor(id.Resource, id.Scope)
		if lastEng == nil || tid != lastTenant {
			eng, err := engineFor(tid)
			if err != nil {
				firstErr = err

				return
			}

			lastTenant, lastEng = tid, eng
			lastAdmit = s.admissionFor(tid)
			lastLimits = s.tenant.Resolve(s.normalizeTenant(tid)).Limits
		}

		// Admission (same valves as metrics, no sampling — dropping a log/span breaks a
		// stream/trace): the ingest-rate valve sheds a whole over-budget stream batch; cardinality
		// and in-flight-memory limits are enforced per record inside the engine.
		if !lastAdmit.allowRate(lastLimits, b.ByteSize(), s.now()) {
			rej.rate += int64(b.Len())
			lastAdmit.addRate(int64(b.Len()))

			return
		}

		// AppendBatch can also fail when a WAL is wired (a backend/fs write error).
		res, err := lastEng.AppendBatch(b, recordengine.AppendLimits{
			MaxSeries:        lastLimits.MaxSeries,
			MaxInFlightBytes: lastLimits.MaxInFlightBytes,
		})
		if err != nil {
			firstErr = err

			return
		}

		rej.ooo += int64(res.RejectedOOO)
		rej.cardinality += int64(res.RejectedCardinality)
		rej.inflight += int64(res.RejectedBytes)
		lastAdmit.record(int64(res.Accepted), int64(res.RejectedOOO), int64(res.RejectedCardinality), int64(res.RejectedBytes))
	})

	if firstErr != nil {
		return Accepted{}, firstErr
	}

	total := rej.total()

	return Accepted{Accepted: int64(emitted) - total, Rejected: total, RejectedReason: rej.reason()}, nil
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
		rej, err := s.routeToPrimary(ctx, sig, string(s.normalizeTenant(tid)), payload)
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
