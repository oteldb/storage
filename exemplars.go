package storage

import (
	"context"
	"maps"

	"github.com/go-faster/errors"
	"go.uber.org/zap"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/exemplar"
	"github.com/oteldb/storage/signal/metric"
)

// exemplarsPrefix is the per-tenant key prefix under which a tenant's exemplar parts and indexes
// live. Exemplars are a separate engine from the metrics they hang off — a separate part lifecycle,
// so exemplar retention and compaction are independent of the samples'.
const exemplarsPrefix = "/exemplars"

// writeExemplars ingests the exemplars carried by a metrics batch, keyed by the metric series each
// one hangs off. It is driven from [Storage.WriteMetrics] rather than exposed as its own facade
// method: OTLP delivers exemplars inline with their points, so splitting them would force the
// embedder to re-derive metric identity.
//
// It is **best-effort**: its accept/reject counts are deliberately not folded into the [Accepted]
// returned to the producer, which counts data points for OTLP partial-success. Losing an exemplar
// degrades trace correlation; losing a sample corrupts a time series. A failure here is counted and
// logged, never propagated.
func (s *Storage) writeExemplars(ctx context.Context, md metric.Metrics) {
	// The common case is a producer that emits no exemplars at all. Check with a cheap structural
	// scan before the projection pass, which would otherwise re-hash every point's identity.
	if !hasExemplars(md) {
		return
	}

	project := func(emit func(*recordengine.Batch)) int {
		n := 0
		metric.Project(md, func(mb *metric.Batch) { n += exemplar.Project(mb, emit) })

		return n
	}

	var err error
	if s.cluster != nil {
		_, err = s.writeRecordsClustered(ctx, signal.Exemplar, project)
	} else {
		_, err = s.writeRecordsLocal(ctx, signal.Exemplar, project, s.exemplarEngineFor)
	}

	if err != nil {
		s.obs.Logger(ctx).Warn("drop exemplars", zap.Error(err))
	}
}

// hasExemplars reports whether any point in the batch carries an exemplar.
func hasExemplars(md metric.Metrics) bool {
	for ri := range md.Resources {
		rm := &md.Resources[ri]
		for si := range rm.Scopes {
			sm := &rm.Scopes[si]
			for mi := range sm.Metrics {
				mt := &sm.Metrics[mi]
				for pi := range mt.Points {
					if len(mt.Points[pi].Exemplars) > 0 {
						return true
					}
				}
			}
		}
	}

	return false
}

// ExemplarFetcher returns the read seam for exemplars — a [fetch.Fetcher] over the named tenants'
// exemplar rows. Because an exemplar stream *is* its metric series, the label matchers are exactly
// the metric's: the matcher set an embedder resolves a PromQL selector to selects the matching
// exemplar streams unchanged, which is what an /api/v1/query_exemplars endpoint needs. Column
// Conditions filter rows (value, trace/span id, filtered attributes).
//
// Row values arrive in the exemplars schema's column order; decode the value column with
// [exemplar.DecodeValue]. Same tenant scoping as [Storage.TraceFetcher].
func (s *Storage) ExemplarFetcher(tenants ...signal.TenantID) fetch.Fetcher {
	return s.recordFetcher(
		signal.Exemplar, tenants, s.exemplarEngineSnapshot, s.lookupExemplarEngine, s.clusterExemplarFetcherFor,
	)
}

// ExemplarsForTrace returns every exemplar recorded against a trace id — the metrics half of the
// trace-correlation triangle completed by [Storage.Trace] and [Storage.LogsForTrace]. It rides the
// trace_id column's per-part equality bloom, so parts that never saw the trace are skipped without
// being read; within a surviving part it is a column scan, not an index lookup.
func (s *Storage) ExemplarsForTrace(
	ctx context.Context, tenant signal.TenantID, traceID []byte,
) ([]*fetch.Batch, error) {
	if s.closed.Load() {
		return nil, errors.Wrap(ErrClosed, "exemplars for trace")
	}

	return s.fetchByEquality(ctx, s.ExemplarFetcher(tenant), signal.Exemplar, exemplar.ColTraceID, traceID)
}

// exemplarEngineFor returns the exemplars engine for a tenant, creating it (with a WAL when
// [Options.WALDir] is set) on first use.
func (s *Storage) exemplarEngineFor(tid signal.TenantID) (*recordengine.Engine, error) {
	s.tmu.Lock()
	defer s.tmu.Unlock()

	return s.recordEngineCached(s.exemplarTenants, tid, signal.Exemplar, exemplarsPrefix, exemplar.Schema, nil)
}

func (s *Storage) lookupExemplarEngine(tid signal.TenantID) (*recordengine.Engine, bool) {
	s.tmu.Lock()
	defer s.tmu.Unlock()

	e, ok := s.exemplarTenants[tid]

	return e, ok
}

func (s *Storage) exemplarEngineSnapshot() []*recordengine.Engine {
	s.tmu.Lock()
	defer s.tmu.Unlock()

	out := make([]*recordengine.Engine, 0, len(s.exemplarTenants))
	for _, eng := range s.exemplarTenants {
		out = append(out, eng)
	}

	return out
}

func (s *Storage) exemplarEngineSnapshotByTenant() map[signal.TenantID]*recordengine.Engine {
	s.tmu.Lock()
	defer s.tmu.Unlock()

	out := make(map[signal.TenantID]*recordengine.Engine, len(s.exemplarTenants))
	maps.Copy(out, s.exemplarTenants)

	return out
}
