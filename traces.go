package storage

import (
	"bytes"
	"context"
	"maps"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/trace"
)

// tracesPrefix is the per-tenant key prefix under which a tenant's span parts and indexes live.
const tracesPrefix = "/traces"

// WriteTraces ingests a traces batch. It projects the span model (computing nested-set ids and
// serializing events/links), derives each span's tenant from its Resource+Scope, and appends to
// that tenant's traces engine. Returns per-OTLP partial-success counts (out-of-order drops).
func (s *Storage) WriteTraces(ctx context.Context, td trace.Traces) (acc Accepted, err error) {
	ctx, finish := s.writeSpan(ctx, "storage.write.traces")
	defer finish(&acc, &err)

	if s.closed.Load() {
		return Accepted{}, errors.Wrap(ErrClosed, "write traces")
	}

	project := func(emit func(*recordengine.Batch)) int { return trace.Project(td, emit) }

	if s.cluster != nil {
		return s.writeRecordsClustered(ctx, signal.Trace, project)
	}

	return s.writeRecordsLocal(ctx, signal.Trace, project, s.traceEngineFor)
}

// TraceFetcher returns the read seam for traces — a [fetch.Fetcher] over the named tenants' span
// data. Label matchers resolve streams (services); column Conditions filter spans (name, kind,
// status, duration, attributes). Same tenant scoping as [Storage.LogFetcher].
func (s *Storage) TraceFetcher(tenants ...signal.TenantID) fetch.Fetcher {
	return s.recordFetcher(signal.Trace, tenants, s.traceEngineSnapshot, s.lookupTraceEngine, s.clusterTraceFetcherFor)
}

// TraceSeries returns the identities of a tenant's span streams matching the label matchers within
// [start, end] (zero start AND end disables the time filter). It mirrors [Storage.ProfileSeries]
// for the traces vertical, so an embedder can build tag-name/tag-value responses without
// materializing spans. In cluster mode it serves locally when this node owns the tenant, else it
// fans out to an owner (hedged), re-applying the non-equality matchers to the superset.
func (s *Storage) TraceSeries(
	ctx context.Context, tenant signal.TenantID, matchers []fetch.Matcher, start, end int64,
) ([]signal.Series, error) {
	if s.closed.Load() {
		return nil, errors.Wrap(ErrClosed, "trace series")
	}

	if s.cluster != nil {
		return s.clusterSeries(ctx, signal.Trace, s.normalizeTenant(tenant), matchers, start, end)
	}

	eng, ok := s.lookupTraceEngine(s.normalizeTenant(tenant))
	if !ok {
		return nil, nil
	}

	return eng.Series(matchers, start, end), nil
}

// Trace fetches every span of one trace from a tenant by trace id: an equality condition on the
// trace_id column, pruned by its per-part equality bloom (trace-by-id). It returns one batch per
// stream (service) carrying the trace's spans — including the nested-set columns the embedder's
// TraceQL uses for structural operators.
func (s *Storage) Trace(ctx context.Context, tenant signal.TenantID, traceID []byte) ([]*fetch.Batch, error) {
	if s.closed.Load() {
		return nil, errors.Wrap(ErrClosed, "trace")
	}

	want := bytes.Clone(traceID)
	cond := fetch.Condition{
		Column: trace.ColTraceID,
		Match:  func(v signal.Value) bool { return bytes.Equal(v.Str(), want) },
		Equal:  &fetch.EqualMatcher{Name: trace.ColTraceID, Value: string(want)},
	}

	it, err := s.TraceFetcher(tenant).Fetch(ctx, fetch.Request{
		Signal: signal.Trace, Start: 0, End: 1<<63 - 1,
		Conditions: []fetch.Condition{cond}, AllConditions: true,
	})
	if err != nil {
		return nil, err
	}

	return fetch.Drain(ctx, it)
}

// traceEngineFor returns the traces engine for a tenant, creating it (with a WAL when
// [Options.WALDir] is set) on first use.
func (s *Storage) traceEngineFor(tid signal.TenantID) (*recordengine.Engine, error) {
	s.tmu.Lock()
	defer s.tmu.Unlock()

	return s.recordEngineCached(s.traceTenants, tid, signal.Trace, tracesPrefix, trace.Schema, nil)
}

func (s *Storage) lookupTraceEngine(tid signal.TenantID) (*recordengine.Engine, bool) {
	s.tmu.Lock()
	defer s.tmu.Unlock()

	e, ok := s.traceTenants[tid]

	return e, ok
}

func (s *Storage) traceEngineSnapshot() []*recordengine.Engine {
	s.tmu.Lock()
	defer s.tmu.Unlock()

	out := make([]*recordengine.Engine, 0, len(s.traceTenants))
	for _, eng := range s.traceTenants {
		out = append(out, eng)
	}

	return out
}

func (s *Storage) traceEngineSnapshotByTenant() map[signal.TenantID]*recordengine.Engine {
	s.tmu.Lock()
	defer s.tmu.Unlock()

	out := make(map[signal.TenantID]*recordengine.Engine, len(s.traceTenants))
	maps.Copy(out, s.traceTenants)

	return out
}
