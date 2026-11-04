package storage

import (
	"context"
	"math"
	"time"

	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

type retentionFloor struct {
	value   int64
	enabled bool
}

func ageReadFloor(now int64, age time.Duration) retentionFloor {
	if age <= 0 {
		return retentionFloor{}
	}
	floor := int64(math.MinInt64)
	if now >= math.MinInt64+int64(age) {
		floor = now - int64(age)
	}
	return retentionFloor{value: floor, enabled: true}
}

func (s *Storage) readFloor(sig signal.Signal, tenant signal.TenantID, now int64) retentionFloor {
	tenant = s.normalizeTenant(tenantOfShard(tenant))
	return ageReadFloor(now, s.tenant.Resolve(tenant).Retention.AgeFor(sig))
}

type retentionSnapshotKey struct{}

type retentionReadSnapshot struct {
	store   *Storage
	sig     signal.Signal
	now     int64
	tenants []signal.TenantID
	floors  []retentionFloor
}

func (s *Storage) clampRead(ctx context.Context, sig signal.Signal, tenant signal.TenantID, r fetch.Request) fetch.Request {
	tenant = s.normalizeTenant(tenantOfShard(tenant))
	if snapshot, ok := ctx.Value(retentionSnapshotKey{}).(*retentionReadSnapshot); ok && snapshot.store == s && snapshot.sig == sig {
		for i, tid := range snapshot.tenants {
			if tid == tenant {
				return snapshot.floors[i].clamp(r)
			}
		}
	}
	return s.readFloor(sig, tenant, s.now()).clamp(r)
}

func (f retentionFloor) clamp(r fetch.Request) fetch.Request {
	if f.enabled {
		r.Start = max(r.Start, f.value)
	}
	return r
}

type retentionForwarder struct{ fetch.Fetcher }

func (f retentionForwarder) Unwrap() fetch.Fetcher { return f.Fetcher }

func (f retentionForwarder) TryFetch(ctx context.Context, r fetch.Request) (fetch.Iterator, error) {
	if inner, ok := f.Fetcher.(fetch.TryFetcher); ok {
		return inner.TryFetch(ctx, r)
	}
	return nil, fetch.ErrTryUnsupported
}

type retentionFetcher struct {
	retentionForwarder

	store  *Storage
	sig    signal.Signal
	tenant signal.TenantID
}

func (f retentionFetcher) Fetch(ctx context.Context, r fetch.Request) (fetch.Iterator, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r = f.store.clampRead(ctx, f.sig, f.tenant, r)
	if r.Start > r.End {
		return fetch.NewSliceIterator(nil), nil
	}
	return f.Fetcher.Fetch(ctx, r)
}

func (f retentionFetcher) TryFetch(ctx context.Context, r fetch.Request) (fetch.Iterator, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r = f.store.clampRead(ctx, f.sig, f.tenant, r)
	if r.Start > r.End {
		return fetch.NewSliceIterator(nil), nil
	}
	return f.retentionForwarder.TryFetch(ctx, r)
}

type retentionCounter struct {
	retentionForwarder

	policy retentionFetcher
	inner  fetch.Counter
}

func (f retentionCounter) Count(ctx context.Context, r fetch.Request) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	r = f.policy.store.clampRead(ctx, f.policy.sig, f.policy.tenant, r)
	if r.Start > r.End {
		return 0, nil
	}
	return f.inner.Count(ctx, r)
}

type retentionGroupCounter struct {
	retentionForwarder

	policy retentionFetcher
	inner  fetch.GroupCounter
}

func (f retentionGroupCounter) CountBy(ctx context.Context, r fetch.Request, group []byte) (map[string]int, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r = f.policy.store.clampRead(ctx, f.policy.sig, f.policy.tenant, r)
	if r.Start > r.End {
		return map[string]int{}, nil
	}
	return f.inner.CountBy(ctx, r, group)
}

type retentionGridCounter struct {
	retentionForwarder

	policy retentionFetcher
	inner  fetch.GridCounter
}

func (f retentionGridCounter) CountGrid(
	ctx context.Context, r fetch.Request, grid fetch.WindowGrid, group []byte,
) (map[string][]int, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r = f.policy.store.clampRead(ctx, f.policy.sig, f.policy.tenant, r)
	return f.inner.CountGrid(ctx, r, grid, group)
}

func (s *Storage) retainedFetcher(sig signal.Signal, tenant signal.TenantID, inner fetch.Fetcher) fetch.Fetcher {
	policy := retentionFetcher{retentionForwarder: retentionForwarder{inner}, store: s, sig: sig, tenant: tenant}
	var wrapped fetch.Fetcher = policy
	if counter := fetch.CounterOf(inner); counter != nil {
		wrapped = retentionCounter{retentionForwarder{wrapped}, policy, counter}
	}
	if counter := fetch.GroupCounterOf(inner); counter != nil {
		wrapped = retentionGroupCounter{retentionForwarder{wrapped}, policy, counter}
	}
	if counter := fetch.GridCounterOf(inner); counter != nil {
		wrapped = retentionGridCounter{retentionForwarder{wrapped}, policy, counter}
	}
	return wrapped
}

type retentionCache struct {
	store    *Storage
	tenants  []signal.TenantID
	uncached fetch.Fetcher
}

func (s *Storage) snapshotRead(ctx context.Context, sig signal.Signal, tenants []signal.TenantID) context.Context {
	if len(tenants) == 0 {
		return ctx
	}
	if previous, ok := ctx.Value(retentionSnapshotKey{}).(*retentionReadSnapshot); ok && previous.store == s && previous.sig == sig {
		complete := true
		for _, tid := range tenants {
			found := false
			for _, prior := range previous.tenants {
				found = found || prior == tid
			}
			complete = complete && found
		}
		if complete {
			return ctx
		}
	}
	snapshot := &retentionReadSnapshot{store: s, sig: sig, now: s.now(), tenants: tenants, floors: make([]retentionFloor, len(tenants))}
	for i, tid := range tenants {
		snapshot.floors[i] = s.readFloor(sig, tid, snapshot.now)
	}
	return context.WithValue(ctx, retentionSnapshotKey{}, snapshot)
}

type retentionQueryFetcher struct {
	retentionForwarder

	store   *Storage
	sig     signal.Signal
	tenants []signal.TenantID
}

func (f retentionQueryFetcher) Fetch(ctx context.Context, r fetch.Request) (fetch.Iterator, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return f.Fetcher.Fetch(f.store.snapshotRead(ctx, f.sig, f.tenants), r)
}

func (f retentionQueryFetcher) TryFetch(ctx context.Context, r fetch.Request) (fetch.Iterator, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return f.retentionForwarder.TryFetch(f.store.snapshotRead(ctx, f.sig, f.tenants), r)
}

func (s *Storage) retainedQuery(sig signal.Signal, tenants []signal.TenantID, inner fetch.Fetcher) fetch.Fetcher {
	shared := s.cluster != nil || sig == signal.Metric && (s.queryCache != nil || s.opts.QuerySplitInterval > 0)
	if len(tenants) == 0 || len(tenants) == 1 && !shared {
		return inner
	}
	normalized := make([]signal.TenantID, len(tenants))
	for i, tid := range tenants {
		normalized[i] = s.normalizeTenant(tenantOfShard(tid))
	}
	return retentionQueryFetcher{retentionForwarder{inner}, s, sig, normalized}
}

func (f *retentionCache) snapshot(ctx context.Context, start int64) (context.Context, bool) {
	ctx = f.store.snapshotRead(ctx, signal.Metric, f.tenants)
	snapshot := ctx.Value(retentionSnapshotKey{}).(*retentionReadSnapshot)
	clipped := false
	for _, floor := range snapshot.floors {
		clipped = clipped || floor.enabled && start < floor.value
	}
	return ctx, clipped
}

func metricAggregateRead[T any](
	ctx context.Context, s *Storage, t signal.TenantID, r fetch.Request,
	remote func(context.Context, signal.TenantID, fetch.Request) (T, error),
	local func(context.Context, *engine.Engine, fetch.Request) (T, error),
) (T, error) {
	var zero T
	if s.closed.Load() {
		return zero, nil
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	if s.cluster != nil {
		ctx = s.snapshotRead(ctx, signal.Metric, []signal.TenantID{s.normalizeTenant(tenantOfShard(t))})
	}
	r = s.clampRead(ctx, signal.Metric, t, r)
	if r.Start > r.End {
		return zero, nil
	}
	if s.cluster != nil {
		return remote(ctx, t, r)
	}
	tid := s.normalizeTenant(t)
	eng, ok := s.lookupEngine(tid)
	if !ok {
		return zero, nil
	}
	if err := s.answerLocally(ctx, rpcOpRead, signal.Metric, tid, eng, r.Start, r.End); err != nil {
		return zero, err
	}
	return local(ctx, eng, r)
}

type localMetricRead struct {
	eng     *engine.Engine
	request fetch.Request
}

func (s *Storage) prepareLocalMetricRead(ctx context.Context, tenant string, start, end int64) (localMetricRead, error) {
	if err := ctx.Err(); err != nil {
		return localMetricRead{}, err
	}
	tid := s.normalizeTenant(signal.TenantID(tenant))
	r := s.clampRead(ctx, signal.Metric, tid, fetch.Request{Tenant: tid, Start: start, End: end})
	if r.Start > r.End {
		return localMetricRead{}, nil
	}
	eng, ok := s.lookupEngine(tid)
	if err := s.canAnswer(ctx, rpcOpRead, signal.Metric, tid, ok, r.Start, r.End); err != nil {
		return localMetricRead{}, err
	}
	return localMetricRead{eng, r}, nil
}
