package storage

import (
	"context"
	"maps"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/profile"
)

// profilesPrefix is the per-tenant key prefix under which a tenant's sample parts, indexes, and
// symbol-store sidecars live.
const profilesPrefix = "/profiles"

// WriteProfiles ingests a profiles batch. It projects each sample into a record row (flattening
// timestamped samples and denormalizing profile fields) and a content-addressed symbol delta,
// derives each sample's tenant from its Resource+Scope, and appends to that tenant's profiles
// engine — which persists the symbol store as part sidecars. Returns OTLP partial-success counts.
func (s *Storage) WriteProfiles(ctx context.Context, pd profile.Profiles) (Accepted, error) {
	if s.closed.Load() {
		return Accepted{}, errors.Wrap(ErrClosed, "write profiles")
	}

	project := func(emit func(*recordengine.Batch)) int { return profile.Project(&pd, emit) }

	if s.cluster != nil {
		return s.writeRecordsClustered(ctx, signal.Profile, project)
	}

	return s.writeRecordsLocal(project, s.profileEngineFor)
}

// ProfileFetcher returns the read seam for profiles — a [fetch.Fetcher] over the named tenants'
// sample data. Label matchers resolve streams (service plus the profile type, carried in reserved
// `otel.profile.*` labels); column Conditions filter samples (value, profile id, attributes).
// Returned rows carry the global content-addressed `stack_id`; resolve it with
// [Storage.ProfileResolver]. Same tenant scoping as [Storage.TraceFetcher].
func (s *Storage) ProfileFetcher(tenants ...signal.TenantID) fetch.Fetcher {
	return s.recordFetcher(tenants, s.profileEngineSnapshot, s.lookupProfileEngine, s.clusterProfileFetcherFor)
}

// ProfileSeries returns the identities of a tenant's profile streams matching the label matchers
// within [start, end] (zero start AND end disables the time filter). It is the enumeration primitive
// an embedder uses to build the Pyroscope-style ProfileTypes / LabelNames / LabelValues responses:
// the profile type is carried in each series' reserved `otel.profile.*` labels (see `signal/profile`),
// and the user labels are the resource/scope attributes. Local to this node — cluster fan-out for
// enumeration is not yet wired (the sample read path already fans out).
func (s *Storage) ProfileSeries(
	ctx context.Context, tenant signal.TenantID, matchers []fetch.Matcher, start, end int64,
) ([]signal.Series, error) {
	if s.closed.Load() {
		return nil, errors.Wrap(ErrClosed, "profile series")
	}

	_ = ctx // enumeration is in-memory over the index; ctx is reserved for future cluster fan-out.

	eng, ok := s.lookupProfileEngine(s.normalizeTenant(tenant))
	if !ok {
		return nil, nil
	}

	return eng.Series(matchers, start, end), nil
}

// ProfileResolver returns a symbol resolver over a tenant's profile symbol store (the unioned head +
// part sidecars), so an embedder resolves the content-addressed `stack_id` column of a sample fetch
// to function frames and builds a flamegraph. An unknown tenant yields an empty resolver (every
// stack resolves to no frames), so callers need not special-case "no data". Resolution is correct
// across the cluster — `stack_id`s are global content ids — but reads the local node's symbol store;
// replicating the store itself is deferred (see `signal/profile`).
func (s *Storage) ProfileResolver(ctx context.Context, tenant signal.TenantID) (*profile.Resolver, error) {
	if s.closed.Load() {
		return nil, errors.Wrap(ErrClosed, "profile resolver")
	}

	eng, ok := s.lookupProfileEngine(s.normalizeTenant(tenant))
	if !ok {
		return profile.NewResolver(nil)
	}

	tables, err := eng.SideSnapshot(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "load profile symbols")
	}

	return profile.NewResolver(tables)
}

// profileEngineFor returns the profiles engine for a tenant, creating it on first use. The engine
// carries a [profile.SymbolStore] side store, so flush/merge persist and union the symbol tables.
func (s *Storage) profileEngineFor(tid signal.TenantID) *recordengine.Engine {
	s.tmu.Lock()
	defer s.tmu.Unlock()

	e := s.profileTenants[tid]
	if e == nil {
		e = recordengine.New(recordengine.Config{
			Schema:    profile.Schema,
			OOOWindow: s.opts.OOOWindow,
			Backend:   s.backend,
			Prefix:    string(s.normalizeTenant(tid)) + profilesPrefix,
			SideStore: profile.NewSymbolStore(),
		})
		s.profileTenants[tid] = e
	}

	return e
}

func (s *Storage) lookupProfileEngine(tid signal.TenantID) (*recordengine.Engine, bool) {
	s.tmu.Lock()
	defer s.tmu.Unlock()

	e, ok := s.profileTenants[tid]

	return e, ok
}

func (s *Storage) profileEngineSnapshot() []*recordengine.Engine {
	s.tmu.Lock()
	defer s.tmu.Unlock()

	out := make([]*recordengine.Engine, 0, len(s.profileTenants))
	for _, eng := range s.profileTenants {
		out = append(out, eng)
	}

	return out
}

func (s *Storage) profileEngineSnapshotByTenant() map[signal.TenantID]*recordengine.Engine {
	s.tmu.Lock()
	defer s.tmu.Unlock()

	out := make(map[signal.TenantID]*recordengine.Engine, len(s.profileTenants))
	maps.Copy(out, s.profileTenants)

	return out
}
