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
// sample data. Label matchers resolve streams (services); column Conditions filter samples (value,
// sample_type, profile id, attributes). Returned rows carry content-addressed stack/sample-type ids
// (resolution against the symbol store is the embedder's, deferred at the fetch seam this milestone).
// Same tenant scoping as [Storage.TraceFetcher].
func (s *Storage) ProfileFetcher(tenants ...signal.TenantID) fetch.Fetcher {
	return s.recordFetcher(tenants, s.profileEngineSnapshot, s.lookupProfileEngine, s.clusterProfileFetcherFor)
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
