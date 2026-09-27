package storage

import (
	"context"
	"maps"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/internal/parallel"
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
func (s *Storage) WriteProfiles(ctx context.Context, pd profile.Profiles) (acc Accepted, err error) {
	ctx, finish := s.writeSpan(ctx, "storage.write.profiles")
	defer finish(&acc, &err)

	return s.writeRecords(ctx, signal.Profile, "write profiles",
		func(emit func(*recordengine.Batch)) int { return profile.Project(&pd, emit) }, s.profileEngineFor)
}

// ProfileFetcher returns the read seam for profiles — a [fetch.Fetcher] over the named tenants'
// sample data. Label matchers resolve streams (service plus the profile type, carried in reserved
// `otel.profile.*` labels); column Conditions filter samples (value, profile id, attributes).
// Returned rows carry the global content-addressed `stack_id`; resolve it with
// [Storage.ProfileResolver]. Same tenant scoping as [Storage.TraceFetcher].
func (s *Storage) ProfileFetcher(tenants ...signal.TenantID) fetch.Fetcher {
	return s.recordFetcher(signal.Profile, tenants, s.profileEngineSnapshotByTenant, s.lookupProfileEngine, s.clusterProfileFetcherFor)
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

	if s.cluster != nil {
		return s.clusterProfileSeries(ctx, tenant, matchers, start, end)
	}

	tid := s.normalizeTenant(tenant)

	eng, ok := s.lookupProfileEngine(tid)
	if !ok {
		return nil, nil
	}

	if err := s.answerLocally(ctx, rpcOpSeries, signal.Profile, tid, eng, start, end); err != nil {
		return nil, err
	}

	return eng.Series(matchers, start, end), nil
}

// ProfileResolver returns a symbol resolver for the samples a tenant holds within [start, end] (a
// zero start AND end covers every part), so an embedder resolves the content-addressed `stack_id`
// column of a sample fetch over the same window to function frames and builds a flamegraph. It
// reads the unflushed head and the parts overlapping the window, each part's tables decoded once and
// cached ([Options.ProfileSymbolCacheBytes]); a stack only samples outside the window reference may
// not resolve. An unknown tenant yields an empty resolver (every stack resolves to no frames), so
// callers need not special-case "no data". In cluster mode it gathers every shard's symbols:
// `stack_id`s are global content ids.
func (s *Storage) ProfileResolver(
	ctx context.Context, tenant signal.TenantID, start, end int64,
) (*profile.Resolver, error) {
	if s.closed.Load() {
		return nil, errors.Wrap(ErrClosed, "profile resolver")
	}

	var (
		layers []*profile.Tables
		err    error
	)

	if s.cluster != nil {
		layers, err = s.clusterProfileSymbols(ctx, tenant, start, end)
	} else if eng, ok := s.lookupProfileEngine(s.normalizeTenant(tenant)); ok {
		if incomplete := s.answerLocally(ctx, rpcOpSide, signal.Profile, s.normalizeTenant(tenant), eng, start, end); incomplete != nil {
			return nil, incomplete
		}

		layers, err = s.profileSymbolLayers(ctx, eng, start, end)
	}

	if err != nil {
		return nil, errors.Wrap(err, "load profile symbols")
	}

	return profile.NewResolverFrom(layers...), nil
}

// profileSymbolLayers returns an engine's symbols for [start, end], newest first: the live
// accumulator, an in-flight flush's snapshot, then each overlapping part's cached tables.
func (s *Storage) profileSymbolLayers(
	ctx context.Context, eng *recordengine.Engine, start, end int64,
) ([]*profile.Tables, error) {
	var (
		head    *profile.Tables
		foreign recordengine.SideStore
	)

	rd := eng.ReadSide(start, end, func(live recordengine.SideStore) {
		if st, ok := live.(*profile.SymbolStore); ok {
			head = st.Tables()
		} else {
			foreign = live
		}
	})
	defer rd.Release()

	if foreign != nil {
		return nil, errors.Errorf("profile engine side store is %T", foreign)
	}

	layers := make([]*profile.Tables, 0, len(rd.Parts)+2)
	layers = append(layers, head)

	if rd.Flushing != nil {
		flushing, err := profile.DecodeTables(rd.Flushing)
		if err != nil {
			return nil, errors.Wrap(err, "decode flushing symbols")
		}

		layers = append(layers, flushing)
	}

	parts := make([]*profile.Tables, len(rd.Parts))
	errs := make([]error, len(rd.Parts))

	parallel.ForEach(len(rd.Parts), parallel.DefaultLimit(), func(i int) {
		parts[i], errs[i] = s.profileSymbols.Get(ctx, rd.Parts[i].Key, rd.Parts[i].Load)
	})

	for i, err := range errs {
		if err != nil {
			return nil, errors.Wrapf(err, "part %q", rd.Parts[i].Key)
		}
	}

	return append(layers, parts...), nil
}

// profileEngineFor returns the profiles engine for a tenant, creating it (with a WAL when
// [Options.WALDir] is set) on first use. The engine carries a [profile.SymbolStore] side store, so
// flush/merge persist and union the symbol tables.
func (s *Storage) profileEngineFor(tid signal.TenantID) (*recordengine.Engine, error) {
	s.tmu.Lock()
	defer s.tmu.Unlock()

	return s.recordEngineCached(s.profileTenants, tid, signal.Profile, profilesPrefix, profile.Schema,
		func() recordengine.SideStore { return profile.NewSymbolStore() })
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
