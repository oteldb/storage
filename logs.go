package storage

import (
	"context"
	"maps"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/log"
)

// logsPrefix is the per-tenant key prefix under which a tenant's log parts and indexes live.
const logsPrefix = "/logs"

// WriteLogs ingests a logs batch. It projects the internal model, derives each record's tenant
// from its Resource+Scope, and appends to that tenant's logs engine (indexing stream labels +
// buffering records). Returns per-OTLP partial-success counts: rejected counts out-of-order drops.
func (s *Storage) WriteLogs(ctx context.Context, ld log.Logs) (acc Accepted, err error) {
	ctx, finish := s.writeSpan(ctx, "storage.write.logs")
	defer finish(&acc, &err)

	if s.closed.Load() {
		return Accepted{}, errors.Wrap(ErrClosed, "write logs")
	}

	project := func(emit func(*recordengine.Batch)) int { return log.Project(ld, emit) }

	if s.cluster != nil {
		return s.writeRecordsClustered(ctx, signal.Log, project)
	}

	return s.writeRecordsLocal(ctx, signal.Log, project, s.logEngineFor)
}

// LogFetcher returns the read seam for logs — a [fetch.Fetcher] over the named tenants' log data
// (head ∪ flushed parts). Like [Storage.Fetcher] it scopes by tenant: one, several (concatenated),
// or none ⇒ all tenants with log data. Always usable: an empty fetcher when no tenant matches or
// after [Close]. Label matchers resolve streams; column Conditions filter records.
func (s *Storage) LogFetcher(tenants ...signal.TenantID) fetch.Fetcher {
	return s.recordFetcher(signal.Log, tenants, s.logEngineSnapshot, s.lookupLogEngine, s.clusterLogFetcherFor)
}

// LogSeries returns the identities of a tenant's log streams matching the label matchers within
// [start, end] (zero start AND end disables the time filter). It mirrors [Storage.ProfileSeries]
// for the logs vertical, so an embedder can build LabelNames/LabelValues/Series responses without
// materializing records. Local to this node; cluster fan-out for log enumeration is not yet wired.
func (s *Storage) LogSeries(
	_ context.Context, tenant signal.TenantID, matchers []fetch.Matcher, start, end int64,
) ([]signal.Series, error) {
	if s.closed.Load() {
		return nil, errors.Wrap(ErrClosed, "log series")
	}

	eng, ok := s.lookupLogEngine(s.normalizeTenant(tenant))
	if !ok {
		return nil, nil
	}

	return eng.Series(matchers, start, end), nil
}

// KeyScope is a bitset of the scopes an attribute key appears in. A key can appear in more than one
// (e.g. as a resource attribute on one stream and a record attribute on another).
type KeyScope uint8

const (
	// KeyScopeResource marks a resource attribute (stream identity).
	KeyScopeResource = KeyScope(recordengine.KeyScopeResource)
	// KeyScopeScope marks an instrumentation-scope attribute (stream identity).
	KeyScopeScope = KeyScope(recordengine.KeyScopeScope)
	// KeyScopeRecord marks a per-record attribute (the attrs column).
	KeyScopeRecord = KeyScope(recordengine.KeyScopeRecord)
)

// KeyInfo is a distinct attribute key and the scope(s) it was observed in. Key aliases interned,
// low-cardinality metadata owned by the engine; copy it to retain past the call.
type KeyInfo struct {
	Key   []byte
	Scope KeyScope
}

// LogKeys returns the distinct attribute keys present in a tenant's log records within [start, end],
// each tagged with the scope(s) it appears in (resource, scope, record). A zero start AND end
// disables the time filter. It is the counterpart to [Storage.LogSeries] (which enumerates only
// stream identities): the per-record [KeyScopeRecord] keys let an embedder list and push down
// record-attribute labels its [Storage.LogSeries]-based resolution cannot see, and the scope bitset
// authoritatively distinguishes a stream label from a record attribute (or both). Keys are
// low-cardinality metadata. Local to this node; cluster fan-out is a follow-up.
func (s *Storage) LogKeys(_ context.Context, tenant signal.TenantID, start, end int64) ([]KeyInfo, error) {
	if s.closed.Load() {
		return nil, errors.Wrap(ErrClosed, "log keys")
	}

	eng, ok := s.lookupLogEngine(s.normalizeTenant(tenant))
	if !ok {
		return nil, nil
	}

	raw := eng.Keys(start, end)
	if len(raw) == 0 {
		return nil, nil
	}

	out := make([]KeyInfo, len(raw))
	for i := range raw {
		out[i] = KeyInfo{Key: raw[i].Key, Scope: KeyScope(raw[i].Scope)}
	}

	return out, nil
}

// concatFetcher runs each child and concatenates their batches. Unlike [fetch.Merge] it does not
// deduplicate by timestamp — records are append-only and several may share a timestamp, and the
// metric-shaped merge would drop the record columns. Used for multi-tenant record reads.
type concatFetcher []fetch.Fetcher

func (c concatFetcher) Fetch(ctx context.Context, r fetch.Request) (fetch.Iterator, error) {
	var all []*fetch.Batch

	for _, f := range c {
		it, err := f.Fetch(ctx, r)
		if err != nil {
			return nil, err
		}

		b, err := fetch.Drain(ctx, it)
		if err != nil {
			return nil, err
		}

		all = append(all, b...)
	}

	return fetch.NewSliceIterator(all), nil
}

// logEngineFor returns the logs engine for a tenant, creating it (with a WAL when [Options.WALDir]
// is set) on first use.
func (s *Storage) logEngineFor(tid signal.TenantID) (*recordengine.Engine, error) {
	s.tmu.Lock()
	defer s.tmu.Unlock()

	return s.recordEngineCached(s.logTenants, tid, signal.Log, logsPrefix, log.Schema, nil)
}

// lookupLogEngine returns the tenant's logs engine if it exists, without creating one.
func (s *Storage) lookupLogEngine(tid signal.TenantID) (*recordengine.Engine, bool) {
	s.tmu.Lock()
	defer s.tmu.Unlock()

	e, ok := s.logTenants[tid]

	return e, ok
}

// logEngineSnapshot returns the current log engines (a copy, for lock-free iteration).
func (s *Storage) logEngineSnapshot() []*recordengine.Engine {
	s.tmu.Lock()
	defer s.tmu.Unlock()

	out := make([]*recordengine.Engine, 0, len(s.logTenants))
	for _, eng := range s.logTenants {
		out = append(out, eng)
	}

	return out
}

// logEngineSnapshotByTenant is [Storage.logEngineSnapshot] keyed by tenant id (for policy lookup).
func (s *Storage) logEngineSnapshotByTenant() map[signal.TenantID]*recordengine.Engine {
	s.tmu.Lock()
	defer s.tmu.Unlock()

	out := make(map[signal.TenantID]*recordengine.Engine, len(s.logTenants))
	maps.Copy(out, s.logTenants)

	return out
}
