package storage

import (
	"bytes"
	"context"
	"maps"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/logengine"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/log"
	"github.com/oteldb/storage/wal"
)

// logsPrefix is the per-tenant key prefix under which a tenant's log parts and indexes live.
const logsPrefix = "/logs"

// WriteLogs ingests a logs batch. It projects the internal model, derives each record's tenant
// from its Resource+Scope, and appends to that tenant's logs engine (indexing stream labels +
// buffering records). Returns per-OTLP partial-success counts: rejected counts out-of-order drops.
func (s *Storage) WriteLogs(ctx context.Context, ld log.Logs) (Accepted, error) {
	if s.closed.Load() {
		return Accepted{}, errors.Wrap(ErrClosed, "write logs")
	}

	if s.cluster != nil {
		return s.writeLogsClustered(ctx, ld)
	}

	var oooRejected int64

	// Tenant (hence engine) is derived from Resource+Scope, constant within a stream, so streams
	// arrive in tenant-contiguous runs; cache the last resolution to skip the locked lookup.
	var (
		lastTenant signal.TenantID
		lastEng    *logengine.Engine
	)

	emitted := log.Project(ld, func(b *log.Batch) {
		tid := s.tenantFor(b.Resource(), b.Scope())
		if lastEng == nil || tid != lastTenant {
			lastTenant, lastEng = tid, s.logEngineFor(tid)
		}

		// Engines are ephemeral here (no WAL wired into the facade), so AppendBatch never errors;
		// records beyond the OOO window are not accepted and counted as rejected.
		accepted, _ := lastEng.AppendBatch(b)
		oooRejected += int64(b.Len() - accepted)
	})

	return Accepted{
		Accepted: int64(emitted) - oooRejected,
		Rejected: oooRejected,
	}, nil
}

// writeLogsClustered is the cluster ingest path for logs: it projects the batch, frames each
// tenant's stream registrations + records as a WAL-encoded payload, and routes each to its ring
// primary (the single authority — it OOO-checks, reports the reject count, and replicates the
// accepted set to the secondary owners). The returned accounting matches the single-node path.
func (s *Storage) writeLogsClustered(ctx context.Context, ld log.Logs) (Accepted, error) {
	type tenantWAL struct {
		buf  bytes.Buffer
		w    *wal.Writer
		seen map[signal.SeriesID]struct{}
	}

	byTenant := make(map[signal.TenantID]*tenantWAL)

	emitted := log.Project(ld, func(b *log.Batch) {
		tid := s.tenantFor(b.Resource(), b.Scope())

		tw := byTenant[tid]
		if tw == nil {
			tw = &tenantWAL{seen: make(map[signal.SeriesID]struct{})}
			tw.w = wal.NewWriter(&tw.buf)
			byTenant[tid] = tw
		}

		id := b.StreamID
		if _, ok := tw.seen[id]; !ok { // register each stream once
			tw.seen[id] = struct{}{}
			_ = tw.w.WriteSeries(id, b.Series())
		}

		recs := make([]wal.LogRecord, b.Len())
		for i := range b.Records() {
			recs[i] = logRecordToWAL(b.At(i))
		}

		_ = tw.w.WriteLogRecords(id, recs)
	})

	var rejected int64

	for tid, tw := range byTenant {
		tenant := string(s.normalizeTenant(tid))

		rej, err := s.routeToPrimary(ctx, signal.Log, tenant, tw.buf.Bytes())
		if err != nil {
			return Accepted{Accepted: int64(emitted) - rejected, Rejected: rejected}, err
		}

		rejected += int64(rej)
	}

	return Accepted{Accepted: int64(emitted) - rejected, Rejected: rejected}, nil
}

// logRecordToWAL converts a model record to the WAL wire form, serializing attributes.
func logRecordToWAL(r log.Record) wal.LogRecord {
	return wal.LogRecord{
		Timestamp:         r.Timestamp,
		ObservedTimestamp: r.ObservedTimestamp,
		SeverityNumber:    r.SeverityNumber,
		Flags:             r.Flags,
		Dropped:           r.Dropped,
		SeverityText:      r.SeverityText,
		Body:              r.Body,
		TraceID:           r.TraceID,
		SpanID:            r.SpanID,
		Attrs:             r.Attributes.AppendHashInput(nil),
	}
}

// LogFetcher returns the read seam for logs — a [fetch.Fetcher] over the named tenants' log data
// (head ∪ flushed parts). Like [Storage.Fetcher] it scopes by tenant: one, several (concatenated),
// or none ⇒ all tenants with log data. It is always usable: an empty fetcher when no tenant
// matches or after [Close]. Label matchers resolve streams; column Conditions filter records.
func (s *Storage) LogFetcher(tenants ...signal.TenantID) fetch.Fetcher {
	if s.closed.Load() {
		return fetch.Merge() // empty
	}

	// In cluster mode a named tenant is served owner-aware (local if owned, else fanned out).
	if s.cluster != nil && len(tenants) > 0 {
		fetchers := make([]fetch.Fetcher, 0, len(tenants))
		for _, t := range tenants {
			fetchers = append(fetchers, s.clusterLogFetcherFor(t))
		}

		if len(fetchers) == 1 {
			return fetchers[0]
		}

		return concatFetcher(fetchers)
	}

	var fetchers []fetch.Fetcher

	if len(tenants) == 0 {
		for _, eng := range s.logEngineSnapshot() {
			fetchers = append(fetchers, eng)
		}
	} else {
		for _, t := range tenants {
			if e, ok := s.lookupLogEngine(s.normalizeTenant(t)); ok {
				fetchers = append(fetchers, e)
			}
		}
	}

	switch len(fetchers) {
	case 0:
		return fetch.Merge() // empty
	case 1:
		return fetchers[0]
	default:
		return concatFetcher(fetchers)
	}
}

// concatFetcher runs each child and concatenates their batches. Unlike [fetch.Merge] it does not
// deduplicate by timestamp — log records are append-only and several may share a timestamp, and
// the metric-shaped merge would also drop the record columns. Used for multi-tenant log reads.
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

// logEngineFor returns the logs engine for a tenant, creating it on first use.
func (s *Storage) logEngineFor(tid signal.TenantID) *logengine.Engine {
	s.tmu.Lock()
	defer s.tmu.Unlock()

	e := s.logTenants[tid]
	if e == nil {
		e = logengine.New(logengine.Config{
			OOOWindow: s.opts.OOOWindow,
			Backend:   s.backend,
			Prefix:    string(s.normalizeTenant(tid)) + logsPrefix,
		})
		s.logTenants[tid] = e
	}

	return e
}

// lookupLogEngine returns the tenant's logs engine if it exists, without creating one.
func (s *Storage) lookupLogEngine(tid signal.TenantID) (*logengine.Engine, bool) {
	s.tmu.Lock()
	defer s.tmu.Unlock()

	e, ok := s.logTenants[tid]

	return e, ok
}

// logEngineSnapshot returns the current log engines (a copy, for lock-free iteration).
func (s *Storage) logEngineSnapshot() []*logengine.Engine {
	s.tmu.Lock()
	defer s.tmu.Unlock()

	out := make([]*logengine.Engine, 0, len(s.logTenants))
	for _, eng := range s.logTenants {
		out = append(out, eng)
	}

	return out
}

// logEngineSnapshotByTenant is [Storage.logEngineSnapshot] keyed by tenant id (for policy lookup).
func (s *Storage) logEngineSnapshotByTenant() map[signal.TenantID]*logengine.Engine {
	s.tmu.Lock()
	defer s.tmu.Unlock()

	out := make(map[signal.TenantID]*logengine.Engine, len(s.logTenants))
	maps.Copy(out, s.logTenants)

	return out
}
