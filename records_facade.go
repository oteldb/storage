package storage

import (
	"bytes"
	"context"

	"github.com/oteldb/storage/encoding/compress"
	"github.com/oteldb/storage/internal/parallel"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/tenant"
)

// fetchByEquality fetches every record whose byte column equals value from f, pruned by that
// column's per-part equality bloom. It is the shared body of the by-id lookups ([Storage.Trace],
// [Storage.LogsForTrace]): an operator-free equality Condition carrying the serializable Equal hint.
func (s *Storage) fetchByEquality(
	ctx context.Context, f fetch.Fetcher, sig signal.Signal, column string, value []byte,
) ([]*fetch.Batch, error) {
	want := bytes.Clone(value)
	cond := fetch.Condition{
		Column: column,
		Match:  func(v signal.Value) bool { return bytes.Equal(v.Str(), want) },
		Equal:  &fetch.EqualMatcher{Name: column, Value: string(want)},
	}

	it, err := f.Fetch(ctx, fetch.Request{
		Signal: sig, Start: 0, End: 1<<63 - 1,
		Conditions: []fetch.Condition{cond}, AllConditions: true,
	})
	if err != nil {
		return nil, err
	}

	return fetch.Drain(ctx, it)
}

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
	m map[signal.TenantID]*recordengine.Engine, tid signal.TenantID, sig signal.Signal, suffix string,
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
		Obs:       s.obs,
		Signal:    sig.String(),
		// Bound part size so size-tiered compaction can seal large parts and keep the merge's working
		// set O(part size) instead of O(dataset) (records_facade shares this with the metric engine's
		// engineFor). Resolved from the tenant policy, falling back to defaultMaxPartBytes when unset.
		MaxPartBytes: partSizeOrDefault(s.tenant.Resolve(s.normalizeTenant(tenantOfShard(tid))).Limits.MaxPartSize),
		// ZSTD-compress compacted parts: record byte columns are dict-coded but not entropy-coded, so
		// the cold, long-lived data is otherwise stored far larger than necessary (≈10× on logs).
		// Flushes stay codec-only, so ingest is unaffected.
		MergeCompression:      compress.AlgorithmZSTD,
		MergeCompressionLevel: compress.LevelBest,
	})
	m[tid] = e

	return e, nil
}

// recordEngineFunc is the engine accessor passed to [Storage.writeRecordsLocal].
type recordEngineFunc func(signal.TenantID) (*recordengine.Engine, error)

// writeRecordsLocal ingests a projected record batch into per-tenant engines (single-node path),
// deriving the tenant from each stream's Resource+Scope and returning OTLP partial-success counts.
func (s *Storage) writeRecordsLocal(
	ctx context.Context, sig signal.Signal, project recordProjector, engineFor recordEngineFunc,
) (Accepted, error) {
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
	accepted := int64(emitted) - total
	s.emitAdmission(ctx, sig, accepted, rej, 0, 0) // records are not sampled

	return Accepted{Accepted: accepted, Rejected: total, RejectedReason: rej.reason()}, nil
}

// writeRecordsClustered frames each tenant's streams+records as a WAL payload and routes it to the
// tenant's ring primary (primary-authoritative replication); the reject count flows back.
func (s *Storage) writeRecordsClustered(ctx context.Context, sig signal.Signal, project recordProjector) (Accepted, error) {
	// Group each stream by its shard key — (tenant, hash(streamID) % N) — so a tenant's streams
	// spread across the ring instead of pinning to one owner set (with N=1 the key is the tenant,
	// byte-identical to the unsharded path).
	n := s.cluster.shardCount()
	byShard := make(map[signal.TenantID][]byte)

	// The ingest-rate valve is applied at the origin (per real tenant, like the single-node path);
	// cardinality and in-flight memory are head-enforced by the shard primary in primaryWrite.
	var (
		rateRejected int64
		lastTenant   signal.TenantID
		lastAdmit    *tenantAdmission
		lastLimits   tenant.Limits
		haveTenant   bool
	)

	emitted := project(func(b *recordengine.Batch) {
		id := b.Identity()
		tid := s.normalizeTenant(s.tenantFor(id.Resource, id.Scope))
		if !haveTenant || tid != lastTenant {
			lastTenant, haveTenant = tid, true
			lastAdmit = s.admissionFor(tid)
			lastLimits = s.tenant.Resolve(tid).Limits
		}

		if !lastAdmit.allowRate(lastLimits, b.ByteSize(), s.now()) {
			rateRejected += int64(b.Len())
			lastAdmit.addRate(int64(b.Len()))

			return // whole over-budget stream batch shed before framing
		}

		sk := shardKeyOf(tid, shardOf(b.Stream, n), n)
		byShard[sk] = append(byShard[sk], recordengine.EncodeWAL(b)...)
	})

	// Each shard routes to its own ring primary independently; fan the routes out under a bound
	// rather than paying the sum of per-primary round-trips. Order-independent: results accumulate
	// into per-index slots.
	type route struct {
		key     signal.TenantID
		payload []byte
	}

	routes := make([]route, 0, len(byShard))
	for sk, payload := range byShard {
		routes = append(routes, route{sk, payload})
	}

	rejects := make([]primaryReject, len(routes))
	errs := make([]error, len(routes))

	parallel.ForEach(len(routes), clusterWriteFanOut, func(i int) {
		rej, err := s.routeToPrimary(ctx, sig, string(routes[i].key), routes[i].payload)
		if err != nil {
			errs[i] = err

			return
		}

		rejects[i] = rej
	})

	// Combine the origin rate rejections with each primary's per-reason breakdown.
	rej := rejectTally{rate: rateRejected}
	for _, r := range rejects {
		rej.ooo += int64(r.ooo)
		rej.cardinality += int64(r.cardinality)
		rej.inflight += int64(r.inflight)
	}

	for _, err := range errs { // surface the first error deterministically (by route index)
		if err != nil {
			return Accepted{Accepted: int64(emitted) - rej.total(), Rejected: rej.total()}, err
		}
	}

	total := rej.total()
	accepted := int64(emitted) - total
	s.emitAdmission(ctx, sig, accepted, rej, 0, 0)

	return Accepted{Accepted: accepted, Rejected: total, RejectedReason: rej.reason()}, nil
}

// recordFetcher builds a record signal's read seam over the named tenants: owner-aware in cluster
// mode (via clusterFor), else the local engines (snapshot for all tenants, lookup for named ones).
// Multi-tenant reads concatenate (records are append-only and column-shaped, not ts-deduped).
func (s *Storage) recordFetcher(
	sig signal.Signal,
	tenants []signal.TenantID,
	snapshot func() []*recordengine.Engine,
	lookup func(signal.TenantID) (*recordengine.Engine, bool),
	clusterFor func(signal.TenantID) fetch.Fetcher,
) fetch.Fetcher {
	if s.closed.Load() {
		return fetch.Merge()
	}

	seed := func(f fetch.Fetcher) fetch.Fetcher {
		return seedFetcher{inner: f, obs: s.obs, signal: sig.String()}
	}

	if s.cluster != nil && len(tenants) > 0 {
		fetchers := make([]fetch.Fetcher, 0, len(tenants))
		for _, t := range tenants {
			fetchers = append(fetchers, clusterFor(t))
		}

		return seed(oneOrConcat(fetchers))
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

	return seed(oneOrConcat(fetchers))
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
