package storage

import (
	"cmp"
	"context"
	"slices"

	"go.uber.org/zap"

	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/tenant"
)

// retentionCutoff converts a retention window into an absolute cutoff at the given now (unix
// nanoseconds); 0 means retain forever.
func retentionCutoff(r tenant.Retention, now int64) int64 {
	if r.MaxAge <= 0 {
		return 0
	}

	return now - r.MaxAge.Nanoseconds()
}

// sizedPart is one flushed part's contribution to a tenant's size budget: its inclusive upper time
// bound and its stored (on-backend) byte size.
type sizedPart struct {
	maxTime int64
	bytes   int64
}

// sizeRetentionCutoff turns a byte budget into an absolute time cutoff: the parts are dropped
// oldest-first (by upper time bound) until the retained total fits maxBytes, and the cutoff is one
// nanosecond past the newest dropped part. 0 means the budget is unset or already satisfied.
//
// The newest part is never dropped, so a budget smaller than one part degrades to "keep the newest
// part" instead of emptying the tenant — parts here are not time-bounded (one merge can hold a
// tenant's whole history unless [tenant.Limits.MaxPartSize] bounds them), and silently discarding
// everything on a too-small budget is the worse failure. Size the parts to get a tighter bound.
//
// Overlapping parts make the drop slightly stronger than planned (the cutoff also trims rows below
// it from the retained parts), which only converges faster on the budget. It sorts parts in place.
func sizeRetentionCutoff(parts []sizedPart, maxBytes int64) int64 {
	if maxBytes <= 0 || len(parts) < 2 {
		return 0
	}

	var total int64
	for _, p := range parts {
		total += p.bytes
	}

	if total <= maxBytes {
		return 0
	}

	slices.SortFunc(parts, func(a, b sizedPart) int { return cmp.Compare(a.maxTime, b.maxTime) })

	var cutoff int64

	for _, p := range parts[:len(parts)-1] {
		if total <= maxBytes {
			break
		}

		total -= p.bytes
		cutoff = p.maxTime + 1
	}

	return cutoff
}

// sizeCutoffFor resolves the size-retention cutoff of one tenant (a real tenant id, not a shard
// key): 0 when the tenant has no MaxBytes budget, is already under it, or its part sizes cannot be
// read. The budget spans every signal and every locally-held shard of the tenant, since it bounds
// the bytes this node stores for it.
//
// It reads per-part object sizes from the backend (like [Storage.EfficiencyStats]), so it is only
// called for tenants that actually set a budget — and, within that, only when the answer can have
// changed: the cutoff is a pure function of the tenant's part set and its budget, so it is memoized
// against a fingerprint of both ([Storage.partSetFingerprint], in-memory and I/O-free). Parts are
// immutable, so on a node where nothing flushed, merged, or was dropped since the last cycle the
// enumeration would re-read every part's object sizes to arrive at the same number.
func (s *Storage) sizeCutoffFor(ctx context.Context, t signal.TenantID) int64 {
	t = s.normalizeTenant(t) // engines are keyed by the normalized id
	maxBytes := s.tenant.Resolve(t).Retention.MaxBytes

	if maxBytes <= 0 {
		return 0
	}

	fp := s.partSetFingerprint(t, maxBytes)
	if cutoff, ok := s.sizeRetention.lookup(t, fp); ok {
		return cutoff
	}

	parts, err := s.sizedParts(ctx, t)
	if err != nil {
		// Best-effort, like the rest of the cycle: without sizes we fall back to age-only retention
		// rather than dropping data on a guess.
		s.obs.Logger(ctx).Warn("size retention: part sizes unavailable",
			zap.String("tenant", string(t)), zap.Error(err))

		return 0
	}

	cutoff := sizeRetentionCutoff(parts, maxBytes)
	s.sizeRetention.store(t, fp, cutoff)

	return cutoff
}

// sizedParts collects every flushed part this node holds for a tenant, across signals and shards.
func (s *Storage) sizedParts(ctx context.Context, t signal.TenantID) ([]sizedPart, error) {
	var out []sizedPart

	for tid, eng := range s.engineSnapshotByTenant() {
		if tenantOfShard(tid) != t {
			continue
		}

		parts, err := eng.PartsDetailed(ctx)
		if err != nil {
			return nil, err
		}

		for _, p := range parts {
			out = append(out, sizedPart{maxTime: p.MaxTime, bytes: p.Bytes})
		}
	}

	addRecord := func(engines map[signal.TenantID]*recordengine.Engine) error {
		for tid, eng := range engines {
			if tenantOfShard(tid) != t {
				continue
			}

			parts, err := eng.PartsDetailed(ctx)
			if err != nil {
				return err
			}

			for _, p := range parts {
				out = append(out, sizedPart{maxTime: p.MaxTime, bytes: p.Bytes})
			}
		}

		return nil
	}

	for _, engines := range []map[signal.TenantID]*recordengine.Engine{
		s.logEngineSnapshotByTenant(),
		s.traceEngineSnapshotByTenant(),
		s.profileEngineSnapshotByTenant(),
	} {
		if err := addRecord(engines); err != nil {
			return nil, err
		}
	}

	return out, nil
}

// sizeCutoffs resolves the size-retention cutoff of every tenant behind the given shard keys, keyed
// by shard key so a maintenance cycle can look one up per engine. Tenants without a MaxBytes budget
// are absent (the common case costs nothing); a tenant sharded across engines is resolved once.
func (s *Storage) sizeCutoffs(ctx context.Context, tids map[signal.TenantID]struct{}) map[signal.TenantID]int64 {
	var (
		byTenant map[signal.TenantID]int64
		out      map[signal.TenantID]int64
	)

	resolved := make(map[signal.TenantID]struct{}, len(tids))

	for tid := range tids {
		t := tenantOfShard(tid)
		resolved[s.normalizeTenant(t)] = struct{}{} // the memo is keyed by the normalized id

		cutoff, ok := byTenant[t]
		if !ok {
			cutoff = s.sizeCutoffFor(ctx, t)

			if byTenant == nil {
				byTenant = make(map[signal.TenantID]int64)
			}

			byTenant[t] = cutoff
		}

		if cutoff > 0 {
			if out == nil {
				out = make(map[signal.TenantID]int64)
			}

			out[tid] = cutoff
		}
	}

	s.sizeRetention.retain(resolved)

	return out
}
