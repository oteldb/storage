package storage

import (
	"cmp"
	"context"
	"slices"

	"go.uber.org/zap"

	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/tenant"
)

// retentionCutoff converts a signal's retention window into an absolute cutoff at the given now
// (unix nanoseconds); 0 means retain forever. The window is per signal ([tenant.Retention.AgeFor]),
// so exemplars can expire ahead of the samples they hang off.
func retentionCutoff(r tenant.Retention, sig signal.Signal, now int64) int64 {
	age := r.AgeFor(sig)
	if age <= 0 {
		return 0
	}

	return now - age.Nanoseconds()
}

// signalCount is one past the highest [signal.Signal] value, so a [signal.Signal] indexes a
// bySignal array directly. Index 0 is unused (no signal has that value).
const signalCount = int(signal.Exemplar) + 1

// bySignal is one int64 per signal, indexed by [signal.Signal]. It carries both a tenant's
// per-signal byte budgets and the cutoffs they resolve to, which is why it is a plain array: the
// maintenance loop looks one up per engine per cycle, and a map there allocates for nothing.
type bySignal [signalCount]int64

// at returns the value for sig, or 0 for a signal outside the known range.
func (v bySignal) at(sig signal.Signal) int64 {
	if int(sig) >= len(v) {
		return 0
	}

	return v[sig]
}

func (v bySignal) any() bool {
	for _, n := range v {
		if n > 0 {
			return true
		}
	}

	return false
}

// sizeBudgets is a tenant's resolved byte budgets: the pooled one spanning every signal plus the
// per-signal ones. Both apply — a signal is trimmed to whichever binds first — so the pooled budget
// stays the tenant-wide outer bound while a per-signal budget isolates one signal from the others'
// growth.
type sizeBudgets struct {
	pooled    int64
	perSignal bySignal
}

// budgetsOf resolves a retention policy's byte budgets. Entries for unknown signals are ignored.
func budgetsOf(r tenant.Retention) sizeBudgets {
	b := sizeBudgets{pooled: r.MaxBytes}

	for sig, n := range r.MaxBytesPerSignal {
		if int(sig) < len(b.perSignal) {
			b.perSignal[sig] = n
		}
	}

	return b
}

// empty reports whether no budget is set, which is the common case and must cost nothing: no part
// enumeration, no fingerprint, no memo entry.
func (b sizeBudgets) empty() bool { return b.pooled <= 0 && !b.perSignal.any() }

// covers reports whether any budget bounds sig, and so whether its parts have to be measured. A
// tenant that budgets only its logs pays no metric-part enumeration.
func (b sizeBudgets) covers(sig signal.Signal) bool {
	return b.pooled > 0 || b.perSignal.at(sig) > 0
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
// part" instead of emptying the set — parts here are not time-bounded (one merge can hold a
// tenant's whole history unless [tenant.Limits.MaxPartSize] bounds them), and silently discarding
// everything on a too-small budget is the worse failure. Size the parts to get a tighter bound. The
// caller decides what the set is: a per-signal budget sees one signal's parts, so the floor it
// leaves is one part of that signal, not one part of the tenant.
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

// sizeCutoffCached is [Storage.sizeCutoffFor] without its backend reads: the last memoized cutoffs,
// and whether they belong to the tenant's current part set. A flush or merge since the last
// maintenance cycle makes them stale; the next cycle re-measures.
func (s *Storage) sizeCutoffCached(t signal.TenantID) (cutoffs bySignal, current bool) {
	t = s.normalizeTenant(t)

	b := budgetsOf(s.tenant.Resolve(t).Retention)
	if b.empty() {
		return bySignal{}, true
	}

	return s.sizeRetention.latest(t, s.partSetFingerprint(t, b))
}

// sizeCutoffFor resolves the size-retention cutoffs of one tenant (a real tenant id, not a shard
// key), one per signal: a zero cutoff where the signal has no budget, is already under it, or its
// part sizes cannot be read. Every budget spans all of the tenant's locally-held shards, since it
// bounds the bytes this node stores for it.
//
// The pooled budget ([tenant.Retention.MaxBytes]) and the per-signal ones
// ([tenant.Retention.MaxBytesPerSignal]) both apply: a signal's cutoff is the later of the two, so
// the pooled budget remains the tenant-wide outer bound while a per-signal budget keeps one signal's
// growth from evicting the others' history.
//
// It reads per-part object sizes from the backend (like [Storage.EfficiencyStats]), so it is only
// called for tenants that actually set a budget — and, within that, only for the signals a budget
// covers, and only when the answer can have changed: the cutoffs are a pure function of the tenant's
// part set and its budgets, so they are memoized against a fingerprint of both
// ([Storage.partSetFingerprint], in-memory and I/O-free). Parts are immutable, so on a node where
// nothing flushed, merged, or was dropped since the last cycle the enumeration would re-read every
// part's object sizes to arrive at the same numbers.
func (s *Storage) sizeCutoffFor(ctx context.Context, t signal.TenantID) bySignal {
	t = s.normalizeTenant(t) // engines are keyed by the normalized id

	b := budgetsOf(s.tenant.Resolve(t).Retention)
	if b.empty() {
		return bySignal{}
	}

	fp := s.partSetFingerprint(t, b)
	if cutoffs, ok := s.sizeRetention.lookup(t, fp); ok {
		return cutoffs
	}

	parts, err := s.sizedParts(ctx, t, b)
	if err != nil {
		// Best-effort, like the rest of the cycle: without sizes we fall back to age-only retention
		// rather than dropping data on a guess.
		s.obs.Logger(ctx).Warn("size retention: part sizes unavailable",
			zap.String("tenant", string(t)), zap.Error(err))

		return bySignal{}
	}

	var cutoffs bySignal

	if b.pooled > 0 {
		var all []sizedPart
		for _, ps := range parts {
			all = append(all, ps...) // a fresh slice: the pooled sort must not reorder the buckets
		}

		pooled := sizeRetentionCutoff(all, b.pooled)
		for sig := range cutoffs {
			cutoffs[sig] = pooled
		}
	}

	for sig := range cutoffs {
		cutoffs[sig] = max(cutoffs[sig], sizeRetentionCutoff(parts[sig], b.perSignal[sig]))
	}

	s.sizeRetention.store(t, fp, cutoffs)

	return cutoffs
}

// sizedParts collects the flushed parts this node holds for a tenant, bucketed by signal and pooled
// across the tenant's shards. Signals no budget covers are skipped: measuring them would be pure
// backend I/O for a number nothing reads.
func (s *Storage) sizedParts(ctx context.Context, t signal.TenantID, b sizeBudgets) ([signalCount][]sizedPart, error) {
	var out [signalCount][]sizedPart

	if b.covers(signal.Metric) {
		for tid, eng := range s.engineSnapshotByTenant() {
			if tenantOfShard(tid) != t {
				continue
			}

			parts, err := eng.PartsDetailed(ctx)
			if err != nil {
				return out, err
			}

			for _, p := range parts {
				out[signal.Metric] = append(out[signal.Metric], sizedPart{maxTime: p.MaxTime, bytes: p.Bytes})
			}
		}
	}

	for _, sig := range recordSignals {
		if !b.covers(sig) {
			continue
		}

		for tid, eng := range s.recordEngineSnapshot(sig) {
			if tenantOfShard(tid) != t {
				continue
			}

			parts, err := eng.PartsDetailed(ctx)
			if err != nil {
				return out, err
			}

			for _, p := range parts {
				out[sig] = append(out[sig], sizedPart{maxTime: p.MaxTime, bytes: p.Bytes})
			}
		}
	}

	return out, nil
}

// sizeCutoffs resolves the per-signal size-retention cutoffs of every tenant behind the given shard
// keys, keyed by shard key so a maintenance cycle can look one up per engine. Tenants with no byte
// budget are absent (the common case costs nothing); a tenant sharded across engines is resolved
// once.
func (s *Storage) sizeCutoffs(ctx context.Context, tids map[signal.TenantID]struct{}) map[signal.TenantID]bySignal {
	var (
		byTenant map[signal.TenantID]bySignal
		out      map[signal.TenantID]bySignal
	)

	resolved := make(map[signal.TenantID]struct{}, len(tids))

	for tid := range tids {
		t := s.normalizeTenant(tenantOfShard(tid)) // the memo is keyed by the normalized id
		resolved[t] = struct{}{}

		cutoffs, ok := byTenant[t]
		if !ok {
			cutoffs = s.sizeCutoffFor(ctx, t)

			if byTenant == nil {
				byTenant = make(map[signal.TenantID]bySignal)
			}

			byTenant[t] = cutoffs
		}

		if cutoffs.any() {
			if out == nil {
				out = make(map[signal.TenantID]bySignal)
			}

			out[tid] = cutoffs
		}
	}

	s.sizeRetention.retain(resolved)

	return out
}
