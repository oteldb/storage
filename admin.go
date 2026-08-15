package storage

import (
	"context"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/signal"
)

// ErrNotOwner is returned by an [Admin] flush/compact when this node is not the cluster
// compaction-owner (ring primary) of the tenant/shard, so it must not write that shard's parts.
var ErrNotOwner = errors.New("storage: this node is not the compaction owner of the tenant/shard")

// Admin is the imperative operator-control surface, complementing the background maintenance loop:
// force a flush, compaction, retention sweep, or ownership reconciliation on demand. It is the
// surface a CLI/UI drives. Obtain it from [Storage.Admin]; it holds no state of its own.
//
// The key argument is the engine key — the tenant id in the default layout, or a metric shard key
// ({tenant}/_s{idx}) when [Options.Cluster] sets ShardsPerTenant > 1. In cluster mode flush/compact
// act only on shards this node is the ring-primary of (else [ErrNotOwner]), so a shard's parts are
// still written by exactly one node — the same invariant the maintenance loop preserves.
type Admin struct{ s *Storage }

// Admin returns the operator-control surface for on-demand maintenance.
func (s *Storage) Admin() Admin { return Admin{s} }

// Flush drains a tenant/shard's in-memory head for one signal to an immutable part now. It is a
// no-op (nil) when nothing has been ingested for that key+signal. In cluster mode it returns
// [ErrNotOwner] unless this node is the shard's ring-primary.
func (a Admin) Flush(ctx context.Context, key signal.TenantID, sig signal.Signal) error {
	if a.s.closed.Load() {
		return errors.Wrap(ErrClosed, "admin flush")
	}

	if err := a.s.adminOwns(key); err != nil {
		return err
	}

	fn, ok := a.flushFn(sig, key)
	if !ok {
		return nil // nothing ingested for this key+signal
	}

	return fn(ctx)
}

// Compact merges a tenant/shard's parts for one signal now, applying the tenant's policy
// (retention cutoff, plus downsampling/recompression/precision for metrics) — the same merge the
// background loop runs, so there is no parallel code path. No-op when nothing is ingested; returns
// [ErrNotOwner] in cluster mode unless this node is the shard's ring-primary.
func (a Admin) Compact(ctx context.Context, key signal.TenantID, sig signal.Signal) error {
	if a.s.closed.Load() {
		return errors.Wrap(ErrClosed, "admin compact")
	}

	if err := a.s.adminOwns(key); err != nil {
		return err
	}

	fn, ok := a.compactFn(ctx, sig, key)
	if !ok {
		return nil
	}

	return fn(ctx)
}

// CompactNow compacts a tenant/shard's parts for one signal even when the selector would decline —
// the escape from the fixed point where parts remain mergeable ([SignalStats.MergeBacklog] > 0) but
// none of them qualify ([SignalStats.MergeCandidates] == 0), which a plain [Admin.Compact] or
// [Admin.MaintainNow] cannot break because a cycle that selects nothing is a no-op.
//
// It is the same merge, with the *selection heuristic* overridden and nothing else: the seal
// threshold, the cumulative-bytes cap and the merge memory bound still apply, so a forced compaction
// reads, writes and holds no more than a background one. One call compacts one group; call it again
// to make further progress. No-op when nothing is ingested for the key+signal; [ErrNotOwner] in
// cluster mode unless this node is the shard's ring-primary.
func (a Admin) CompactNow(ctx context.Context, key signal.TenantID, sig signal.Signal) error {
	if a.s.closed.Load() {
		return errors.Wrap(ErrClosed, "admin compact now")
	}

	if err := a.s.adminOwns(key); err != nil {
		return err
	}

	norm := a.s.normalizeTenant(key)

	if sig == signal.Metric {
		eng, ok := a.s.lookupEngine(norm)
		if !ok {
			return nil
		}

		opts := a.s.metricMergeOptions(key, a.s.sizeCutoffFor(ctx, tenantOfShard(key)))
		opts.Force = true

		return eng.MergeWith(ctx, opts)
	}

	eng, ok := a.s.lookupRecordEngine(sig, norm)
	if !ok {
		return nil
	}

	cutoff := a.s.retainFrom(key, a.s.sizeCutoffFor(ctx, tenantOfShard(key)))

	return eng.MergeWith(ctx, recordengine.MergeOptions{RetainFrom: cutoff, Force: true})
}

// Retention forces a retention sweep across all of a tenant/shard's signals by compacting each
// (a merge drops parts older than the policy's cutoff). Signals this node does not own are skipped.
func (a Admin) Retention(ctx context.Context, key signal.TenantID) error {
	for _, sig := range []signal.Signal{signal.Metric, signal.Log, signal.Trace, signal.Profile} {
		if err := a.Compact(ctx, key, sig); err != nil && !errors.Is(err, ErrNotOwner) {
			return err
		}
	}

	return nil
}

// PruneIdentities drops the identities retention has left without data across every one of a
// tenant/shard's signals, and returns how many it removed in total. The background maintenance does
// this after anything that changed the part set, but only once enough has died to pay for the
// rebuild; this ignores those thresholds, so an operator can reclaim immediately — after a
// cardinality incident, say — and see the number rather than a silent no-op. Watch the effect on
// `SignalStats.IdentityBytes`.
//
// Signals with no engine on this node contribute 0; ones it does not own are refused.
func (a Admin) PruneIdentities(ctx context.Context, key signal.TenantID) (int, error) {
	if a.s.closed.Load() {
		return 0, errors.Wrap(ErrClosed, "admin prune identities")
	}

	if err := a.s.adminOwns(key); err != nil {
		return 0, err
	}

	norm := a.s.normalizeTenant(key)
	total := 0

	if eng, ok := a.s.lookupEngine(norm); ok {
		n, err := eng.PruneIdentitiesWith(ctx, engine.PruneOptions{Force: true})
		if err != nil {
			return total, errors.Wrap(err, "prune metric identities")
		}

		total += n
	}

	for _, sig := range []signal.Signal{signal.Log, signal.Trace, signal.Profile} {
		eng, ok := a.s.lookupRecordEngine(sig, norm)
		if !ok {
			continue
		}

		n, err := eng.PruneIdentitiesWith(ctx, recordengine.PruneOptions{Force: true})
		if err != nil {
			return total, errors.Wrapf(err, "prune %s identities", sig)
		}

		total += n
	}

	return total, nil
}

// Rebalance triggers an immediate cluster ownership reconciliation (the maintenance loop otherwise
// does it on its tick), so a freshly-changed ring takes effect without waiting. It is a no-op in
// single-node mode.
func (a Admin) Rebalance(ctx context.Context) error {
	if a.s.closed.Load() {
		return errors.Wrap(ErrClosed, "admin rebalance")
	}

	if a.s.cluster == nil {
		return nil
	}

	shards := a.s.allEngineKeys()

	_, err := a.s.cluster.ownership.Reconcile(ctx, a.s.cluster.membership.Ring(), shards)

	return errors.Wrap(err, "reconcile ownership")
}

// MaintainNow runs one full maintenance cycle immediately — flush + merge + retention across every
// owned tenant and signal (the background loop's body). Best-effort: per-engine errors are logged,
// not returned, matching the loop.
func (a Admin) MaintainNow(ctx context.Context) error {
	if a.s.closed.Load() {
		return errors.Wrap(ErrClosed, "admin maintain")
	}

	a.s.maintain(ctx)

	return nil
}

// flushFn resolves the flush closure for a key+signal, or (nil, false) when no engine exists.
func (a Admin) flushFn(sig signal.Signal, key signal.TenantID) (func(context.Context) error, bool) {
	if sig == signal.Metric {
		eng, ok := a.s.lookupEngine(a.s.normalizeTenant(key))
		if !ok {
			return nil, false
		}

		return eng.Flush, true
	}

	eng, ok := a.s.lookupRecordEngine(sig, a.s.normalizeTenant(key))
	if !ok {
		return nil, false
	}

	return eng.Flush, true
}

// compactFn resolves the compaction closure for a key+signal (with the tenant's resolved merge
// policy, size retention included), or (nil, false) when no engine exists.
func (a Admin) compactFn(ctx context.Context, sig signal.Signal, key signal.TenantID) (func(context.Context) error, bool) {
	if sig == signal.Metric {
		eng, ok := a.s.lookupEngine(a.s.normalizeTenant(key))
		if !ok {
			return nil, false
		}

		opts := a.s.metricMergeOptions(key, a.s.sizeCutoffFor(ctx, tenantOfShard(key)))

		return func(ctx context.Context) error { return eng.MergeWith(ctx, opts) }, true
	}

	eng, ok := a.s.lookupRecordEngine(sig, a.s.normalizeTenant(key))
	if !ok {
		return nil, false
	}

	cutoff := a.s.retainFrom(key, a.s.sizeCutoffFor(ctx, tenantOfShard(key)))

	return func(ctx context.Context) error { return eng.Merge(ctx, cutoff) }, true
}

// adminOwns gates a flush/compact in cluster mode: it succeeds only when this node is the ring
// primary of the key's shard (so exactly one node writes its parts). Single-node always owns.
func (s *Storage) adminOwns(key signal.TenantID) error {
	if s.cluster == nil {
		return nil
	}

	norm := string(s.normalizeTenant(key))

	primary, ok := s.cluster.membership.Ring().Primary([]byte(norm))
	if ok && s.cluster.membership.AddrOf(primary.ID) == s.cluster.self {
		return nil
	}

	return errors.Wrapf(ErrNotOwner, "tenant/shard %q", norm)
}

// allEngineKeys returns the union of every engine's key across all signals (normalized), for an
// ownership reconcile. Engines are lazily created and retained, so this is the full known set.
func (s *Storage) allEngineKeys() []string {
	seen := make(map[signal.TenantID]struct{})

	for tid := range s.engineSnapshotByTenant() {
		seen[tid] = struct{}{}
	}

	for tid := range s.logEngineSnapshotByTenant() {
		seen[tid] = struct{}{}
	}

	for tid := range s.traceEngineSnapshotByTenant() {
		seen[tid] = struct{}{}
	}

	for tid := range s.profileEngineSnapshotByTenant() {
		seen[tid] = struct{}{}
	}

	keys := make([]string, 0, len(seen))
	for tid := range seen {
		keys = append(keys, string(s.normalizeTenant(tid)))
	}

	return keys
}
