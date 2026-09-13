package storage

import (
	"maps"
	"sync"

	"github.com/oteldb/storage/signal"
)

// sizeRetentionCache memoizes each tenant's per-signal size-retention cutoffs against the part set
// (and budgets) they were computed from. Computing them enumerates every part's objects on the
// backend, while the maintenance loop runs on the flush cadence — without this, a tenant with a byte
// budget pays a full part enumeration every tick forever, whether or not anything was written.
//
// The memoization is exact rather than a heuristic: parts are immutable and the cutoffs are a pure
// function of their time bounds, byte sizes, and the budgets. The one thing that can move a part's
// byte size under a stable part set is erasure coding (full copies → shards), which forgets the
// tenant's entry as it converts — see [Storage.convertColdParts].
//
// One entry covers all four signals: a change to any of a tenant's parts re-resolves every signal's
// cutoff, which costs exactly the one enumeration a single-signal invalidation would.
type sizeRetentionCache struct {
	mu       sync.Mutex
	byTenant map[signal.TenantID]sizeRetentionEntry
}

// sizeRetentionEntry is one tenant's memoized per-signal cutoffs and the part-set fingerprint they
// belong to.
type sizeRetentionEntry struct {
	parts   uint64
	cutoffs bySignal
}

// lookup returns the cutoffs memoized for the given part-set fingerprint, if they are still current.
func (c *sizeRetentionCache) lookup(t signal.TenantID, parts uint64) (bySignal, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	e, ok := c.byTenant[t]
	if !ok || e.parts != parts {
		return bySignal{}, false
	}

	return e.cutoffs, true
}

func (c *sizeRetentionCache) store(t signal.TenantID, parts uint64, cutoffs bySignal) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.byTenant == nil {
		c.byTenant = make(map[signal.TenantID]sizeRetentionEntry)
	}

	c.byTenant[t] = sizeRetentionEntry{parts: parts, cutoffs: cutoffs}
}

// forget drops a tenant's entry — every signal's cutoff — forcing the next cycle to re-measure. Used
// when a part's stored bytes change without its identity changing (erasure coding).
func (c *sizeRetentionCache) forget(t signal.TenantID) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.byTenant, t)
}

// retain drops every tenant outside keep, so tenants this node no longer holds (rebalance, a
// policy that stopped setting a budget) do not pin entries.
func (c *sizeRetentionCache) retain(keep map[signal.TenantID]struct{}) {
	c.mu.Lock()
	defer c.mu.Unlock()

	maps.DeleteFunc(c.byTenant, func(t signal.TenantID, _ sizeRetentionEntry) bool {
		_, ok := keep[t]

		return !ok
	})
}

// partSetFingerprint hashes the identity of every part this node holds for tenant t, across the
// signals a budget covers and across shards, together with the budgets the cutoffs are resolved
// against. It reads the engines' in-memory part lists ([engine.Engine.Parts]) — no backend I/O — so
// it is cheap enough to run on every maintenance cycle as the guard on the expensive enumeration.
//
// It walks exactly the signals [Storage.sizedParts] measures, so an unbudgeted signal's churn
// neither invalidates the memo nor costs a walk. Part prefixes carry the signal ({tenant}/metrics,
// /logs, …), so two signals' parts cannot cancel each other in the XOR.
//
// Engine maps iterate in random order, so parts are combined with XOR (order-independent); the part
// count and every budget are folded in afterwards — a budget that moves must invalidate the entry,
// including a per-signal one.
func (s *Storage) partSetFingerprint(t signal.TenantID, b sizeBudgets) uint64 {
	var mix, count uint64

	if b.covers(signal.Metric) {
		for tid, eng := range s.engineSnapshotByTenant() {
			if tenantOfShard(tid) != t {
				continue
			}

			for _, p := range eng.Parts() {
				mix ^= hashPartID(p.ID, p.MaxTime)
				count++
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

			for _, p := range eng.Parts() {
				mix ^= hashPartID(p.ID, p.MaxTime)
				count++
			}
		}
	}

	h := hashUint64(hashUint64(mix, count), uint64(b.pooled))
	for _, n := range b.perSignal {
		h = hashUint64(h, uint64(n))
	}

	return h
}

// FNV-1a, inlined so hashing a part list allocates nothing.
const (
	fnvOffset64 uint64 = 14695981039346656037
	fnvPrime64  uint64 = 1099511628211
)

// hashPartID hashes one part's identity: its object prefix and upper time bound (both fixed for the
// life of the part, and both inputs to the cutoff).
func hashPartID(id string, maxTime int64) uint64 {
	h := fnvOffset64
	for i := range len(id) {
		h ^= uint64(id[i])
		h *= fnvPrime64
	}

	return hashUint64(h, uint64(maxTime))
}

func hashUint64(h, v uint64) uint64 {
	for range 8 {
		h ^= v & 0xff
		h *= fnvPrime64
		v >>= 8
	}

	return h
}
