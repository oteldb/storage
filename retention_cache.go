package storage

import (
	"maps"
	"sync"

	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/signal"
)

// sizeRetentionCache memoizes each tenant's size-retention cutoff against the part set (and budget)
// it was computed from. Computing it enumerates every part's objects on the backend, while the
// maintenance loop runs on the flush cadence — without this, a tenant with a byte budget pays a full
// part enumeration every tick forever, whether or not anything was written.
//
// The memoization is exact rather than a heuristic: parts are immutable and the cutoff is a pure
// function of their time bounds, byte sizes, and the budget. The one thing that can move a part's
// byte size under a stable part set is erasure coding (full copies → shards), which forgets the
// tenant's entry as it converts — see [Storage.convertColdParts].
type sizeRetentionCache struct {
	mu       sync.Mutex
	byTenant map[signal.TenantID]sizeRetentionEntry
}

// sizeRetentionEntry is one tenant's memoized cutoff and the part-set fingerprint it belongs to.
type sizeRetentionEntry struct {
	parts  uint64
	cutoff int64
}

// lookup returns the cutoff memoized for the given part-set fingerprint, if it is still current.
func (c *sizeRetentionCache) lookup(t signal.TenantID, parts uint64) (int64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	e, ok := c.byTenant[t]
	if !ok || e.parts != parts {
		return 0, false
	}

	return e.cutoff, true
}

func (c *sizeRetentionCache) store(t signal.TenantID, parts uint64, cutoff int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.byTenant == nil {
		c.byTenant = make(map[signal.TenantID]sizeRetentionEntry)
	}

	c.byTenant[t] = sizeRetentionEntry{parts: parts, cutoff: cutoff}
}

// forget drops a tenant's entry, forcing the next cycle to re-measure. Used when a part's stored
// bytes change without its identity changing (erasure coding).
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

// partSetFingerprint hashes the identity of every part this node holds for tenant t, across signals
// and shards, together with the budget the cutoff is resolved against. It reads the engines'
// in-memory part lists ([engine.Engine.Parts]) — no backend I/O — so it is cheap enough to run on
// every maintenance cycle as the guard on the expensive enumeration.
//
// Engine maps iterate in random order, so parts are combined with XOR (order-independent); the part
// count and budget are folded in afterwards.
func (s *Storage) partSetFingerprint(t signal.TenantID, maxBytes int64) uint64 {
	var mix, count uint64

	for tid, eng := range s.engineSnapshotByTenant() {
		if tenantOfShard(tid) != t {
			continue
		}

		for _, p := range eng.Parts() {
			mix ^= hashPartID(p.ID, p.MaxTime)
			count++
		}
	}

	for _, engines := range []map[signal.TenantID]*recordengine.Engine{
		s.logEngineSnapshotByTenant(),
		s.traceEngineSnapshotByTenant(),
		s.profileEngineSnapshotByTenant(),
	} {
		for tid, eng := range engines {
			if tenantOfShard(tid) != t {
				continue
			}

			for _, p := range eng.Parts() {
				mix ^= hashPartID(p.ID, p.MaxTime)
				count++
			}
		}
	}

	return hashUint64(hashUint64(mix, count), uint64(maxBytes))
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
