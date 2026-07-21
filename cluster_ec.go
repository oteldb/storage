package storage

import (
	"context"
	"time"

	"github.com/go-faster/errors"
	"go.uber.org/zap"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/cluster/ec"
	"github.com/oteldb/storage/cluster/partsync"
	"github.com/oteldb/storage/cluster/ring"
	"github.com/oteldb/storage/signal"
	tenantpkg "github.com/oteldb/storage/tenant"
)

// backendFor returns the backend an engine for shardKey should read/write through. In
// shared-nothing cluster mode with an erasure-coding policy on the tenant, it is an [ecBackend]
// wrapper that reconstructs erasure-coded part objects on read; otherwise it is the raw backend.
// The wrapper delegates every write/list/delete to the raw backend — only reads of converted
// parts differ — so flush, partsync mirroring, and the converter all still operate on the plain
// object layout.
func (s *Storage) backendFor(shardKey signal.TenantID) backend.Backend {
	scheme, ok := s.ecSchemeFor(shardKey)
	if !ok {
		return s.backend
	}

	return &ecBackend{inner: s.backend, s: s, shardKey: shardKey, scheme: scheme}
}

// ecSchemeFor resolves the erasure-coding scheme for shardKey, reporting ok=false when EC does
// not apply: single-node, a shared backend (the store owns durability there), or no EC policy
// on the tenant. Owner count is exactly Data+Parity (the tenant's RF is ignored under EC).
func (s *Storage) ecSchemeFor(shardKey signal.TenantID) (ec.Scheme, bool) {
	if s.cluster == nil || !s.cluster.private {
		return ec.Scheme{}, false
	}

	pol := s.tenant.Resolve(s.normalizeTenant(tenantOfShard(shardKey))).Durability.EC
	if pol == nil {
		return ec.Scheme{}, false
	}

	scheme := ec.Scheme{Data: pol.Data, Parity: pol.Parity}
	if scheme.Validate() != nil {
		return ec.Scheme{}, false
	}

	return scheme, true
}

// ownerLookup returns the ring owners for a shard key, using the erasure-coding balanced
// placement (rack/server/disk spread across all Data+Parity slots) for an EC tenant and the
// zone-aware replica placement otherwise. It is the single source of the owner *set* so the
// head-replication targets, the sync peers, and the EC shard placement all agree on the same
// nodes in the same slot order.
func (s *Storage) ownerLookup(shardKey signal.TenantID) []ring.Node {
	r := s.cluster.membership.Ring()
	if _, isEC := s.ecSchemeFor(shardKey); isEC {
		return r.LookupBalanced([]byte(shardKey), s.rfFor(shardKey))
	}

	return r.Lookup([]byte(shardKey), s.rfFor(shardKey))
}

// ecOwners returns the addresses of shardKey's n shard owners in slot order (slot i is owner i),
// used both to place shards and to fetch them for reconstruction. Placement is **rack-aware**:
// [ring.Ring.LookupBalanced] spreads the shards across zones (failure domains) as evenly as
// possible, so losing one rack costs at most ceil(n/zones) shards — kept ≤ Parity when the
// cluster has at least [ec.Scheme.MinZones] zones.
func (s *Storage) ecOwners(shardKey signal.TenantID, n int) []string {
	nodes := s.cluster.membership.Ring().LookupBalanced([]byte(shardKey), n)

	addrs := make([]string, len(nodes))
	for i, node := range nodes {
		addrs[i] = s.cluster.membership.AddrOf(node.ID)
	}

	return addrs
}

// ecZoneShortfall reports the distinct failure domains among shardKey's shard owners and whether
// they meet the scheme's rack-safety floor (scheme.MinZones). Fewer zones than that means some
// rack necessarily holds more than Parity shards, so a whole-rack failure could be
// unrecoverable — the caller warns, but placement still proceeds (best-effort even spread).
func (s *Storage) ecZoneShortfall(shardKey signal.TenantID, scheme ec.Scheme) (zones int, safe bool) {
	nodes := s.cluster.membership.Ring().LookupBalanced([]byte(shardKey), scheme.Shards())

	// Count distinct coarsest-level domains (racks): a rack failure is the worst case, so
	// rack-safety (≤ Parity shards per rack) is the binding constraint. Finer levels (server,
	// disk) balance within that via LookupBalanced.
	seen := make(map[string]struct{}, len(nodes))
	for _, n := range nodes {
		seen[n.DomainAt(0)] = struct{}{}
	}

	return len(seen), len(seen) >= scheme.MinZones()
}

// ecKeepFilter returns the partsync object filter for shardKey: it keeps every non-shard
// object (full copies, the ecmeta sidecar, the bucket index) and, of the erasure-coded shards,
// only this node's own slot — so a replica mirrors one shard per part instead of the whole k+m
// set (each node converges to a single shard, the EC storage target). Returns nil (no filtering)
// when EC does not apply to the tenant, preserving the plain full-copy mirror.
func (s *Storage) ecKeepFilter(shardKey signal.TenantID) partsync.KeepFunc {
	scheme, ok := s.ecSchemeFor(shardKey)
	if !ok {
		return nil
	}

	owners := s.ecOwners(shardKey, scheme.Shards())

	mySlot := -1

	for i, addr := range owners {
		if addr == s.cluster.self {
			mySlot = i

			break
		}
	}

	return func(key string) bool {
		slot, isShard := ec.ShardSlotOf(key)
		if !isShard {
			return true // full copies, ecmeta, index: always mirrored
		}

		return slot == mySlot // of the shards, keep only this node's slot
	}
}

// ecBackend wraps the raw backend for an EC-policy tenant's engine: reads of an erasure-coded
// part object are reconstructed (this node's own shard slot from the local backend, the rest
// pulled from the slot-owning peers over the partsync object endpoint), while a still-full-copy
// object (a hot part not yet converted, or a sub-floor object) is served straight from the
// local backend with its zero-copy view intact. Every write/list/delete/CAS passes through to
// the raw backend unchanged.
type ecBackend struct {
	inner    backend.Backend
	s        *Storage
	shardKey signal.TenantID
	scheme   ec.Scheme
}

var (
	_ backend.Backend = (*ecBackend)(nil)
	_ backend.Viewer  = (*ecBackend)(nil)
)

// Read returns the object under key, reconstructing an erasure-coded part object when no full
// copy is present locally.
func (e *ecBackend) Read(ctx context.Context, key string) ([]byte, error) {
	data, err := e.inner.Read(ctx, key)
	if err == nil {
		return data, nil
	}

	if !errors.Is(err, backend.ErrNotExist) {
		return nil, err
	}

	return e.reconstruct(ctx, key)
}

// ReadView serves a full-copy object as a zero-copy view (the common hot-part path) and
// reconstructs a converted object as fresh bytes (which satisfy the read-only view contract).
func (e *ecBackend) ReadView(ctx context.Context, key string) ([]byte, error) {
	data, err := backend.ReadView(ctx, e.inner, key)
	if err == nil {
		return data, nil
	}

	if !errors.Is(err, backend.ErrNotExist) {
		return nil, err
	}

	return e.reconstruct(ctx, key)
}

func (e *ecBackend) Write(ctx context.Context, key string, data []byte) error {
	return e.inner.Write(ctx, key, data)
}

func (e *ecBackend) Delete(ctx context.Context, key string) error {
	return e.inner.Delete(ctx, key)
}

func (e *ecBackend) List(ctx context.Context, prefix string) ([]string, error) {
	return e.inner.List(ctx, prefix)
}

func (e *ecBackend) PutIfAbsent(ctx context.Context, key string, data []byte) (bool, error) {
	return e.inner.PutIfAbsent(ctx, key, data)
}

func (e *ecBackend) IsEphemeral() bool { return e.inner.IsEphemeral() }

// reconstruct is the counted reader fallback: an object with no local full copy is reassembled
// from shards, and the outcome lands in the EC operator stats (a missing object is not an
// error of the reconstruction machinery and is not counted as one).
func (e *ecBackend) reconstruct(ctx context.Context, key string) ([]byte, error) {
	data, err := e.reader().Read(ctx, key)
	if err != nil {
		if !errors.Is(err, backend.ErrNotExist) {
			e.s.ecStats.reconstructErr.Add(1)
		}

		return nil, err
	}

	e.s.ecStats.reconstructs.Add(1)

	return data, nil
}

// reader builds an [ec.Reader] over the current ring: this node's shard slot (its index in the
// owner list, or -1 when it is not an owner) and a peer fetch that reads the slot-owner's copy.
func (e *ecBackend) reader() *ec.Reader {
	owners := e.s.ecOwners(e.shardKey, e.scheme.Shards())
	self := e.s.cluster.self

	slot := -1

	for i, addr := range owners {
		if addr == self {
			slot = i

			break
		}
	}

	client := &partsync.Client{HTTP: e.s.cluster.httpc}

	return &ec.Reader{
		Local: e.inner,
		Slot:  slot,
		Fetch: func(ctx context.Context, sl int, key string) ([]byte, error) {
			if sl < 0 || sl >= len(owners) {
				return nil, errors.Errorf("ec: slot %d out of range", sl)
			}

			if owners[sl] == self {
				return nil, errors.New("ec: own slot fetched remotely") // guarded by Reader (local first)
			}

			return client.Fetch(ctx, owners[sl], key)
		},
	}
}

// ecPartRef is a part's identity for the cold-part converter: its backend prefix and the
// newest timestamp it holds (the age test is against the whole part, so the max).
type ecPartRef struct {
	prefix  string
	maxTime int64
}

// convertColdParts erasure-codes the compaction owner's parts that are fully older than the
// tenant's EC age threshold (ECScheme.After). It runs after merge on the owned branch of the
// maintenance loop; a no-op when EC does not apply to the tenant. Already-converted parts are
// skipped (cheap sidecar probe), and a conversion failure is logged and left for the next tick
// — the part stays full-copy and readable meanwhile.
func (s *Storage) convertColdParts(ctx context.Context, shardKey signal.TenantID, parts []ecPartRef) {
	scheme, ok := s.ecSchemeFor(shardKey)
	if !ok {
		return
	}

	pol := s.tenant.Resolve(s.normalizeTenant(tenantOfShard(shardKey))).Durability.EC
	cutoff := time.Now().Add(-durationOf(pol)).UnixNano()

	log := s.obs.Logger(ctx)
	zones, rackSafe := s.ecZoneShortfall(shardKey, scheme)

	owners := s.ecOwners(shardKey, scheme.Shards())

	mySlot := indexOfString(owners, s.cluster.self)
	client := &partsync.Client{HTTP: s.cluster.httpc}

	for _, p := range parts {
		meta, converted, err := ec.Converted(ctx, s.backend, p.prefix)
		if err != nil {
			log.Warn("ec: sidecar probe failed", zap.String("part", p.prefix), zap.Error(err))

			continue
		}

		if !converted {
			if p.maxTime >= cutoff {
				continue // still hot: keep full-copy for fast reads
			}

			if !rackSafe {
				// The topology cannot place this scheme rack-safely (a zone holds > Parity
				// shards), so a whole-rack failure may be unrecoverable. Convert anyway — the
				// shards are still spread as evenly as the zones allow — but flag the shortfall.
				log.Warn("ec: placement not rack-safe; a single zone failure may be unrecoverable",
					zap.String("part", p.prefix), zap.Int("zones", zones),
					zap.Int("need_zones", scheme.MinZones()), zap.Int("parity", scheme.Parity))
			}

			if meta, err = ec.Convert(ctx, s.backend, p.prefix, scheme); err != nil {
				s.ecStats.convertErrors.Add(1)
				log.Warn("ec: convert failed", zap.String("part", p.prefix), zap.Error(err))

				continue
			}
			s.ecStats.converted.Add(1)

			log.Debug("ec: converted cold part",
				zap.String("part", p.prefix), zap.Int("data", scheme.Data),
				zap.Int("parity", scheme.Parity), zap.Int("zones", zones))
		}

		// The compaction owner stages every shard on conversion; once each slot-owner peer holds
		// its shard, drop the staged foreign copies so this node keeps only its own slot — the
		// EC storage target. Reconstruction then fetches a foreign slot from its owner.
		s.pruneStagedShards(ctx, client, p.prefix, meta, scheme, owners, mySlot)
	}
}

// pruneStagedShards deletes the owner's staged copies of every slot it does not own, but only
// after confirming each of those slots is present on its own owner (so a prune never drops the
// last copy of a shard). A no-op when this node holds no foreign shards, when it is not an owner,
// or when the ring is too small to place every slot (the extra copies stay as redundancy).
func (s *Storage) pruneStagedShards(
	ctx context.Context, client *partsync.Client, prefix string, meta *ec.Meta,
	scheme ec.Scheme, owners []string, mySlot int,
) {
	if mySlot < 0 || !s.ownerHasForeignShards(ctx, prefix, mySlot) {
		return
	}

	for slot := range scheme.Shards() {
		if slot == mySlot {
			continue
		}

		if slot >= len(owners) || owners[slot] == s.cluster.self {
			return // slot unplaced (ring smaller than k+m) or aliased to self: keep the staged copy
		}

		if !peerHoldsSlot(ctx, client, owners[slot], prefix, slot, meta) {
			return // not yet confirmed on its owner — keep every staged copy for now
		}
	}

	for slot := range scheme.Shards() {
		if slot == mySlot {
			continue
		}

		for _, o := range meta.Objects {
			if err := s.backend.Delete(ctx, ec.ShardKey(prefix, slot, o.Name)); err != nil && !errors.Is(err, backend.ErrNotExist) {
				s.obs.Logger(ctx).Warn("ec: prune staged shard failed",
					zap.String("part", prefix), zap.Int("slot", slot), zap.Error(err))

				return
			}
		}
	}

	s.ecStats.prunedStaged.Add(1)
	s.obs.Logger(ctx).Debug("ec: pruned staged shards to own slot",
		zap.String("part", prefix), zap.Int("slot", mySlot))
}

// ownerHasForeignShards reports whether this node's backend still holds any shard slot other
// than its own for the part (i.e. staged copies pending distribution).
func (s *Storage) ownerHasForeignShards(ctx context.Context, prefix string, mySlot int) bool {
	keys, err := s.backend.List(ctx, prefix+"/ecshard/")
	if err != nil {
		return false
	}

	for _, k := range keys {
		if slot, ok := ec.ShardSlotOf(k); ok && slot != mySlot {
			return true
		}
	}

	return false
}

// peerHoldsSlot reports whether the peer at addr holds every object of the part's given shard
// slot (an unreachable peer or a missing object reads as "not yet").
func peerHoldsSlot(ctx context.Context, client *partsync.Client, addr, prefix string, slot int, meta *ec.Meta) bool {
	keys, err := client.List(ctx, addr, ec.ShardSlotPrefix(prefix, slot))
	if err != nil {
		return false
	}

	have := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		have[k] = struct{}{}
	}

	for _, o := range meta.Objects {
		if _, ok := have[ec.ShardKey(prefix, slot, o.Name)]; !ok {
			return false
		}
	}

	return true
}

// indexOfString returns the index of v in s, or -1.
func indexOfString(s []string, v string) int {
	for i, x := range s {
		if x == v {
			return i
		}
	}

	return -1
}

// durationOf is the EC age threshold; a nil policy (EC disabled) yields zero (which would
// convert every part), but convertColdParts is only reached for an EC-enabled tenant.
func durationOf(pol *tenantpkg.ECScheme) time.Duration {
	if pol == nil {
		return 0
	}

	return pol.After
}
