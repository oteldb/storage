package storage

import (
	"context"
	"time"

	"github.com/go-faster/errors"
	"go.uber.org/zap"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/cluster/ec"
	"github.com/oteldb/storage/cluster/partsync"
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

	return e.reader().Read(ctx, key)
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

	return e.reader().Read(ctx, key)
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

	for _, p := range parts {
		if p.maxTime >= cutoff {
			continue // still hot: keep full-copy for fast reads
		}

		if _, converted, err := ec.Converted(ctx, s.backend, p.prefix); err != nil {
			log.Warn("ec: sidecar probe failed", zap.String("part", p.prefix), zap.Error(err))

			continue
		} else if converted {
			continue
		}

		if !rackSafe {
			// The topology cannot place this scheme rack-safely (a zone holds > Parity shards),
			// so a whole-rack failure may be unrecoverable. Convert anyway — the shards are still
			// spread as evenly as the zones allow — but make the shortfall visible.
			log.Warn("ec: placement not rack-safe; a single zone failure may be unrecoverable",
				zap.String("part", p.prefix), zap.Int("zones", zones),
				zap.Int("need_zones", scheme.MinZones()), zap.Int("parity", scheme.Parity))
		}

		if _, err := ec.Convert(ctx, s.backend, p.prefix, scheme); err != nil {
			log.Warn("ec: convert failed", zap.String("part", p.prefix), zap.Error(err))

			continue
		}

		log.Debug("ec: converted cold part",
			zap.String("part", p.prefix), zap.Int("data", scheme.Data),
			zap.Int("parity", scheme.Parity), zap.Int("zones", zones))
	}
}

// durationOf is the EC age threshold; a nil policy (EC disabled) yields zero (which would
// convert every part), but convertColdParts is only reached for an EC-enabled tenant.
func durationOf(pol *tenantpkg.ECScheme) time.Duration {
	if pol == nil {
		return 0
	}

	return pol.After
}
