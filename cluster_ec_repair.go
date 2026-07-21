package storage

import (
	"context"

	"github.com/go-faster/errors"
	"go.uber.org/zap"

	"github.com/oteldb/storage/cluster/ec"
	"github.com/oteldb/storage/cluster/partsync"
	"github.com/oteldb/storage/signal"
)

// repairEcShards rebuilds this node's shard slot for the erasure-coded parts it owns but whose
// shard it is missing — the durability counterpart of owner-prune. It runs from the maintenance
// loop for every EC owner (the compaction primary and the replicas alike).
//
// A membership change (a lost node) reassigns shard slots, and the balanced placement can
// **renumber** them, so a surviving shard may sit under a slot index no node currently expects.
// Repair therefore reconstructs by **content, not position**: it discovers which shards each
// current owner physically holds (listing their `ecshard/` objects), gathers any Data valid
// shards, RS-reconstructs the missing ones, and writes this node's positional slot. Once every
// owner has repaired, each owner i again holds shard i — the positional invariant the read path
// relies on is restored, so reads converge without a membership-aware gather on the hot path.
//
// No data is destroyed: a node loss leaves ≥ Data shards on the surviving owners (a lost node is
// never pushed *out* of the owner set, only removed), and repair only ever writes — the stale
// foreign shards a renumber leaves behind are pruned by slot filtering / owner-prune.
func (s *Storage) repairEcShards(ctx context.Context, shardKey signal.TenantID, parts []ecPartRef) {
	scheme, ok := s.ecSchemeFor(shardKey)
	if !ok {
		return
	}

	owners := s.ecOwners(shardKey, scheme.Shards())

	mySlot := indexOfString(owners, s.cluster.self)
	if mySlot < 0 || mySlot >= scheme.Shards() {
		return // not one of this shard's owners
	}

	client := &partsync.Client{HTTP: s.cluster.httpc}
	log := s.obs.Logger(ctx)

	for _, p := range parts {
		meta, converted, err := ec.Converted(ctx, s.backend, p.prefix)
		if err != nil || !converted {
			continue
		}

		if s.holdsSlot(ctx, p.prefix, meta, mySlot) {
			continue // already have this node's slot: nothing to repair
		}

		if err := s.rebuildSlot(ctx, client, p.prefix, meta, scheme, owners, mySlot); err != nil {
			log.Warn("ec: shard repair failed",
				zap.String("part", p.prefix), zap.Int("slot", mySlot), zap.Error(err))

			continue
		}

		log.Debug("ec: repaired shard slot", zap.String("part", p.prefix), zap.Int("slot", mySlot))
	}
}

// holdsSlot reports whether this node's backend has every object of the part's given shard slot.
func (s *Storage) holdsSlot(ctx context.Context, prefix string, meta *ec.Meta, slot int) bool {
	keys, err := s.backend.List(ctx, ec.ShardSlotPrefix(prefix, slot))
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

	return len(meta.Objects) > 0
}

// rebuildSlot reconstructs this node's mySlot shard for every object of the part and writes it
// locally. It gathers by content: [Storage.discoverShardSlots] maps each physically-present
// shard slot to the owner holding it, then for each object it fetches Data valid shards, RS-fills
// the missing ones, and stores the mySlot shard (checksum-verified against the sidecar).
func (s *Storage) rebuildSlot(
	ctx context.Context, client *partsync.Client, prefix string, meta *ec.Meta,
	scheme ec.Scheme, owners []string, mySlot int,
) error {
	slotAddr := s.discoverShardSlots(ctx, client, prefix, owners)
	if len(slotAddr) < scheme.Data {
		return errors.Errorf("only %d shard slots reachable, need %d", len(slotAddr), scheme.Data)
	}

	for _, o := range meta.Objects {
		shards := make([][]byte, scheme.Shards())
		have := 0

		for slot, addr := range slotAddr {
			if have >= scheme.Data {
				break
			}

			data := s.fetchShard(ctx, client, addr, prefix, slot, o.Name)
			if data == nil || ec.ChecksumShard(data) != o.Checksums[slot] {
				continue // unreachable or corrupt: never feed a bad shard into the rebuild
			}

			shards[slot] = data
			have++
		}

		if have < scheme.Data {
			return errors.Errorf("object %q: only %d of %d valid shards", o.Name, have, scheme.Data)
		}

		if err := ec.Reconstruct(scheme, shards); err != nil {
			return errors.Wrapf(err, "reconstruct %q", o.Name)
		}

		if ec.ChecksumShard(shards[mySlot]) != o.Checksums[mySlot] {
			return errors.Errorf("object %q: rebuilt slot %d checksum mismatch", o.Name, mySlot)
		}

		if err := s.backend.Write(ctx, ec.ShardKey(prefix, mySlot, o.Name), shards[mySlot]); err != nil {
			return errors.Wrapf(err, "write rebuilt shard %q", o.Name)
		}
	}

	return nil
}

// discoverShardSlots maps each physically-present shard slot of the part to an owner that holds
// it (this node's local backend first, then peers), so a rebuild can gather shards by their true
// slot index regardless of how a membership change renumbered the owner set. The first holder
// found for a slot wins.
func (s *Storage) discoverShardSlots(ctx context.Context, client *partsync.Client, prefix string, owners []string) map[int]string {
	slotAddr := make(map[int]string, len(owners))

	record := func(addr string, keys []string) {
		for _, k := range keys {
			if slot, ok := ec.ShardSlotOf(k); ok {
				if _, seen := slotAddr[slot]; !seen {
					slotAddr[slot] = addr
				}
			}
		}
	}

	// Local first (free, and covers any staged/old-slot shards this node still holds).
	if keys, err := s.backend.List(ctx, prefix+"/ecshard/"); err == nil {
		record(s.cluster.self, keys)
	}

	for _, addr := range owners {
		if addr == s.cluster.self {
			continue
		}

		if keys, err := client.List(ctx, addr, prefix+"/ecshard/"); err == nil {
			record(addr, keys)
		}
	}

	return slotAddr
}

// fetchShard reads one shard object from addr — the local backend when addr is this node, else
// the peer's partsync object endpoint. Returns nil on any error (the caller treats it as absent).
func (s *Storage) fetchShard(ctx context.Context, client *partsync.Client, addr, prefix string, slot int, object string) []byte {
	key := ec.ShardKey(prefix, slot, object)

	if addr == s.cluster.self {
		data, err := s.backend.Read(ctx, key)
		if err != nil {
			return nil
		}

		return data
	}

	data, err := client.Fetch(ctx, addr, key)
	if err != nil {
		return nil
	}

	return data
}
