package ec

import (
	"context"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
)

// PeerFetch fetches one backend object from the node holding shard slot. The read path uses it
// to gather remote shards; the caller maps a slot to a peer (ring owner order) and its
// transport (the partsync object endpoint). A slot whose node is down returns an error; the
// gatherer just moves on — any Data shards suffice.
type PeerFetch func(ctx context.Context, slot int, key string) ([]byte, error)

// Reader is the reconstructing read path over a node's private backend (issue 108 phase 2,
// milestone: `ecread`): Read serves a key from the local backend when the full object is
// present (hot full-copy parts, sub-floor objects, this node's own materialized copies) and
// otherwise reassembles it from erasure-coded shards — its own slot locally, the rest from
// peers — verifying every shard against the sidecar checksums before it participates in a
// reconstruction. The engine stays unaware: wrap the engine's backend and EC parts read like
// any others (cache with backend.Cached outside this wrapper, so a reconstructed object is
// decoded once).
type Reader struct {
	// Local is the node's private backend.
	Local backend.Backend
	// Slot is this node's shard slot for the engine's shard key (its index in the ring's
	// owner list at rf = Data+Parity), or -1 when this node is not an owner (every shard is
	// fetched from peers).
	Slot int
	// Fetch gathers a remote slot's objects; nil disables remote gathering (only local
	// full copies and this node's own shards are usable — enough for unit tests and
	// single-node repair tooling).
	Fetch PeerFetch
}

// Read returns the object stored under key, reconstructing it from erasure-coded shards when
// no full copy is local. It satisfies the [backend.Backend] Read contract (ErrNotExist when
// the object exists nowhere).
func (r *Reader) Read(ctx context.Context, key string) ([]byte, error) {
	if data, err := backend.ReadView(ctx, r.Local, key); err == nil {
		out := make([]byte, len(data)) // Read hands ownership to the caller; views must be copied
		copy(out, data)

		return out, nil
	} else if !errors.Is(err, backend.ErrNotExist) {
		return nil, err
	}

	partPrefix, object, ok := SplitKey(key)
	if !ok {
		return nil, errors.Wrapf(backend.ErrNotExist, "read %q", key)
	}

	metaRaw, err := backend.ReadView(ctx, r.Local, MetaKey(partPrefix))
	if err != nil {
		if errors.Is(err, backend.ErrNotExist) {
			return nil, errors.Wrapf(backend.ErrNotExist, "read %q (no full copy, no EC sidecar)", key)
		}

		return nil, errors.Wrap(err, "read ec sidecar")
	}

	meta, err := DecodeMeta(metaRaw)
	if err != nil {
		return nil, errors.Wrapf(err, "part %q", partPrefix)
	}

	om, ok := findObject(meta, object)
	if !ok {
		return nil, errors.Wrapf(backend.ErrNotExist, "read %q (not in EC sidecar)", key)
	}

	return r.assemble(ctx, partPrefix, meta.Scheme, om)
}

// assemble gathers any Scheme.Data valid shards of one object — this node's slot from the
// local backend, the rest from peers — verifies each against the sidecar checksum, and
// reconstructs the original bytes. It tolerates up to Scheme.Parity unavailable or corrupt
// shards.
func (r *Reader) assemble(ctx context.Context, partPrefix string, s Scheme, om ObjectMeta) ([]byte, error) {
	shards := make([][]byte, s.Shards())
	have := 0

	get := func(slot int) []byte {
		key := ShardKey(partPrefix, slot, om.Name)

		// Local first for any slot: a node normally holds only its own slot, but right after a
		// conversion (before pruning) or during repair staging it may hold others — reading them
		// locally avoids a needless network hop and makes a fully-local part reconstructible with
		// no Fetch at all.
		if data, err := backend.ReadView(ctx, r.Local, key); err == nil {
			return data
		}

		if r.Fetch == nil {
			return nil
		}

		data, err := r.Fetch(ctx, slot, key)
		if err != nil {
			return nil
		}

		return data
	}

	// Own slot first (free), then data slots (a full systematic set needs no RS decode), then
	// parity. Stop as soon as Data valid shards are in hand.
	order := gatherOrder(s, r.Slot)

	for _, slot := range order {
		if have == s.Data {
			break
		}

		sh := get(slot)
		if sh == nil {
			continue
		}

		if ChecksumShard(sh) != om.Checksums[slot] {
			continue // corrupt shard: never let it into a reconstruction
		}

		shards[slot] = sh
		have++
	}

	if have < s.Data {
		return nil, errors.Errorf("ec: object %q in part %q: only %d of %d required shards available",
			om.Name, partPrefix, have, s.Data)
	}

	if err := Reconstruct(s, shards); err != nil {
		return nil, errors.Wrapf(err, "object %q in part %q", om.Name, partPrefix)
	}

	return Join(s, shards, om.Size)
}

// gatherOrder returns shard slots in preference order: this node's own slot, then data slots,
// then parity slots.
func gatherOrder(s Scheme, self int) []int {
	order := make([]int, 0, s.Shards())
	if self >= 0 && self < s.Shards() {
		order = append(order, self)
	}

	for i := range s.Shards() {
		if i != self {
			order = append(order, i)
		}
	}

	return order
}

// findObject looks an object name up in the sidecar.
func findObject(m *Meta, name string) (ObjectMeta, bool) {
	for _, o := range m.Objects {
		if o.Name == name {
			return o, true
		}
	}

	return ObjectMeta{}, false
}
