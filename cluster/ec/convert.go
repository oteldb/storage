package ec

import (
	"context"
	"strings"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
)

// Convert erasure-codes a full-copy part in be to scheme, in place: it shards every part object
// at or above [FullCopyFloor], writes the [Meta] sidecar, and deletes the full copies it
// replaced. It is the compaction owner's cold-part re-encode step (issue 108 phase 2). Objects
// below the floor and the part's other non-object keys are left untouched; the returned Meta
// lists exactly the sharded objects.
//
// All Data+Parity shards are written into be — the owner's backend is the staging area from
// which the shards are distributed to peers and pruned to the owner's own slot (a later
// milestone). Reads work throughout via [Reader], which serves a surviving full copy or
// reconstructs from the shards.
//
// Crash-safety mirrors the flush commit discipline: shards are new keys that leave the full
// copies intact, the sidecar is written next as the commit point, and only then are the full
// copies deleted. A crash before the sidecar leaves a readable full-copy part (the orphan
// shards are overwritten on retry); a crash mid-delete leaves a still-readable part (full copy
// or reconstruction). Convert is idempotent: a part that already has a valid sidecar is not
// re-read (its full copies may be gone) — only any leftover full copies are swept.
func Convert(ctx context.Context, be backend.Backend, partPrefix string, scheme Scheme) (*Meta, error) {
	if err := scheme.Validate(); err != nil {
		return nil, err
	}

	// Already converted? Finish any pending deletes and return the recorded sidecar.
	if raw, err := backend.ReadView(ctx, be, MetaKey(partPrefix)); err == nil {
		meta, derr := DecodeMeta(raw)
		if derr != nil {
			return nil, errors.Wrapf(derr, "part %q sidecar", partPrefix)
		}

		if derr := deleteFullCopies(ctx, be, partPrefix, meta); derr != nil {
			return nil, derr
		}

		return meta, nil
	} else if !errors.Is(err, backend.ErrNotExist) {
		return nil, errors.Wrap(err, "read sidecar")
	}

	objects, err := partObjects(ctx, be, partPrefix)
	if err != nil {
		return nil, err
	}

	meta := &Meta{Scheme: scheme}

	for _, name := range objects {
		data, err := be.Read(ctx, partPrefix+"/"+name)
		if err != nil {
			return nil, errors.Wrapf(err, "read object %q", name)
		}

		if !ShouldShard(int64(len(data))) {
			continue // sub-floor: stays full-copy, absent from the sidecar
		}

		shards, om, err := EncodeObject(scheme, name, data)
		if err != nil {
			return nil, errors.Wrapf(err, "encode object %q", name)
		}

		for i, sh := range shards {
			if err := be.Write(ctx, ShardKey(partPrefix, i, name), sh); err != nil {
				return nil, errors.Wrapf(err, "write shard %d of %q", i, name)
			}
		}

		meta.Objects = append(meta.Objects, om)
	}

	// Commit point: the sidecar is written after every shard, so it never references a shard
	// that is not present.
	if err := be.Write(ctx, MetaKey(partPrefix), meta.AppendBinary(nil)); err != nil {
		return nil, errors.Wrap(err, "write sidecar")
	}

	if err := deleteFullCopies(ctx, be, partPrefix, meta); err != nil {
		return nil, err
	}

	return meta, nil
}

// Converted reports whether the part at partPrefix has been erasure-coded (has a valid
// sidecar), returning the decoded sidecar when so.
func Converted(ctx context.Context, be backend.Backend, partPrefix string) (*Meta, bool, error) {
	raw, err := backend.ReadView(ctx, be, MetaKey(partPrefix))
	if err != nil {
		if errors.Is(err, backend.ErrNotExist) {
			return nil, false, nil
		}

		return nil, false, errors.Wrap(err, "read sidecar")
	}

	meta, err := DecodeMeta(raw)
	if err != nil {
		return nil, false, errors.Wrapf(err, "part %q sidecar", partPrefix)
	}

	return meta, true, nil
}

// partObjects lists the part-relative object names under partPrefix, excluding the EC sidecar
// and any shard objects (so a re-run after a partial conversion does not re-shard shards).
func partObjects(ctx context.Context, be backend.Backend, partPrefix string) ([]string, error) {
	keys, err := be.List(ctx, partPrefix+"/")
	if err != nil {
		return nil, errors.Wrap(err, "list part")
	}

	var out []string

	for _, k := range keys {
		name := strings.TrimPrefix(k, partPrefix+"/")
		if name == MetaObject || strings.HasPrefix(name, "ecshard/") {
			continue
		}

		out = append(out, name)
	}

	return out, nil
}

// deleteFullCopies removes the full-copy object of every sharded entry in meta (idempotent: a
// key already gone is not an error).
func deleteFullCopies(ctx context.Context, be backend.Backend, partPrefix string, meta *Meta) error {
	for _, o := range meta.Objects {
		if err := be.Delete(ctx, partPrefix+"/"+o.Name); err != nil && !errors.Is(err, backend.ErrNotExist) {
			return errors.Wrapf(err, "delete full copy %q", o.Name)
		}
	}

	return nil
}
