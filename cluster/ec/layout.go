package ec

import (
	"path"
	"strconv"
	"strings"
)

// The on-backend layout of an erasure-coded part. A full-copy part stores each object at
// {partPrefix}/{object}; an EC part stores, on the node holding shard slot i, only
// {partPrefix}/ecshard/{i}/{object}, plus the [Meta] sidecar at {partPrefix}/ecmeta on every
// owner. Objects smaller than [FullCopyFloor] are cheaper as full copies than as k+m tiny
// shards, so they stay at their plain key on every owner and are absent from the sidecar.

// MetaObject is the object name of the per-part [Meta] sidecar, relative to the part prefix.
const MetaObject = "ecmeta"

// FullCopyFloor is the object size (bytes) below which an object in an EC part stays
// full-copy on every owner instead of being sharded: k+m shards of a tiny object cost more
// (framing, per-object round-trips) than the bytes they save.
const FullCopyFloor = 4 << 10

// MetaKey returns the backend key of a part's EC sidecar.
func MetaKey(partPrefix string) string { return partPrefix + "/" + MetaObject }

// ShardKey returns the backend key under which shard slot holds object (a part-relative name
// like "c/0") for the part at partPrefix.
func ShardKey(partPrefix string, slot int, object string) string {
	return ShardSlotPrefix(partPrefix, slot) + object
}

// ShardSlotPrefix is the backend key prefix under which shard slot's objects live for the part
// at partPrefix — a List of it enumerates exactly that slot's shards (used to confirm a peer
// holds its slot before the owner prunes its staged copy).
func ShardSlotPrefix(partPrefix string, slot int) string {
	return partPrefix + "/ecshard/" + strconv.Itoa(slot) + "/"
}

// shardMarker separates a part prefix from a shard slot in an EC shard key.
const shardMarker = "/ecshard/"

// ShardSlotOf reports whether key is an EC shard object and, if so, its slot index. It is how
// the mirror path filters a peer's listing down to the shards a node should hold (its own
// slot), leaving every non-shard object — full copies, the ecmeta sidecar, the bucket index —
// kept (ok=false).
func ShardSlotOf(key string) (slot int, ok bool) {
	_, rest, found := strings.Cut(key, shardMarker)
	if !found {
		return 0, false
	}

	num, _, ok := strings.Cut(rest, "/") // "{slot}/{object}"
	if !ok {
		return 0, false
	}

	n, err := strconv.Atoi(num)
	if err != nil || n < 0 {
		return 0, false
	}

	return n, true
}

// SplitKey splits a backend key into its part prefix and part-relative object name, keyed off
// the engine's fixed-width numeric part-sequence segment ("{enginePrefix}/{seq:010d}/{object}").
// It reports ok=false for keys not under a part (e.g. the engine's bucket index or identity
// objects).
func SplitKey(key string) (partPrefix, object string, ok bool) {
	segs := strings.Split(key, "/")
	for i, s := range segs[:max(len(segs)-1, 0)] {
		if isSeqSegment(s) {
			return strings.Join(segs[:i+1], "/"), strings.Join(segs[i+1:], "/"), true
		}
	}

	return "", "", false
}

// isSeqSegment reports whether s is a part-sequence path segment: exactly ten digits (the
// flush path's %010d formatting).
func isSeqSegment(s string) bool {
	if len(s) != 10 {
		return false
	}

	if _, err := strconv.Atoi(s); err != nil || path.Clean(s) != s {
		return false
	}

	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}

	return true
}
