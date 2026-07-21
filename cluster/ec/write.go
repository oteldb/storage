package ec

// ShouldShard reports whether an object of the given size is worth erasure-coding: objects
// under [FullCopyFloor] stay full-copy on every owner (see the layout comment).
func ShouldShard(size int64) bool { return size >= FullCopyFloor }

// EncodeObject shards one part object for the converter/repair paths: it encodes data into
// Scheme.Shards() shards and returns them with the object's sidecar entry (size + per-shard
// checksums) for [Meta.Objects]. The caller stores shard i under [ShardKey] on owner i and
// appends the entry to the part's sidecar.
func EncodeObject(s Scheme, name string, data []byte) ([][]byte, ObjectMeta, error) {
	shards, err := Encode(s, data)
	if err != nil {
		return nil, ObjectMeta{}, err
	}

	om := ObjectMeta{Name: name, Size: int64(len(data)), Checksums: make([]uint64, len(shards))}
	for i, sh := range shards {
		om.Checksums[i] = ChecksumShard(sh)
	}

	return shards, om, nil
}
