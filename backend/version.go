package backend

import (
	"crypto/sha256"
	"encoding/hex"
)

// Version is an opaque token identifying the exact bytes stored under a key. It is produced by
// the backend ([Backend.ReadVersioned], [Backend.CompareAndSwap]) and consumed by it; its form is
// implementation-defined (an object store's ETag, a content digest) and no other meaning may be
// read into it — two versions are only ever compared for equality, never ordered.
//
// A version identifies *contents*, not a write: rewriting a key with the bytes it already holds
// may leave the version unchanged. That is what makes the token safe against the ABA problem
// here, rather than exposed to it — the state a committer conditions on is the object itself, so
// a value that came back is the value it read.
type Version string

// VersionAbsent is the version of a key holding no object. Passing it to
// [Backend.CompareAndSwap] demands that the key be absent, which makes the create case the same
// call as every other commit.
const VersionAbsent Version = ""

// ContentVersion derives a version token from an object's bytes. It is the token for backends
// with no native one of their own (memory, file); truncating SHA-256 to 128 bits keeps the token
// short while leaving a collision — two distinct index states one writer would mistake for the
// other — out of reach.
func ContentVersion(data []byte) Version {
	sum := sha256.Sum256(data)

	return Version(hex.EncodeToString(sum[:16]))
}
