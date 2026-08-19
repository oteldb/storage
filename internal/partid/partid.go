// Package partid mints the globally unique identifiers that name a part on a backend.
//
// A part's backend key must be unique across every writer that can ever touch a prefix, not just
// within one process: two engines over one shared prefix (an ownership handoff, a restore from a
// stale index, a rejoin after a lease loss) would otherwise mint the same key for different content
// and overwrite each other's objects. A local counter cannot provide that, so the id is minted from
// a timestamp plus randomness instead of derived from what a node happens to hold.
package partid

import (
	"crypto/rand"
	"encoding/binary"
	"slices"
	"sync"
	"time"
)

// Len is the size of an [ID] in bytes.
const Len = 16

// ID identifies a part: a 48-bit big-endian unix-millisecond timestamp followed by 80 random bits,
// the ULID layout. Its [ID.String] form is base32, so lexicographic order over part prefixes still
// matches creation order — the engine relies on that for stable part ordering.
type ID [Len]byte

var gen struct {
	mu   sync.Mutex
	ms   uint64
	rand [Len - 6]byte
}

// New mints a fresh ID. Safe for concurrent use.
//
// Ids minted by one process are strictly increasing even within a millisecond: the entropy is
// incremented rather than redrawn, which keeps ordering total without weakening uniqueness against
// other writers (each millisecond still starts from a fresh random draw).
func New() ID {
	ms := uint64(time.Now().UnixMilli())

	gen.mu.Lock()
	defer gen.mu.Unlock()

	switch {
	case ms > gen.ms:
		gen.ms = ms
		randomize(gen.rand[:])
	default:
		// Same millisecond, or a clock that stepped backwards: keep the monotonic sequence going.
		// The increment carries into the timestamp on the (practically unreachable) 2^80 overflow.
		if increment(gen.rand[:]) {
			gen.ms++
		}
	}

	var id ID

	binary.BigEndian.PutUint16(id[0:2], uint16(gen.ms>>32))
	binary.BigEndian.PutUint32(id[2:6], uint32(gen.ms))
	copy(id[6:], gen.rand[:])

	return id
}

// Time returns the millisecond timestamp the id was minted at.
func (id ID) Time() time.Time {
	ms := uint64(binary.BigEndian.Uint16(id[0:2]))<<32 | uint64(binary.BigEndian.Uint32(id[2:6]))

	return time.UnixMilli(int64(ms)).UTC()
}

func randomize(b []byte) {
	// crypto/rand.Read never fails since Go 1.24; it panics internally instead.
	_, _ = rand.Read(b)
}

// increment adds one to b as a big-endian integer, reporting whether it wrapped to zero.
func increment(b []byte) bool {
	for i := range slices.Backward(b) {
		b[i]++
		if b[i] != 0 {
			return false
		}
	}

	return true
}
