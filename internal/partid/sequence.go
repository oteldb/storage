package partid

import (
	"encoding/binary"
	"sync"
	"time"
)

// Sequence mints ordered IDs within a reserved logical millisecond. It is safe for concurrent use.
type Sequence struct {
	mu        sync.Mutex
	next      ID
	exhausted bool
}

// NewSequence reserves a timestamp before subsequent IDs without reducing the random entropy.
func NewSequence() *Sequence {
	gen.mu.Lock()
	defer gen.mu.Unlock()
	ms := max(uint64(time.Now().UnixMilli()), gen.ms+1)
	s := &Sequence{}
	binary.BigEndian.PutUint16(s.next[:2], uint16(ms>>32))
	binary.BigEndian.PutUint32(s.next[2:6], uint32(ms))
	randomize(s.next[6:])
	// Ordinary IDs and the next reservation must sort after every ID in this sequence.
	gen.ms = ms + 1
	randomize(gen.rand[:])
	return s
}

// Next returns a fresh ID or false when the reserved timestamp's entropy space is exhausted.
func (s *Sequence) Next() (ID, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.exhausted {
		return ID{}, false
	}
	id := s.next
	s.exhausted = increment(s.next[6:])
	return id, true
}
