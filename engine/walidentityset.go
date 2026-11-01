package engine

import (
	"sync/atomic"

	"github.com/oteldb/storage/index/series"
	"github.com/oteldb/storage/internal/memsize"
)

const walIdentitySegmentWords = 1024

type walIdentitySegment struct {
	words [walIdentitySegmentWords]atomic.Uint64
}

type walIdentitySet struct {
	registry *series.Index
	epoch    uint64
	segments []*walIdentitySegment
}

// Preparation excludes writers; ordinary segment rotation does not call it with a new epoch.
func (s *walIdentitySet) prepare(registry *series.Index, epoch uint64, refs int) {
	if registry != s.registry || epoch != s.epoch {
		for _, segment := range s.segments {
			for i := range segment.words {
				segment.words[i].Store(0)
			}
		}
		s.registry, s.epoch = registry, epoch
	}
	needed := 0
	if refs > 0 {
		needed = (refs-1)/(walIdentitySegmentWords*64) + 1
	}
	for len(s.segments) < needed {
		s.segments = append(s.segments, &walIdentitySegment{})
	}
}

func (s *walIdentitySet) contains(ref uint32) bool {
	segment := uint64(ref) / (walIdentitySegmentWords * 64)
	if segment >= uint64(len(s.segments)) {
		return false
	}
	word, bit := ref/64%walIdentitySegmentWords, ref%64
	return s.segments[segment].words[word].Load()&(uint64(1)<<bit) != 0
}

// Mark only after WAL success while retaining the SeriesID reservation.
func (s *walIdentitySet) mark(ref uint32) {
	segment := uint64(ref) / (walIdentitySegmentWords * 64)
	if segment >= uint64(len(s.segments)) {
		panic("WAL identity marker outside prepared registry extent")
	}
	word, bit := ref/64%walIdentitySegmentWords, ref%64
	s.segments[segment].words[word].Or(uint64(1) << bit)
}

func (s *walIdentitySet) bytes() int64 {
	return memsize.Slice(s.segments) + int64(len(s.segments))*memsize.Of[walIdentitySegment]()
}
