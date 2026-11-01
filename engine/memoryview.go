package engine

import (
	"slices"
	"sync/atomic"

	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/index/series"
)

type memoryPartEntry struct {
	data             *part
	minTime, maxTime int64
	blocks           bucketindex.Interval
	claim            bucketindex.Claim
	level            uint32
	term             uint64
	pending          bool
}

type memoryPartSet struct {
	refs atomic.Int64

	entries []memoryPartEntry
}

// The caller protects mutable part metadata and owns the source handles during construction.
func newMemoryPartSet(parts []*part) *memoryPartSet {
	s := &memoryPartSet{entries: make([]memoryPartEntry, len(parts))}
	for i, part := range parts {
		part.acquireView()
		blocks, claim := part.blocks, part.claim
		blocks.Gaps = slices.Clone(blocks.Gaps)
		claim.Blocks.Gaps = slices.Clone(claim.Blocks.Gaps)
		claim.Group.Gaps = slices.Clone(claim.Group.Gaps)
		s.entries[i] = memoryPartEntry{data: part, minTime: part.minTime, maxTime: part.maxTime, blocks: blocks, claim: claim,
			level: part.level, term: part.term, pending: part.pending != nil}
	}
	s.refs.Store(1)
	return s
}

func (s *memoryPartSet) acquire() {
	if s == nil {
		return
	}
	for {
		refs := s.refs.Load()
		if refs <= 0 {
			panic("acquire released part set")
		}
		if s.refs.CompareAndSwap(refs, refs+1) {
			return
		}
	}
}

func (s *memoryPartSet) release() {
	if s == nil {
		return
	}
	refs := s.refs.Add(-1)
	if refs < 0 {
		panic("release unowned part set")
	}
	if refs != 0 {
		return
	}
	for i := range s.entries {
		s.entries[i].data.releaseView()
	}
	s.entries = nil
}

type memoryReadView struct {
	refs atomic.Int64

	active, frozen, recent *memoryDirectoryNode
	parts                  *memoryPartSet
	registry               series.View
	recentMin              int64
	revision               uint64
	onRelease              func()
}

// The destination is reserved before WAL success; the source remains owned until installation ends.
func (v *memoryReadView) inherit(src *memoryReadView) {
	if src != nil {
		v.active, v.frozen, v.recent = src.active, src.frozen, src.recent
		v.parts, v.registry, v.recentMin = src.parts, src.registry, src.recentMin
		v.revision = src.revision + 1
		v.active.acquire()
		v.frozen.acquire()
		v.recent.acquire()
		v.parts.acquire()
	}
	v.refs.Store(1)
}

func (v *memoryReadView) acquire() {
	for {
		refs := v.refs.Load()
		if refs <= 0 {
			panic("acquire released read view")
		}
		if v.refs.CompareAndSwap(refs, refs+1) {
			return
		}
	}
}

// Call outside publication ownership: final release can retire buffers and part-set ownership.
func (v *memoryReadView) release() {
	refs := v.refs.Add(-1)
	if refs < 0 {
		panic("release unowned read view")
	}
	if refs != 0 {
		return
	}
	v.active.release()
	v.frozen.release()
	v.recent.release()
	v.parts.release()
	v.active, v.frozen, v.recent, v.parts = nil, nil, nil, nil
	v.registry = series.View{}
	if release := v.onRelease; release != nil {
		v.onRelease = nil
		release()
	}
}
