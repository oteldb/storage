package engine

import (
	"math"

	"github.com/oteldb/storage/internal/memsize"
	"github.com/oteldb/storage/signal"
)

const headSlotBits = 8
const headSlotSize = 1 << headSlotBits

type headWatermarkPage struct {
	values [headSlotSize]int64
	seen   [headSlotSize / 64]uint64
	used   int
}

type headSlots struct {
	pages []*headWatermarkPage
	free  []uint32
	next  uint64
	seen  int
}

func (s *headSlots) alloc() uint32 {
	if n := len(s.free); n > 0 {
		ref := s.free[n-1]
		s.free = s.free[:n-1]
		return ref
	}
	if s.next > math.MaxUint32 {
		panic("engine: head slots exhausted")
	}
	ref := uint32(s.next)
	s.next++
	return ref
}

func (s *headSlots) get(ref uint32) (int64, bool) {
	p := int(ref >> headSlotBits)
	if p >= len(s.pages) || s.pages[p] == nil {
		return 0, false
	}
	page := s.pages[p]
	i := ref & (headSlotSize - 1)
	return page.values[i], page.seen[i>>6]&(uint64(1)<<(i&63)) != 0
}

func (s *headSlots) set(ref uint32, ts int64) {
	p := int(ref >> headSlotBits)
	for len(s.pages) <= p {
		s.pages = append(s.pages, nil)
	}
	if s.pages[p] == nil {
		s.pages[p] = &headWatermarkPage{}
	}
	page := s.pages[p]
	i := ref & (headSlotSize - 1)
	mask := uint64(1) << (i & 63)
	if page.seen[i>>6]&mask == 0 {
		page.seen[i>>6] |= mask
		page.used++
		s.seen++
	}
	page.values[i] = ts
}

func (s *headSlots) release(ref uint32) {
	p := int(ref >> headSlotBits)
	if p < len(s.pages) && s.pages[p] != nil {
		page := s.pages[p]
		i := ref & (headSlotSize - 1)
		mask := uint64(1) << (i & 63)
		if page.seen[i>>6]&mask != 0 {
			page.values[i] = 0
			page.seen[i>>6] &^= mask
			page.used--
			s.seen--
			if page.used == 0 {
				s.pages[p] = nil
			}
		}
	}
	s.free = append(s.free, ref)
}

func (s *headSlots) sizeBytes() int64 {
	n := memsize.Slice(s.pages) + memsize.Slice(s.free)
	for _, p := range s.pages {
		if p != nil {
			n += memsize.Of[headWatermarkPage]()
		}
	}
	return n
}

type headBufferPage [headSlotSize]*sampleBuf

type headBuffers struct {
	pages []*headBufferPage
}

func (s *headBuffers) get(ref uint32) *sampleBuf {
	p := int(ref >> headSlotBits)
	if p >= len(s.pages) || s.pages[p] == nil {
		return nil
	}
	return s.pages[p][ref&(headSlotSize-1)]
}

func (s *headBuffers) set(ref uint32, buf *sampleBuf) {
	p := int(ref >> headSlotBits)
	for len(s.pages) <= p {
		s.pages = append(s.pages, nil)
	}
	if s.pages[p] == nil {
		if buf == nil {
			return
		}
		s.pages[p] = &headBufferPage{}
	}
	s.pages[p][ref&(headSlotSize-1)] = buf
}

func (s *headBuffers) sizeBytes() int64 {
	n := memsize.Slice(s.pages)
	for _, p := range s.pages {
		if p != nil {
			n += memsize.Of[headBufferPage]()
		}
	}
	return n
}

type headSeriesState struct {
	compact    bool
	historical bool
	buffer     *sampleBuf
	newest     int64
	ref        uint32
	seen       bool
	known      bool
}

func (h *head) seriesState(id signal.SeriesID) headSeriesState {
	if newest, ok := h.known.newest(id); ok {
		return headSeriesState{compact: true, historical: true, buffer: h.samples[id], newest: newest, seen: true}
	}
	ref, ok := h.series.Ref(id)
	if !ok {
		if h.disk != nil {
			entry, ok := h.disk.pending[id]
			if !ok {
				entry, _ = h.disk.cache.get(id)
			}
			return headSeriesState{newest: entry.newest, seen: entry.exists, historical: entry.exists}
		}
		return headSeriesState{}
	}
	slot := h.stateRefs[ref]
	newest, seen := h.slots.get(slot)
	return headSeriesState{buffer: h.buffers.get(slot), newest: newest, ref: ref, seen: seen, known: true}
}

func (h *head) setBuffer(id signal.SeriesID, ref uint32, buf *sampleBuf) {
	h.samples[id] = buf
	h.buffers.set(h.stateRefs[ref], buf)
}

func (h *head) bufForRef(id signal.SeriesID, ref uint32) *sampleBuf {
	slot := h.stateRefs[ref]
	buf := h.buffers.get(slot)
	if buf == nil {
		buf = h.memory.buffer()
		h.setBuffer(id, ref, buf)
	}
	return buf
}

func (h *head) watermark(id signal.SeriesID) (int64, bool) {
	if newest, ok := h.known.newest(id); ok {
		return newest, true
	}
	ref, ok := h.series.Ref(id)
	if !ok {
		return 0, false
	}
	return h.slots.get(h.stateRefs[ref])
}

func (h *head) noteTSRef(ref uint32, ts int64) {
	h.noteBufferedTS(ts)
	slot := h.stateRefs[ref]
	if newest, seen := h.slots.get(slot); !seen || ts > newest {
		h.slots.set(slot, ts)
	}
	h.newest = max(h.newest, ts)
}

func (h *head) resetBuffers() {
	h.buffers = headBuffers{}
	if h.series == nil {
		return
	}
	for id, buf := range h.samples {
		if ref, ok := h.series.Ref(id); ok {
			h.buffers.set(h.stateRefs[ref], buf)
		}
	}
}
