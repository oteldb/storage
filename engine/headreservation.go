package engine

import (
	"slices"
	"sync"
)

const publicationStripes = 64

type reservationMask struct {
	word uint32
	mask uint64
}

type publicationStripe struct {
	mu    sync.Mutex
	words map[uint32]uint64
}

type publicationReservations struct {
	stripes [publicationStripes]publicationStripe
}

func (p *preparedBatch) prepareReservation() {
	slices.Sort(p.refs)
	p.masks = p.masks[:0]
	for _, ref := range p.refs {
		word, mask := ref>>6, uint64(1)<<(ref&63)
		if n := len(p.masks); n > 0 && p.masks[n-1].word == word {
			p.masks[n-1].mask |= mask
		} else {
			p.masks = append(p.masks, reservationMask{word: word, mask: mask})
		}
	}
	var counts [publicationStripes]int
	for _, m := range p.masks {
		counts[m.word%publicationStripes]++
	}
	var pos int
	for i, n := range &counts {
		counts[i], pos = pos, pos+n
	}
	p.maskOrder = resize(p.maskOrder, len(p.masks))
	for i, m := range p.masks {
		stripe := m.word % publicationStripes
		p.maskOrder[counts[stripe]] = uint32(i)
		counts[stripe]++
	}
}

func (r *publicationReservations) acquire(p *preparedBatch) bool {
	position := 0
	for position < len(p.maskOrder) {
		first := position
		stripe := p.masks[p.maskOrder[position]].word % publicationStripes
		s := &r.stripes[stripe]
		s.mu.Lock()
		for position < len(p.maskOrder) && p.masks[p.maskOrder[position]].word%publicationStripes == stripe {
			m := p.masks[p.maskOrder[position]]
			if s.words[m.word]&m.mask != 0 {
				s.mu.Unlock()
				r.release(p, first)
				return false
			}
			position++
		}
		if s.words == nil {
			s.words = make(map[uint32]uint64)
		}
		for _, index := range p.maskOrder[first:position] {
			m := p.masks[index]
			s.words[m.word] |= m.mask
		}
		s.mu.Unlock()
	}
	return true
}

func (r *publicationReservations) release(p *preparedBatch, held int) {
	position := 0
	for position < held {
		stripe := p.masks[p.maskOrder[position]].word % publicationStripes
		s := &r.stripes[stripe]
		s.mu.Lock()
		for position < held && p.masks[p.maskOrder[position]].word%publicationStripes == stripe {
			m := p.masks[p.maskOrder[position]]
			if s.words[m.word]&m.mask != m.mask {
				s.mu.Unlock()
				panic("release unowned head reservation")
			}
			remaining := s.words[m.word] &^ m.mask
			if remaining == 0 {
				delete(s.words, m.word)
			} else {
				s.words[m.word] = remaining
			}
			position++
		}
		s.mu.Unlock()
	}
}
