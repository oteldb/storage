package engine

import (
	"sync"

	"github.com/oteldb/storage/signal"
)

type seriesOwnershipStripe struct {
	mu   sync.Mutex
	busy map[signal.SeriesID]*preparedBatch
}

type seriesOwnership struct {
	stripes [64]seriesOwnershipStripe
	mu      sync.Mutex
	changed chan struct{}
}

func (p *preparedBatch) prepareOwnership() {
	var offsets [64]int
	for _, id := range p.ownedIDs {
		offsets[id.Lo%64]++
	}
	var total int
	for i, count := range offsets {
		offsets[i], total = total, total+count
	}
	p.ownerOrder = resize(p.ownerOrder, len(p.ownedIDs))
	for i, id := range p.ownedIDs {
		stripe := id.Lo % 64
		p.ownerOrder[offsets[stripe]] = uint32(i)
		offsets[stripe]++
	}
}

func (s *seriesOwnership) try(p *preparedBatch) bool {
	for position := 0; position < len(p.ownerOrder); {
		first := position
		stripe := p.ownedIDs[p.ownerOrder[position]].Lo % 64
		owner := &s.stripes[stripe]
		owner.mu.Lock()
		for position < len(p.ownerOrder) && p.ownedIDs[p.ownerOrder[position]].Lo%64 == stripe {
			if _, busy := owner.busy[p.ownedIDs[p.ownerOrder[position]]]; busy {
				owner.mu.Unlock()
				s.drop(p, first)
				return false
			}
			position++
		}
		if owner.busy == nil {
			owner.busy = make(map[signal.SeriesID]*preparedBatch)
		}
		for _, index := range p.ownerOrder[first:position] {
			owner.busy[p.ownedIDs[index]] = p
		}
		owner.mu.Unlock()
	}
	return true
}

func (s *seriesOwnership) acquire(p *preparedBatch, conflict func()) {
	notified := false
	for {
		s.mu.Lock()
		if s.try(p) {
			s.mu.Unlock()
			return
		}
		if s.changed == nil {
			s.changed = make(chan struct{})
		}
		changed := s.changed
		s.mu.Unlock()
		if !notified && conflict != nil {
			conflict()
			notified = true
		}
		<-changed
	}
}

func (s *seriesOwnership) drop(p *preparedBatch, held int) {
	for position := 0; position < held; {
		stripe := p.ownedIDs[p.ownerOrder[position]].Lo % 64
		owner := &s.stripes[stripe]
		owner.mu.Lock()
		for position < held && p.ownedIDs[p.ownerOrder[position]].Lo%64 == stripe {
			if owner.busy[p.ownedIDs[p.ownerOrder[position]]] != p {
				owner.mu.Unlock()
				panic("release unowned series")
			}
			delete(owner.busy, p.ownedIDs[p.ownerOrder[position]])
			position++
		}
		owner.mu.Unlock()
	}
}

func (s *seriesOwnership) release(p *preparedBatch) {
	s.drop(p, len(p.ownerOrder))
	s.mu.Lock()
	if s.changed != nil {
		close(s.changed)
		s.changed = nil
	}
	s.mu.Unlock()
}
