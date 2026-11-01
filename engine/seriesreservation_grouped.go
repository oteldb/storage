package engine

import "github.com/oteldb/storage/signal"

// An optimistic attempt never waits while holding IDs; conflicts use the ordered FIFO path.
func (l *seriesReservations) tryGrouped(r *seriesReservation) bool {
	position := 0
	for position < len(r.byStripe) {
		s := l.stripe(r.ids[r.byStripe[position]])
		s.mu.Lock()
		if s.ids == nil {
			s.ids = make(map[signal.SeriesID]*seriesReservationEntry)
		}
		for position < len(r.byStripe) {
			id := r.ids[r.byStripe[position]]
			if l.stripe(id) != s {
				break
			}
			entry := s.ids[id]
			if entry != nil && (entry.owner != nil || entry.first != nil) {
				s.mu.Unlock()
				l.releaseGrouped(r, position)
				return false
			}
			if entry == nil {
				entry = s.take()
				s.ids[id] = entry
			}
			entry.owner = r
			position++
		}
		s.mu.Unlock()
	}
	r.held, r.grouped = len(r.ids), true
	return true
}

func (l *seriesReservations) releaseGrouped(r *seriesReservation, held int) {
	position := 0
	for position < held {
		s := l.stripe(r.ids[r.byStripe[position]])
		s.mu.Lock()
		for position < held {
			id := r.ids[r.byStripe[position]]
			if l.stripe(id) != s {
				break
			}
			entry := s.ids[id]
			if entry == nil || entry.owner != r {
				s.mu.Unlock()
				panic("release unowned grouped series id")
			}
			entry.owner = nil
			if entry.first == nil {
				delete(s.ids, id)
				s.recycle(entry)
			} else {
				entry.wake()
			}
			position++
		}
		s.mu.Unlock()
	}
}
