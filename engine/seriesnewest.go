package engine

import (
	"github.com/oteldb/storage/internal/memsize"
	"github.com/oteldb/storage/signal"
)

func (l *seriesReservations) newestFor(id signal.SeriesID) (int64, bool) {
	if !l.trackNewest {
		return 0, false
	}
	s := l.stripe(id)
	s.mu.Lock()
	timestamp, found := s.newest[id]
	s.mu.Unlock()
	return timestamp, found
}

func (l *seriesReservations) setNewest(id signal.SeriesID, timestamp int64) {
	if !l.trackNewest {
		return
	}
	s := l.stripe(id)
	s.mu.Lock()
	if s.newest == nil {
		s.newest = make(map[signal.SeriesID]int64)
		l.newestMaps.Add(1)
	}
	prior, found := s.newest[id]
	if !found {
		l.newestEntries.Add(1)
	}
	if !found || timestamp > prior {
		s.newest[id] = timestamp
	}
	s.mu.Unlock()
}

func (l *seriesReservations) deleteNewest(id signal.SeriesID) {
	s := l.stripe(id)
	s.mu.Lock()
	if _, found := s.newest[id]; found {
		delete(s.newest, id)
		l.newestEntries.Add(-1)
	}
	if s.newest != nil && len(s.newest) == 0 {
		s.newest = nil
		l.newestMaps.Add(-1)
	}
	s.mu.Unlock()
}

func (l *seriesReservations) newestBytes() int64 {
	return l.newestEntries.Load()*watermarkEntryBytes + l.newestMaps.Load()*memsize.MapBase
}
