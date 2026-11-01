package engine

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/oteldb/storage/signal"
)

const (
	seriesReservationStripes       = 256
	seriesReservationFreePerStripe = 128
)

type seriesReservations struct {
	onWait        func(signal.SeriesID)
	trackNewest   bool
	newestEntries atomic.Int64
	newestMaps    atomic.Int64
	stripes       [seriesReservationStripes]seriesReservationStripe
}

type seriesReservationStripe struct {
	newest map[signal.SeriesID]int64
	mu     sync.Mutex
	ids    map[signal.SeriesID]*seriesReservationEntry
	free   []*seriesReservationEntry
}

type seriesReservationEntry struct {
	owner       *seriesReservation
	first, last *seriesReservation
}

type seriesReservation struct {
	ids            []signal.SeriesID
	byStripe       []uint32
	grouped        bool
	held           int
	ready          chan struct{}
	previous, next *seriesReservation
}

func newSeriesReservation(ids []signal.SeriesID) *seriesReservation {
	r := &seriesReservation{ready: make(chan struct{}, 1)}
	r.prepare(ids)
	return r
}

func (r *seriesReservation) prepare(ids []signal.SeriesID) {
	if r.held != 0 || r.previous != nil || r.next != nil {
		panic("prepare owned series reservation")
	}
	r.ids = append(r.ids[:0], ids...)
	slices.SortFunc(r.ids, signal.SeriesID.Compare)
	r.ids = slices.Compact(r.ids)
	r.byStripe = resize(r.byStripe, len(r.ids))
	var counts [seriesReservationStripes]int
	for _, id := range r.ids {
		counts[(id.Hi^id.Lo)%seriesReservationStripes]++
	}
	var position int
	for i, count := range &counts {
		counts[i], position = position, position+count
	}
	for i, id := range r.ids {
		stripe := (id.Hi ^ id.Lo) % seriesReservationStripes
		r.byStripe[counts[stripe]] = uint32(i)
		counts[stripe]++
	}
}

func (l *seriesReservations) stripe(id signal.SeriesID) *seriesReservationStripe {
	return &l.stripes[(id.Hi^id.Lo)%seriesReservationStripes]
}

func (l *seriesReservations) acquire(ctx context.Context, r *seriesReservation) error {
	return l.acquireUntil(r, ctx.Done(), ctx.Err)
}

func reservationCancellation(err func() error) error {
	if err != nil {
		return err()
	}
	return nil
}

func (l *seriesReservations) acquireUntil(r *seriesReservation, done <-chan struct{}, cancellation func() error) error {
	if r.held != 0 || r.previous != nil || r.next != nil {
		panic("reuse owned series reservation")
	}
	if err := reservationCancellation(cancellation); err != nil {
		return err
	}
	if l.tryGrouped(r) {
		if err := reservationCancellation(cancellation); err != nil {
			l.release(r)
			return err
		}
		return nil
	}
	for _, id := range r.ids {
		if err := l.acquireID(r, id, done, cancellation); err != nil {
			l.release(r)
			return err
		}
		r.held++
	}
	if err := reservationCancellation(cancellation); err != nil {
		l.release(r)
		return err
	}
	return nil
}

func (l *seriesReservations) acquireID(r *seriesReservation, id signal.SeriesID, done <-chan struct{}, cancellation func() error) error {
	if err := reservationCancellation(cancellation); err != nil {
		return err
	}
	s := l.stripe(id)
	s.mu.Lock()
	if s.ids == nil {
		s.ids = make(map[signal.SeriesID]*seriesReservationEntry)
	}
	entry := s.ids[id]
	if entry == nil {
		entry = s.take()
		entry.owner = r
		s.ids[id] = entry
		s.mu.Unlock()
		return nil
	}
	if entry.owner == nil && entry.first == nil {
		entry.owner = r
		s.mu.Unlock()
		return nil
	}
	entry.enqueue(r)
	s.mu.Unlock()
	if l.onWait != nil {
		l.onWait(id)
	}

	for {
		select {
		case <-done:
			s.mu.Lock()
			entry.remove(r)
			entry.wake()
			if entry.owner == nil && entry.first == nil {
				delete(s.ids, id)
				s.recycle(entry)
			}
			s.mu.Unlock()
			return reservationCancellation(cancellation)
		case <-r.ready:
			s.mu.Lock()
			if entry.owner == nil && entry.first == r {
				entry.remove(r)
				entry.owner = r
				s.mu.Unlock()
				return nil
			}
			s.mu.Unlock()
		}
	}
}

func (l *seriesReservations) release(r *seriesReservation) {
	if r.grouped {
		l.releaseGrouped(r, r.held)
		r.grouped, r.held = false, 0
		return
	}
	for _, id := range r.ids[:r.held] {
		s := l.stripe(id)
		s.mu.Lock()
		entry := s.ids[id]
		if entry == nil || entry.owner != r {
			s.mu.Unlock()
			panic("release unowned series id")
		}
		entry.owner = nil
		if entry.first == nil {
			delete(s.ids, id)
			s.recycle(entry)
		} else {
			entry.wake()
		}
		s.mu.Unlock()
	}
	r.held = 0
}

func (s *seriesReservationStripe) take() *seriesReservationEntry {
	if n := len(s.free); n > 0 {
		entry := s.free[n-1]
		s.free[n-1] = nil
		s.free = s.free[:n-1]
		return entry
	}
	return &seriesReservationEntry{}
}

func (s *seriesReservationStripe) recycle(entry *seriesReservationEntry) {
	*entry = seriesReservationEntry{}
	if len(s.free) < seriesReservationFreePerStripe {
		s.free = append(s.free, entry)
	}
}

func (e *seriesReservationEntry) enqueue(r *seriesReservation) {
	r.previous = e.last
	if e.last == nil {
		e.first = r
	} else {
		e.last.next = r
	}
	e.last = r
	if e.owner == nil {
		e.wake()
	}
}

func (e *seriesReservationEntry) remove(r *seriesReservation) {
	if r.previous == nil {
		e.first = r.next
	} else {
		r.previous.next = r.next
	}
	if r.next == nil {
		e.last = r.previous
	} else {
		r.next.previous = r.previous
	}
	r.previous, r.next = nil, nil
}

func (e *seriesReservationEntry) wake() {
	if e.owner == nil && e.first != nil {
		select {
		case e.first.ready <- struct{}{}:
		default:
		}
	}
}
