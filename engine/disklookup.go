package engine

import (
	"slices"

	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/signal"
)

type diskSeriesLookup struct {
	pending    map[signal.SeriesID]diskHeadEntry
	historical []signal.SeriesID
	parts      []*part
	ownParts   int
	head       *head
	disk       *diskHeadState
	generation bucketindex.Generation
}

func (l *diskSeriesLookup) reset() {
	clear(l.pending)
	l.historical = l.historical[:0]
	clear(l.parts)
	l.parts = l.parts[:0]
	l.head, l.disk = nil, nil
}

func (l *diskSeriesLookup) capture(e *Engine, ids []signal.SeriesID) {
	l.reset()
	l.head, l.disk, l.generation = e.head, e.head.disk, e.generation
	if l.pending == nil {
		l.pending = make(map[signal.SeriesID]diskHeadEntry, len(ids))
	}
	for _, id := range ids {
		if e.head.series.Has(id) {
			continue
		}
		if _, ok := l.pending[id]; ok {
			continue
		}
		if hit, ok := l.disk.cache.get(id); ok {
			l.pending[id] = hit
			continue
		}
		l.pending[id] = diskHeadEntry{}
		if l.disk.mayContain(id) {
			l.historical = append(l.historical, id)
		}
	}
	if len(l.historical) == 0 {
		return
	}
	l.ownParts = len(e.parts)
	l.parts = append(l.parts, e.parts...)
	for _, p := range e.foreignParts {
		l.parts = append(l.parts, p)
	}
	for _, p := range l.parts {
		p.acquire()
	}
}

func (l *diskSeriesLookup) valid(e *Engine) bool {
	if l.head != e.head || l.disk != e.head.disk || l.generation != e.generation {
		return false
	}
	if len(l.historical) == 0 {
		return true
	}
	if len(l.parts) != len(e.parts)+len(e.foreignParts) || l.ownParts != len(e.parts) {
		return false
	}
	if !slices.Equal(l.parts[:l.ownParts], e.parts) {
		return false
	}
	for _, p := range l.parts[l.ownParts:] {
		if e.foreignParts[p.prefix] != p {
			return false
		}
	}
	return true
}

// preloadPreparedDiskSeriesLocked returns with writer ownership restored on every path.
func (e *Engine) preloadPreparedDiskSeriesLocked(ids []signal.SeriesID, l *diskSeriesLookup) error {
	if e.parallel == nil || !e.cfg.DiskSeries {
		return e.preloadDiskSeriesLocked(ids)
	}
	if err := e.checkDiskAdmissionLocked(); err != nil {
		return err
	}
	if err := e.mu.WithReaders(func() error { l.capture(e, ids); return nil }); err != nil {
		return err
	}
	if len(l.historical) == 0 {
		e.head.disk.pending = l.pending
		return nil
	}
	err := e.lookupOutsideWriter(l)
	if err := e.refuseWrite(); err != nil {
		return err
	}
	if err := e.flushResidentHeadLocked(); err != nil {
		return err
	}
	if !l.valid(e) {
		// Catalog or head replacement invalidates both hits and misses. Bound retries by
		// resolving once under writer ownership instead of racing maintenance indefinitely.
		return e.preloadDiskSeriesLocked(ids)
	}
	if err != nil {
		return err
	}
	if err := e.checkDiskAdmissionLocked(); err != nil {
		return err
	}
	e.head.disk.pending = l.pending
	for _, id := range l.historical {
		if entry := l.pending[id]; entry.exists {
			e.head.disk.remember(id, entry.newest)
		}
	}
	return nil
}

func (e *Engine) lookupOutsideWriter(l *diskSeriesLookup) error {
	e.mu.UnlockWriter()
	defer e.mu.LockWriter()
	defer func() {
		for _, p := range l.parts {
			p.release()
		}
	}()
	slices.SortFunc(l.historical, signal.SeriesID.Compare)
	return lookupDiskSeries(l.parts, l.historical, l.pending)
}
