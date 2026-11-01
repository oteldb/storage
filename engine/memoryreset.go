package engine

func (e *Engine) resetMemoryViewLocked() {
	if !e.memoryPublicationEnabled() {
		return
	}
	for i := range e.reservations.stripes {
		s := &e.reservations.stripes[i]
		s.mu.Lock()
		s.newest = nil
		s.mu.Unlock()
	}
	e.reservations.newestMaps.Store(0)
	e.reservations.newestEntries.Store(0)
	e.head.memoryRuns = true
	e.head.admission = &e.reservations
	e.head.seriesNewest = nil
	e.head.logged = &e.loggedIdentities
	e.loggedIdentities.prepare(e.head.series, e.cfg.WAL.Epoch(), 0)
	next := &memoryReadView{}
	next.inherit(nil)
	next.registry = e.head.series.View()
	e.viewMu.Lock()
	old := e.readView
	e.readView = next
	e.viewMu.Unlock()
	old.release()
	if l := e.memoryLife; l != nil {
		l.notify()
	}
}
