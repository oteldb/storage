package engine

func (e *Engine) initMemoryPublication() {
	view := &memoryReadView{}
	view.inherit(nil)
	view.registry = e.head.series.View()
	e.readView = view
	e.memoryLife = newMemoryLifecycle()
	e.mu.EnableSharedWriters()
	e.head.memoryRuns = true
	e.reservations.trackNewest = e.cfg.OOOWindow > 0
	e.head.seriesNewest = nil
	e.head.admission = &e.reservations
	e.loggedIdentities.prepare(e.head.series, e.cfg.WAL.Epoch(), e.head.series.Len())
	e.head.logged = &e.loggedIdentities
}
