package engine

func (e *Engine) freezeMemoryView() (*memoryDirectoryNode, *memoryReadView) {
	e.viewMu.Lock()
	defer e.viewMu.Unlock()
	old := e.readView
	if old == nil {
		return nil, nil
	}
	if old.frozen != nil {
		panic("second frozen memory generation")
	}
	next := &memoryReadView{}
	next.inherit(old)
	next.frozen, next.active = next.active, nil
	e.readView = next
	next.frozen.acquire()
	return next.frozen, old
}

func (e *Engine) restoreMemoryView() *memoryReadView {
	path := e.newMemoryPath()
	defer path.close()
	next := &memoryReadView{}
	e.viewMu.Lock()
	defer e.viewMu.Unlock()
	old := e.readView
	if old == nil || old.frozen == nil {
		return nil
	}
	next.inherit(old)
	joined := path.concat(next.frozen, next.active)
	next.active.release()
	next.frozen.release()
	next.active, next.frozen = joined, nil
	e.readView = next
	return old
}

func (e *Engine) finishMemoryView() *memoryReadView {
	e.viewMu.Lock()
	base := e.readView
	if base == nil {
		e.viewMu.Unlock()
		return nil
	}
	base.acquire()
	e.viewMu.Unlock()
	defer base.release()
	readable := e.readablePartsLocked()
	for _, p := range readable {
		e.partLifetimes.bind(p)
	}
	parts := newMemoryPartSet(readable)
	recent, recentMin := e.memoryRecentForFlush(base)
	e.viewMu.Lock()
	defer e.viewMu.Unlock()
	old := e.readView
	next := &memoryReadView{}
	next.inherit(old)
	next.parts.release()
	next.parts = parts
	next.recent.release()
	next.recent, next.recentMin = recent, recentMin
	next.frozen.release()
	next.frozen = nil
	next.registry = e.head.series.View()
	e.readView = next
	return old
}
