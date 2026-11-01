package engine

import "time"

type memoryRunPublication struct {
	run   *memoryRun
	path  *memoryDirectoryPath
	view  memoryReadView
	first time.Time
}

func (e *Engine) prepareMemoryPublication(batch *walBatch) error {
	e.viewMu.Lock()
	enabled := e.readView != nil
	e.viewMu.Unlock()
	if !enabled || batch.empty() {
		return nil
	}
	run, err := memoryRunFromBatch(batch, nil)
	if err != nil {
		return err
	}
	e.ownMemoryRun(run)
	batch.publication = &memoryRunPublication{run: run, path: e.newMemoryPath(), first: batch.first}
	return nil
}

func (p *memoryRunPublication) abort() {
	p.run.release()
	p.path.close()
}

// The current compatibility writer still owns state; the parallel writer will only own publication.
func (e *Engine) applyLoggedBatch(batch *walBatch) {
	publication := batch.publication
	if publication == nil {
		batch.apply(e.head)
		return
	}
	e.publishMemoryBatch(batch, true)
}

func (e *Engine) publishMemoryBatch(batch *walBatch, logged bool) {
	publication := batch.publication
	if !batch.warm {
		batch.applyRunMetadata(e.head)
	}
	e.viewMu.Lock()
	old := e.readView
	publication.view.inherit(old)
	e.runSequence++
	next := publication.path.insert(publication.view.active, e.runSequence, publication.run)
	publication.view.active.release()
	publication.view.active = next
	publication.view.registry = e.head.series.View()
	if e.head.runBytes.Load() == 0 {
		e.head.runFirst.Store(&publication.first)
	}
	e.head.runBytes.Add(publication.run.rows * SampleBytes)
	e.head.runNewest.Store(max(e.head.runNewest.Load(), publication.run.maxTS))
	e.readView = &publication.view
	coalesce := next.runs > 256
	e.viewMu.Unlock()
	publication.path.close()
	publication.path = nil
	publication.run.release()
	publication.run = nil
	batch.publication = nil
	old.release()
	if coalesce {
		e.wakeMemoryCoalescer()
	}
	if hooks := e.warmHooks; batch.warm && hooks != nil && hooks.published != nil {
		hooks.published(batch.order)
	}
	for i, id := range batch.order {
		acc := &batch.accs[i]
		e.head.setNewest(id, acc.newest)
		if logged && e.head.logged != nil {
			ref := acc.identityRef
			if acc.register {
				ref, _ = e.head.series.Ref(id)
			}
			e.loggedIdentities.mark(ref)
		}
	}
}

func (b *walBatch) applyRunMetadata(h *head) {
	for i, id := range b.order {
		acc := &b.accs[i]
		if acc.register && !h.series.Has(id) {
			h.register(id, acc.series)
		}
		if h.logged == nil && h.samples[id] == nil {
			h.samples[id] = h.runMarker
		}
		if !acc.hadNewest || acc.newest > acc.priorNewest {
			h.setNewest(id, acc.newest)
		}
		h.newest = max(h.newest, acc.newest)
	}
}
