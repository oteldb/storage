package engine

import "sync"

// Metadata revisions for one physical prefix share a file-deletion lease.
type partLifetimes struct {
	mu       sync.Mutex
	byPrefix map[string]*partLifetime
}

func (l *partLifetimes) bind(p *part) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.byPrefix == nil {
		l.byPrefix = make(map[string]*partLifetime)
	}
	prior := l.byPrefix[p.prefix]
	if prior == nil {
		l.byPrefix[p.prefix] = p.lifetime()
		return
	}
	if current := p.life.Load(); current != prior {
		if current != nil && current.refs.Load() != 0 {
			panic("bind physical part after exposure")
		}
		p.life.Store(prior)
	}
}

func (l *partLifetimes) protect(dst map[string]struct{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for prefix, lifetime := range l.byPrefix {
		if lifetime.refs.Load() > 0 {
			dst[prefix] = struct{}{}
		} else {
			delete(l.byPrefix, prefix)
		}
	}
}

func (l *partLifetimes) forget(p *part) {
	l.mu.Lock()
	if lifetime := l.byPrefix[p.prefix]; lifetime == p.life.Load() && lifetime != nil && lifetime.refs.Load() == 0 {
		delete(l.byPrefix, p.prefix)
	}
	l.mu.Unlock()
}

// Caller owns catalog/state; construction and final retirement stay outside the publication gate.
func (e *Engine) syncMemoryPartsLocked() {
	if !e.memoryPublicationEnabled() {
		return
	}
	e.loggedIdentities.prepare(e.head.series, e.cfg.WAL.Epoch(), e.head.series.Len())
	parts := e.readablePartsLocked()
	for _, p := range parts {
		e.partLifetimes.bind(p)
	}
	set := newMemoryPartSet(parts)
	next := &memoryReadView{}
	e.viewMu.Lock()
	old := e.readView
	next.inherit(old)
	next.parts.release()
	next.parts = set
	next.registry = e.head.series.View()
	e.readView = next
	e.viewMu.Unlock()
	old.release()
}
