package engine

import (
	"context"
	"sync"

	"github.com/go-faster/errors"
)

const maxActiveMemoryRuns = 1024

var errMemoryClosed = errors.New("metric engine is closed")

var errMemoryCapacity = errors.New("metric memory run capacity exhausted")

type memoryLifecycle struct {
	//nolint:containedctx // The engine owns this worker context until Close.
	ctx     context.Context
	cancel  context.CancelFunc
	wake    chan struct{}
	mu      sync.Mutex
	changed chan struct{}
	err     error
	start   sync.Once
	workers sync.WaitGroup
}

func newMemoryLifecycle() *memoryLifecycle {
	ctx, cancel := context.WithCancel(context.Background())
	return &memoryLifecycle{ctx: ctx, cancel: cancel, wake: make(chan struct{}, 1), changed: make(chan struct{})}
}

func (l *memoryLifecycle) notify() {
	l.mu.Lock()
	close(l.changed)
	l.changed = make(chan struct{})
	l.mu.Unlock()
}

func (e *Engine) wakeMemoryCoalescer() {
	l := e.memoryLife
	if l == nil {
		return
	}
	l.start.Do(func() {
		l.workers.Go(func() { e.runMemoryCoalescer(l.ctx) })
	})
	select {
	case l.wake <- struct{}{}:
	default:
	}
}

func (e *Engine) runMemoryCoalescer(ctx context.Context) {
	l := e.memoryLife
	for {
		select {
		case <-ctx.Done():
			return
		case <-l.wake:
			for {
				changed, err := e.coalesceMemoryOnce(ctx)
				if err == nil && !changed {
					e.viewMu.Lock()
					if view := e.readView; view != nil && view.active != nil && view.active.runs >= maxActiveMemoryRuns {
						err = errMemoryCapacity
					}
					e.viewMu.Unlock()
				}
				l.mu.Lock()
				l.err = err
				l.mu.Unlock()
				l.notify()
				if err != nil || !changed {
					break
				}
			}
		}
	}
}

// Capacity waits precede writer-mode ownership so a flush can always drain writers.
func (e *Engine) awaitMemoryCapacity() error {
	l := e.memoryLife
	if l == nil {
		return nil
	}
	for {
		l.mu.Lock()
		changed, err := l.changed, l.err
		l.mu.Unlock()
		if l.ctx.Err() != nil {
			return errMemoryClosed
		}
		e.viewMu.Lock()
		view := e.readView
		fits := view == nil || view.active == nil || view.active.runs < maxActiveMemoryRuns
		e.viewMu.Unlock()
		if fits {
			return nil
		}
		if err != nil {
			return err
		}
		e.wakeMemoryCoalescer()
		select {
		case <-l.ctx.Done():
			return errMemoryClosed
		case <-changed:
		}
	}
}

func (e *Engine) stopMemoryWork() {
	if l := e.memoryLife; l != nil {
		l.cancel()
		// Pair with the start gate before waiting: no worker can be added afterwards.
		l.start.Do(func() {})
		l.workers.Wait()
	}
}

// Returns with writer ownership on success and without it on error.
func (e *Engine) waitMemoryWriterCapacity() error {
	if !e.head.memoryRuns {
		return nil
	}
	for {
		e.viewMu.Lock()
		view := e.readView
		fits := view == nil || view.active == nil || view.active.runs < maxActiveMemoryRuns
		e.viewMu.Unlock()
		if fits {
			return nil
		}
		e.mu.UnlockWriter()
		if err := e.awaitMemoryCapacity(); err != nil {
			return err
		}
		e.mu.LockWriter()
	}
}
