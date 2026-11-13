package engine

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/oteldb/storage/backend/bucketindex"
)

// A binding may advance after catalog publication while each operation pins its own immutable view.
type seriesBinding struct {
	mu       sync.Mutex
	snapshot *seriesSnapshot
	retired  atomic.Int64
}

func newSeriesBinding(s *seriesSnapshot) *seriesBinding {
	s.acquire()
	b := &seriesBinding{snapshot: s}
	b.retired.Store(int64(s.retired))
	return b
}

func (b *seriesBinding) acquire() *seriesSnapshot {
	b.mu.Lock()
	s := b.snapshot
	s.acquire()
	b.mu.Unlock()
	return s
}

func (b *seriesBinding) replace(s *seriesSnapshot) {
	s.acquire()
	b.mu.Lock()
	old := b.snapshot
	b.snapshot = s
	b.retired.Store(int64(s.retired))
	b.mu.Unlock()
	old.release()
}

func (b *seriesBinding) release() {
	if b == nil {
		return
	}
	b.mu.Lock()
	old := b.snapshot
	b.snapshot = nil
	b.mu.Unlock()
	old.release()
}

func (e *Engine) rebindSeriesPartsLocked(parts []*part, bindings []bucketindex.SeriesRoot, retired int) {
	var targets []*seriesBinding
	for _, p := range parts {
		if p.sharedSeries != nil && p.pending == nil && p.sharedSeries.retired.Load() < int64(retired) {
			targets = append(targets, p.sharedSeries)
		}
	}
	for _, p := range e.foreignParts {
		if p.sharedSeries != nil && p.pending == nil && p.sharedSeries.retired.Load() < int64(retired) {
			targets = append(targets, p.sharedSeries)
		}
	}
	if len(targets) == 0 {
		return
	}
	roots := make([]*seriesRootHandle, 0, len(bindings))
	for _, binding := range bindings {
		found := false
		for _, h := range e.seriesTable.roots {
			if h.binding == binding {
				roots = append(roots, h)
				found = true
				break
			}
		}
		if !found {
			return
		}
	}
	published := newSeriesSnapshot(roots)
	published.retired = retired
	defer published.release()
	for _, binding := range targets {
		binding.replace(published)
	}
}

type partSeriesKey struct{ part *part }

func withPartSeriesSnapshot(ctx context.Context, p *part) (context.Context, func()) {
	if p.sharedSeries == nil {
		return ctx, func() {}
	}
	snapshot := p.sharedSeries.acquire()
	return context.WithValue(ctx, partSeriesKey{p}, snapshot), snapshot.release
}

func (p *part) acquireSeriesSnapshot(ctx context.Context) *seriesSnapshot {
	if snapshot, ok := ctx.Value(partSeriesKey{p}).(*seriesSnapshot); ok {
		snapshot.acquire()
		return snapshot
	}
	return p.sharedSeries.acquire()
}
