package engine

import (
	"context"
	"time"

	"github.com/oteldb/storage/internal/buildbudget"
	"github.com/oteldb/storage/internal/memsize"
)

// One lease includes retained partition/output state across mixed-population builders.
type flushLease struct {
	ctx            context.Context //nolint:containedctx // Owned by one synchronous flush.
	e              *Engine
	held, reserved int64
	release        func()
}

func (l *flushLease) close() {
	if l.release != nil {
		l.release()
		l.release = nil
	}
}
func (l *flushLease) reserve(bytes int64, phase string) error {
	bytes = buildbudget.Add(bytes, l.held)
	if l.release != nil && bytes == l.reserved {
		return nil
	}
	l.close()
	start := time.Now()
	if l.e.cfg.FlushAdmission != nil {
		release, err := l.e.cfg.FlushAdmission(l.ctx, bytes)
		if err != nil {
			return err
		}
		l.release = release
	} else {
		l.release = func() {}
	}
	l.reserved = bytes
	l.e.cfg.Obs.Flush.Reservation(l.ctx, phase, bytes, l.e.flushMemoryBytes(), time.Since(start))
	return nil
}

func privatePartBytes(p *part) int64 {
	n := buildbudget.Add(64<<10, p.reader.ResidentBytes())
	n = buildbudget.Add(n, p.idBloom.Load().sizeBytes())
	if index := p.index.paged; index != nil {
		n = buildbudget.Add(n, memsize.Slice(index.fences))
		if view := index.view.Load(); view != nil {
			n = buildbudget.Add(n, memsize.Slice(*view))
		}
	} else {
		n = buildbudget.Add(n, buildbudget.Add(memsize.Slice(p.index.ids), memsize.Slice(p.index.starts)))
	}
	return n
}
