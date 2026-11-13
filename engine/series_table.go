package engine

import (
	"context"
	"slices"
	"strings"
	"sync/atomic"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/internal/buildbudget"
	"github.com/oteldb/storage/internal/seriestable"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

type seriesRootHandle struct {
	binding bucketindex.SeriesRoot
	run     *seriestable.Run
	refs    atomic.Int32
}

func (h *seriesRootHandle) acquire() { h.refs.Add(1) }
func (h *seriesRootHandle) release() {
	if h.refs.Add(-1) == 0 {
		_ = h.run.Close()
	}
}

// Parts retain one snapshot lease until their last reader and owner have drained.
type seriesSnapshot struct {
	refs    atomic.Int32
	roots   []*seriesRootHandle
	retired int
}

func newSeriesSnapshot(roots []*seriesRootHandle) *seriesSnapshot {
	s := &seriesSnapshot{roots: slices.Clone(roots)}
	slices.SortFunc(s.roots, func(a, b *seriesRootHandle) int { return strings.Compare(a.binding.Prefix, b.binding.Prefix) })
	s.refs.Store(1)
	for _, h := range s.roots {
		h.acquire()
	}
	return s
}

func (s *seriesSnapshot) acquire() {
	if s != nil {
		s.refs.Add(1)
	}
}

func (s *seriesSnapshot) release() {
	if s != nil && s.refs.Add(-1) == 0 {
		for _, h := range s.roots {
			h.release()
		}
	}
}

func (s *seriesSnapshot) bindings() []bucketindex.SeriesRoot {
	if s == nil {
		return nil
	}
	out := make([]bucketindex.SeriesRoot, len(s.roots))
	for i, h := range s.roots {
		out[i] = h.binding
	}
	return out
}

func (s *seriesSnapshot) workspace(matchers int) int64 {
	var largest int64
	if s != nil {
		for _, h := range s.roots {
			n := buildbudget.Mul(int64(matchers+4), h.run.MaxPageBytes())
			n = buildbudget.Add(n, buildbudget.Mul(int64(max(matchers, 1)), (int64(h.run.Count())+63)/64*8))
			largest = max(largest, n)
		}
	}
	return largest
}

func (s *seriesSnapshot) lookup(ctx context.Context, id signal.SeriesID, visit func([]byte) error) (bool, error) {
	if s == nil {
		return false, nil
	}
	for _, h := range s.roots {
		r, err := h.run.NewReader(ctx)
		if err != nil {
			return false, err
		}
		raw, ok, err := r.LookupRaw(id)
		if err == nil && ok {
			err = visit(raw)
		}
		closeErr := r.Close()
		if err != nil {
			return false, err
		}
		if closeErr != nil {
			return false, closeErr
		}
		if ok {
			return true, nil
		}
	}
	return false, nil
}

func (s *seriesSnapshot) visit(ctx context.Context, matchers []fetch.Matcher, visit func(signal.SeriesID, []byte) error) error {
	if s == nil {
		return nil
	}
	for _, h := range s.roots {
		it, err := h.run.Resolve(ctx, matchers)
		if err != nil {
			return err
		}
		for it.Next() {
			if err = visit(it.ID(), it.Raw()); err != nil {
				_ = it.Close()
				return err
			}
		}
		err = it.Err()
		closeErr := it.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
	}
	return nil
}

func (e *Engine) prepareSeriesRoots(
	ctx context.Context, bindings []bucketindex.SeriesRoot, incomingRetired ...[]bucketindex.SeriesRoot,
) (*seriesSnapshot, error) {
	if len(bindings) > 0 {
		if err := e.validateSeparateSeries(); err != nil {
			return nil, err
		}
		if !e.cfg.SeparateSeries {
			return nil, errors.New("engine: separate series catalog requires opt-in")
		}
	}
	retired := e.seriesRetired
	if len(incomingRetired) > 0 {
		var err error
		retired, err = bucketindex.MergeSeriesRoots(retired, incomingRetired[0])
		if err != nil {
			return nil, err
		}
	}
	merged, _, err := bucketindex.MergeSeriesCatalog(e.seriesTable.bindings(), retired, bindings, nil)
	if err != nil {
		return nil, err
	}
	if slices.Equal(merged, e.seriesTable.bindings()) {
		e.seriesTable.acquire()
		return e.seriesTable, nil
	}
	roots := make([]*seriesRootHandle, 0, len(merged))
	var opened []*seriestable.Run
	for _, binding := range merged {
		var found *seriesRootHandle
		if e.seriesTable != nil {
			for _, h := range e.seriesTable.roots {
				if h.binding == binding {
					found = h
					break
				}
			}
		}
		if found == nil {
			run, openErr := seriestable.OpenWithIDCache(ctx, e.cfg.Backend, binding.Prefix, e.seriesIDs, binding.ManifestDigest)
			if openErr != nil {
				for _, r := range opened {
					_ = r.Close()
				}
				return nil, errors.Wrap(openErr, "open shared series root")
			}
			opened = append(opened, run)
			found = &seriesRootHandle{binding: binding, run: run}
		}
		roots = append(roots, found)
	}
	next := newSeriesSnapshot(roots)
	next.retired = len(retired)
	return next, nil
}

func (e *Engine) installSeriesRootsLocked(
	ctx context.Context, bindings []bucketindex.SeriesRoot, retired ...[]bucketindex.SeriesRoot,
) error {
	next, err := e.prepareSeriesRoots(ctx, bindings, retired...)
	if err != nil {
		return err
	}
	if len(retired) > 0 {
		merged, err := bucketindex.MergeSeriesRoots(e.seriesRetired, retired[0])
		if err != nil {
			next.release()
			return err
		}
		e.seriesRetired = merged
	}
	old := e.seriesTable
	e.seriesTable = next
	old.release()
	return nil
}

func (e *Engine) validateSeparateSeries() error {
	if !e.cfg.SeparateSeries {
		return nil
	}
	if e.cfg.Backend == nil || !e.cfg.DiskSeries || !e.cfg.MetricSeriesChunks || e.cfg.Term != nil || e.cfg.WriterID != "" {
		return errors.New("engine: separate series requires standalone disk-series chunks")
	}
	return nil
}
