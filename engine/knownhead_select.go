package engine

import (
	"context"

	"github.com/oteldb/storage/internal/buildbudget"
	"github.com/oteldb/storage/internal/memsize"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/readbudget"
	"github.com/oteldb/storage/signal"
)

type knownHeadSelection struct {
	head     *knownSampleHead
	snapshot *seriesSnapshot
	matched  map[signal.SeriesID]bool
	budget   *readbudget.Budget
	bytes    int64
}

func (s *knownHeadSelection) release() {
	if s == nil {
		return
	}
	s.snapshot.release()
	s.snapshot = nil
	s.budget.Release(s.bytes)
	s.bytes = 0
	s.matched = nil
}
func (s *knownHeadSelection) valid(h *knownSampleHead) bool {
	return s == nil || s.head == h && (h == nil || s.snapshot == h.snapshot)
}

// selectKnownHead resolves immutable predicates before any sample buffers are copied.
//
//nolint:nilnil // Unfiltered requests need no separate predicate selection.
func (e *Engine) selectKnownHead(ctx context.Context, r fetch.Request, budget *readbudget.Budget) (*knownHeadSelection, error) {
	if !e.cfg.KnownSeriesHead || len(r.Matchers) == 0 {
		return nil, nil
	}
	s := &knownHeadSelection{budget: budget}
	e.mu.RLock()
	s.head = e.head.known
	if s.head != nil {
		s.snapshot = s.head.snapshot
		s.snapshot.acquire()
	}
	e.mu.RUnlock()
	if s.snapshot == nil {
		return s, nil
	}
	if err := s.resolve(ctx, r); err != nil {
		s.release()
		return nil, err
	}
	return s, nil
}

func (s *knownHeadSelection) resolve(ctx context.Context, r fetch.Request) error {
	// Root iterators reserve their own page and postings workspace.
	ctx = readbudget.With(ctx, s.budget)
	if err := s.budget.Reserve(2 * memsize.MapBase); err != nil {
		return err
	}
	s.bytes = 2 * memsize.MapBase
	s.matched = make(map[signal.SeriesID]bool)
	// Include matches not in the head yet: concurrent appends may admit them before capture.
	return s.snapshot.visit(ctx, r.Matchers, func(id signal.SeriesID, _ []byte) error {
		if s.matched[id] {
			return nil
		}
		n := 2 * memsize.MapEntry[signal.SeriesID, bool]()
		if err := s.budget.Reserve(n); err != nil {
			return err
		}
		s.bytes = buildbudget.Add(s.bytes, n)
		s.matched[id] = true
		return nil
	})
}

func (s *knownHeadSelection) install(p *enginePlan, withIdentity bool) {
	if s == nil {
		return
	}
	p.knownSnapshot = s.snapshot
	s.snapshot = nil
	p.knownIDs = make(map[signal.SeriesID]bool)
	for _, id := range p.ids {
		if s.head != nil && s.head.has(id) {
			p.knownIDs[id] = true
		}
	}
	p.knownResolved = !withIdentity
	p.knownFiltered = true
}

func (s *knownHeadSelection) each(h *knownSampleHead, yield func(signal.SeriesID)) {
	if h == nil {
		return
	}
	if s != nil && len(s.matched) < h.len() {
		for id := range s.matched {
			if h.has(id) {
				yield(id)
			}
		}
		return
	}
	for id := range h.marks {
		if s == nil || s.matched[id] {
			yield(id)
		}
	}
}
