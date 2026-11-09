package engine

import (
	"bytes"
	"context"
	"math"
	"slices"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/internal/buildbudget"
	"github.com/oteldb/storage/internal/memsize"
	"github.com/oteldb/storage/internal/seriesindex"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/readbudget"
	"github.com/oteldb/storage/signal"
)

type diskSelection struct {
	head     uint32
	identity uint32
}

const diskNoHead = math.MaxUint32

func diskQueryBudget(ctx context.Context) (context.Context, *readbudget.Budget) {
	budget := readbudget.From(ctx)
	if budget == nil {
		budget = readbudget.New(readbudget.ProcessShare(0))
		ctx = readbudget.With(ctx, budget)
	}
	return ctx, budget
}

// planDiskQuery snapshots memory and pins parts together, then resolves immutable postings off-lock.
func (e *Engine) planDiskQuery(ctx context.Context, r fetch.Request, withIdentity, countOnly bool) (*enginePlan, error) {
	return e.snapshotDiskQuery(ctx, r, withIdentity, countOnly, nil)
}

// snapshot runs under state ownership before publication can replace frozen buffers with parts.
func (e *Engine) snapshotDiskQuery(
	ctx context.Context, r fetch.Request, withIdentity, countOnly bool,
	snapshot func(*enginePlan) error,
) (*enginePlan, error) {
	for _, m := range r.Matchers {
		if m.Match == nil {
			return nil, errors.New("disk series query: nil matcher")
		}
	}
	ctx, budget := diskQueryBudget(ctx)
	p, err := e.captureDiskHead(ctx, r, withIdentity, countOnly, budget, snapshot)
	if err != nil {
		return nil, err
	}
	selected := make(map[signal.SeriesID]diskSelection, len(p.ids))
	for i, id := range p.ids {
		selected[id] = diskSelection{head: uint32(i), identity: uint32(i)}
	}
	if !countOnly {
		p.diskMetadata = make(map[*part]map[signal.SeriesID]seriesindex.Metadata)
	}
	for _, pt := range p.liveParts {
		if err := p.selectDiskPart(ctx, pt, r, selected, withIdentity, !countOnly); err != nil {
			p.releaseParts()
			return nil, err
		}
	}
	p.finishDiskSelection(selected, withIdentity, countOnly)
	return p, nil
}

func (e *Engine) captureDiskHead(
	ctx context.Context, r fetch.Request, withIdentity, countOnly bool, budget *readbudget.Budget,
	snapshot func(*enginePlan) error,
) (*enginePlan, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	e.mu.RLock()
	for !e.head.indexSorted() {
		e.mu.RUnlock()
		e.mu.LockState()
		e.head.ensureIndexSorted()
		e.mu.UnlockState()
		e.mu.RLock()
	}
	if err := e.fenceLocked(); err != nil {
		e.mu.RUnlock()
		return nil, err
	}
	// Head matcher iterators and the growing result slice are transient metadata workspace.
	workspace := buildbudget.Mul(int64(e.head.series.Len()), buildbudget.Mul(int64(len(r.Matchers))+1, 256))
	if err := budget.Reserve(workspace); err != nil {
		e.mu.RUnlock()
		return nil, err
	}
	// Only the bounded active/frozen head is resident. Capture it and the parts in one lock interval.
	ids := e.head.resolve(r.Matchers)
	n := e.diskHeadSelectionBytes(ids, withIdentity, countOnly)
	budget.Release(workspace)
	if err := budget.Reserve(n); err != nil {
		e.mu.RUnlock()
		return nil, err
	}
	var p *enginePlan
	if countOnly {
		p = e.planExistence(ids, r, withIdentity)
	} else {
		p = e.planFetch(ids, r)
	}
	p.selectionBudget, p.selectionBytes = budget, n
	var err error
	if snapshot != nil {
		err = snapshot(p)
	}
	e.mu.RUnlock()
	if err != nil {
		p.releaseParts()
		return nil, err
	}
	return p, nil
}

func (e *Engine) diskHeadSelectionBytes(ids []signal.SeriesID, withIdentity, countOnly bool) int64 {
	n := buildbudget.Add(4*memsize.MapBase, buildbudget.Mul(int64(len(ids)), diskSelectionBytes(withIdentity)))
	view := e.head.series.View()
	for _, id := range ids {
		if withIdentity {
			if ref, ok := e.head.series.Ref(id); ok {
				materialized := view.MaterializationBytes(int(ref))
				if !countOnly {
					materialized = buildbudget.Mul(materialized, 2)
				}
				n = buildbudget.Add(n, materialized)
			}
		}
		if !countOnly {
			for _, buf := range [2]*sampleBuf{e.head.samples[id], e.flushing[id]} {
				if buf != nil {
					n = buildbudget.Add(n, buildbudget.Mul(int64(len(buf.ts)), 32))
				}
			}
		}
	}
	return n
}

func (p *enginePlan) selectDiskPart(
	ctx context.Context, pt *part, r fetch.Request, selected map[signal.SeriesID]diskSelection,
	withIdentity, withMetadata bool,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	workspace, err := pt.diskSeriesWorkspace(ctx, len(r.Matchers))
	if err != nil {
		return err
	}
	if err := p.selectionBudget.Reserve(workspace); err != nil {
		return err
	}
	defer p.selectionBudget.Release(workspace)
	var metadata map[signal.SeriesID]seriesindex.Metadata
	if withMetadata {
		if err := p.reserveSelection(2 * memsize.MapBase); err != nil {
			return err
		}
		metadata = make(map[signal.SeriesID]seriesindex.Metadata)
		p.diskMetadata[pt] = metadata
	}
	return pt.visitDiskSeries(ctx, r.Matchers, func(id signal.SeriesID, raw []byte, meta seriesindex.Metadata) error {
		if metadata != nil {
			if err := p.reserveSelection(2 * memsize.MapEntry[signal.SeriesID, seriesindex.Metadata]()); err != nil {
				return err
			}
			metadata[id] = meta
		}
		return p.addDiskSelection(selected, id, raw, withIdentity)
	})
}

func (p *enginePlan) addDiskSelection(
	selected map[signal.SeriesID]diskSelection, id signal.SeriesID, raw []byte, withIdentity bool,
) error {
	if _, exists := selected[id]; exists {
		return nil
	}
	n := diskSelectionBytes(withIdentity)
	if withIdentity {
		// DecodeSeries aliases the canonical body and allocates attribute/value headers.
		n = buildbudget.Add(n, buildbudget.Mul(int64(len(raw)), 128))
	}
	if err := p.reserveSelection(n); err != nil {
		return err
	}
	s := diskSelection{head: diskNoHead}
	if withIdentity {
		owned := bytes.Clone(raw)
		decoded, used, err := signal.DecodeSeries(owned)
		if err != nil {
			return err
		}
		if used != len(owned) {
			return errors.New("disk series identity has trailing bytes")
		}
		if uint64(len(p.series)) >= math.MaxUint32 {
			return errors.Wrap(readbudget.ErrExceeded, "too many selected identities")
		}
		s.identity = uint32(len(p.series))
		p.series = append(p.series, decoded)
	}
	selected[id] = s
	return nil
}

func (p *enginePlan) finishDiskSelection(selected map[signal.SeriesID]diskSelection, withIdentity, countOnly bool) {
	p.ids = make([]signal.SeriesID, 0, len(selected))
	for id := range selected {
		p.ids = append(p.ids, id)
	}
	slices.SortFunc(p.ids, signal.SeriesID.Compare)
	oldSeries, oldActive := p.series, p.memActive
	if withIdentity {
		p.series = make([]signal.Series, len(p.ids))
	}
	if countOnly {
		p.memActive = make([]bool, len(p.ids))
	}
	for i, id := range p.ids {
		s := selected[id]
		if withIdentity {
			p.series[i] = oldSeries[s.identity]
		}
		if countOnly && s.head != diskNoHead {
			p.memActive[i] = oldActive[s.head]
		}
	}
	clear(oldSeries)
}

func diskSelectionBytes(withIdentity bool) int64 {
	// Map growth and old/new ID and presence slices coexist until selection finishes.
	// Identity-bearing reads also hold growing and reordered series-header slabs.
	n := int64(96)
	if withIdentity {
		n = buildbudget.Add(n, 3*memsize.Of[signal.Series]())
	}
	return n
}

func (p *enginePlan) reserveSelection(n int64) error {
	if p.selectionBudget == nil {
		return nil
	}
	if err := p.selectionBudget.Reserve(n); err != nil {
		return err
	}
	p.selectionBytes = buildbudget.Add(p.selectionBytes, n)
	return nil
}
