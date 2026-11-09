package engine

import (
	"context"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/internal/seriesindex"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/readbudget"
	"github.com/oteldb/storage/signal"
)

func (p *enginePlan) selectDiskCountPart(
	ctx context.Context, pt *part, r fetch.Request, selected map[signal.SeriesID]diskSelection,
) (bool, error) {
	baseWorkspace := int64(3*min(indexPageEntries, pt.index.seriesCount())*sidxEntryW + 4096)
	if err := p.selectionBudget.Reserve(baseWorkspace); err != nil {
		return true, err
	}
	defer p.selectionBudget.Release(baseWorkspace)
	ctx = pt.reader.ReaderContext(ctx)
	if len(r.Matchers) == 0 {
		if err := pt.requireRangedCountIDs(ctx); err != nil {
			return true, err
		}
		ids := pt.diskRowIDs(ctx)
		for ids.Next() {
			if err := p.addDiskSelection(selected, ids.At(), nil, false); err != nil {
				return true, err
			}
		}
		return true, ids.Err()
	}
	index, err := pt.seriesIndex(ctx)
	if err != nil {
		return true, err
	}
	if index == nil {
		return false, nil
	}
	if err := pt.requireRangedCountIDs(ctx); err != nil {
		return true, err
	}
	workspace, err := pt.diskSeriesWorkspace(ctx, len(r.Matchers))
	if err != nil {
		return true, err
	}
	if err := p.selectionBudget.Reserve(workspace); err != nil {
		return true, err
	}
	defer p.selectionBudget.Release(workspace)
	refs, err := index.Resolve(ctx, r.Matchers)
	if err != nil {
		return true, err
	}
	reader := diskPartIDs{ctx: ctx, index: pt.index.paged, pos: -1}
	for refs.Next() {
		ref := refs.At()
		if ref.Hi != 0 || ref.Lo >= uint64(pt.index.seriesCount()) {
			return true, errors.Wrap(seriesindex.ErrCorrupt, "count reference bounds")
		}
		var id signal.SeriesID
		if reader.index == nil {
			id = pt.index.ids[int(ref.Lo)]
		} else {
			// Resolve yields canonical ordinals, validated against SIDX before publication/load.
			reader.pos = int(ref.Lo) - 1
			if !reader.Next() {
				return true, reader.Err()
			}
			id = reader.At()
		}
		if err := p.addDiskSelection(selected, id, nil, false); err != nil {
			return true, err
		}
	}
	return true, refs.Err()
}

func (p *part) requireRangedCountIDs(ctx context.Context) error {
	if index := p.index.paged; index != nil && !backend.RangesNatively(ctx, index.be, index.key) {
		return errors.Wrap(readbudget.ErrExceeded, "count identities require bounded SIDX reads")
	}
	return nil
}
