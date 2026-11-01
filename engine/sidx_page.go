package engine

import (
	"context"
	"slices"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/signal"
)

const indexPageEntries = 1024

func (p *pagedIndex) lookup(ctx context.Context, id signal.SeriesID) (rowRange, bool, error) {
	if view := p.view.Load(); view != nil {
		k, ok := p.search(*view, id)
		if !ok {
			return rowRange{}, false, nil
		}
		return p.rangeAt(*view, k), true, nil
	}
	if p.bounded && (id.Compare(p.first) < 0 || id.Compare(p.last) > 0) {
		return rowRange{}, false, nil
	}
	if len(p.fences) == 0 {
		entries, err := p.entries(ctx)
		if err != nil {
			return rowRange{}, false, err
		}
		k, ok := p.search(entries, id)
		if !ok {
			return rowRange{}, false, nil
		}
		return p.rangeAt(entries, k), true, nil
	}

	page, equal := slices.BinarySearchFunc(p.fences, id, signal.SeriesID.Compare)
	if !equal {
		page--
	}
	if page < 0 {
		return rowRange{}, false, nil
	}
	start := page * indexPageEntries
	n := min(indexPageEntries, p.n-start)
	// One extra entry supplies the last run's end when the page is not the final one.
	readN := min(n+1, p.n-start)
	data, err := backend.ReadPageView(ctx, p.be, p.key, p.offset+int64(start*sidxEntryW), int64(readN*sidxEntryW))
	if err != nil {
		return rowRange{}, false, err
	}
	if len(data) != readN*sidxEntryW {
		return rowRange{}, false, errors.Wrap(errSidxCorrupt, "short index page")
	}
	local := pagedIndex{n: n, total: p.total}
	k, ok := local.search(data, id)
	if !ok {
		return rowRange{}, false, nil
	}
	end := p.total
	if k+1 < readN {
		end = sidxEntryStart(data, k+1)
	}
	return rowRange{start: sidxEntryStart(data, k), end: end}, true, nil
}
