package engine

import (
	"context"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/signal"
)

const (
	indexPageEntries       = 1024
	sparseIndexPageEntries = 256
)

func (p *pagedIndex) lookup(ctx context.Context, id signal.SeriesID) (rowRange, bool, error) {
	return p.lookupPage(ctx, id, indexPageEntries)
}

func (p *pagedIndex) lookupPage(ctx context.Context, id signal.SeriesID, pageEntries int) (rowRange, bool, error) {
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

	start := p.pageStart(id, pageEntries)
	if start < 0 {
		return rowRange{}, false, nil
	}
	n := min(pageEntries, p.n-start)
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

func (p *pagedIndex) pageStart(id signal.SeriesID, pageEntries int) int {
	const coarseStride = indexPageEntries / sparseIndexPageEntries
	stride, pages := 1, len(p.fences)
	if pageEntries == indexPageEntries {
		stride = coarseStride
		pages = (pages + coarseStride - 1) / coarseStride
	}
	lo, hi := 0, pages
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if p.fences[mid*stride].Compare(id) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo == pages || p.fences[lo*stride] != id {
		lo--
	}
	return lo * pageEntries
}
