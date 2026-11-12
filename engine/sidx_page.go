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
	_, rows, found, err := p.lookupPosition(ctx, id, pageEntries)
	return rows, found, err
}

func (p *pagedIndex) lookupPosition(ctx context.Context, id signal.SeriesID, pageEntries int) (int, rowRange, bool, error) {
	return p.lookupPositionCursor(ctx, id, pageEntries, nil)
}

func (p *pagedIndex) lookupPositionCursor(
	ctx context.Context, id signal.SeriesID, pageEntries int, cursor *sidxPositionCursor,
) (int, rowRange, bool, error) {
	if p.readContext != nil {
		ctx = p.readContext(ctx)
	}
	if view := p.view.Load(); view != nil {
		k, ok := p.search(*view, id)
		if !ok {
			return 0, rowRange{}, false, nil
		}
		return k, p.rangeAt(*view, k), true, nil
	}
	if p.bounded && (id.Compare(p.first) < 0 || id.Compare(p.last) > 0) {
		return 0, rowRange{}, false, nil
	}
	if len(p.fences) == 0 {
		entries, err := p.entries(ctx)
		if err != nil {
			return 0, rowRange{}, false, err
		}
		k, ok := p.search(entries, id)
		if !ok {
			return 0, rowRange{}, false, nil
		}
		return k, p.rangeAt(entries, k), true, nil
	}

	start := p.pageStart(id, pageEntries)
	if start < 0 {
		return 0, rowRange{}, false, nil
	}
	n := min(pageEntries, p.n-start)
	// One extra entry supplies the last run's end when the page is not the final one.
	readN := min(n+1, p.n-start)
	data, err := p.positionPage(ctx, start, readN, cursor)
	if err != nil {
		return 0, rowRange{}, false, err
	}

	local := pagedIndex{n: n, total: p.total}
	k, ok := local.search(data, id)
	if !ok {
		return 0, rowRange{}, false, nil
	}
	end := p.total
	if k+1 < readN {
		end = sidxEntryStart(data, k+1)
	}
	return start + k, rowRange{start: sidxEntryStart(data, k), end: end}, true, nil
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

// sidxPositionCursor borrows one page only while its part is pinned by the caller.
type sidxPositionCursor struct {
	index *pagedIndex
	start int
	data  []byte
}

func (c *sidxPositionCursor) position(ctx context.Context, idx partIndex, id signal.SeriesID) (int, bool, error) {
	if idx.paged == nil {
		return idx.position(ctx, id)
	}
	pos, _, found, err := idx.paged.lookupPositionCursor(ctx, id, indexPageEntries, c)
	return pos, found, err
}

func (p *pagedIndex) positionPage(ctx context.Context, start, readN int, c *sidxPositionCursor) ([]byte, error) {
	if c != nil && c.index == p && c.start == start && len(c.data) == readN*sidxEntryW {
		return c.data, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	data, err := backend.ReadPageView(ctx, p.be, p.key, p.offset+int64(start*sidxEntryW), int64(readN*sidxEntryW))
	if err != nil {
		return nil, err
	}
	if len(data) != readN*sidxEntryW {
		return nil, errors.Wrap(errSidxCorrupt, "short index page")
	}
	if c != nil {
		c.index, c.start, c.data = p, start, data
	}
	return data, nil
}
