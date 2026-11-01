package engine

import (
	"context"

	"github.com/oteldb/storage/block"
	"github.com/oteldb/storage/internal/memsize"
)

const marksCacheFraction = 16

func marksBlockKey(p *part) blockKey {
	return blockKey{prefix: p.prefix, col: colMarksID, blk: -1}
}

func (e *Engine) cacheWrittenMarks(p *part, marks block.Marks) {
	if e.blockCache == nil || len(marks.Granules) == 0 || !p.validMarks(marks) {
		return
	}
	bytes := memsize.Slice(marks.Granules) + 256
	if bytes > e.blockCache.maxBytes/marksCacheFraction {
		return
	}
	entry := e.blockCache.insert(&blockEntry{key: marksBlockKey(p), granules: marks.Granules, bytes: bytes})
	e.blockCache.release(entry)
}

func (e *Engine) partGranuleTimes(ctx context.Context, p *part) []block.Granule {
	if e != nil && e.blockCache != nil {
		if entry, ok := e.blockCache.get(marksBlockKey(p)); ok {
			granules := entry.granules
			e.blockCache.release(entry)
			// Granules are immutable and never pooled; this slice keeps them alive after eviction.
			return granules
		}
	}
	return p.granuleTimes(ctx)
}
