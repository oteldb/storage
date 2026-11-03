package engine

import "context"

func (p *enginePlan) prepareCountReaders() {
	p.countOnly = true
	for _, pt := range p.liveParts {
		if pt.minTime >= p.start && pt.maxTime <= p.end {
			continue
		}
		p.prepareTimestampReader(pt)
	}
}

func (p *enginePlan) prepareTimestampReader(pt *part) {
	desc, ok := pt.reader.ColumnDescByName(colTs)
	if !ok || !desc.Blocked || !desc.Framed || pt.reader.Manifest().GranuleSize <= 0 {
		return
	}
	if p.blockReaders == nil {
		p.blockReaders = make(map[*part]*seriesBlockReader)
		if p.engine.blockCache != nil {
			p.engine.blockCache.fetchStart()
		}
	}
	p.blockReaders[pt] = &seriesBlockReader{
		engine: p.engine, part: pt, blockRows: pt.reader.Manifest().GranuleSize,
		tsB: -1, valB: -1, sfB: -1,
	}
}

func (r *seriesBlockReader) hasTimestamp(ctx context.Context, rng rowRange, start, end int64) (bool, error) {
	first, last, err := r.rangeBlocks(ctx, rng)
	if err != nil {
		return false, err
	}
	for b := first; b <= last; b++ {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if !r.chunkInWindow(ctx, b, start, end) {
			continue
		}
		ts, err := r.tsBlock(ctx, b)
		if err != nil {
			return false, err
		}
		base, blockEnd := r.blockSpan(b)
		lo, hi := max(rng.start, base)-base, min(rng.end-base, blockEnd-base, len(ts))
		if lo < hi {
			i := lo + lowerBound(ts[lo:hi], start)
			if i < hi && ts[i] <= end {
				return true, nil
			}
		}
		r.releaseSeriesPins()
	}
	return false, nil
}

func (r *seriesBlockReader) uncachedTimestampBlock(ctx context.Context, b int) ([]int64, error) {
	if r.part.variable {
		if err := r.ensureGeometry(ctx); err != nil {
			return nil, err
		}
		desc, _ := r.part.reader.ColumnDescByName(colTs)
		if desc.Const {
			lo, hi := r.blockSpan(b)
			out := make([]int64, hi-lo)
			for i := range out {
				out[i] = desc.ConstInt64
			}
			r.tsB, r.tsHeld = b, out
			return out, nil
		}
	}
	if r.tsDec == nil {
		dec, err := r.partDecoder(ctx, colTs)
		if err != nil {
			return nil, err
		}
		r.tsDec = dec
	}
	dst := r.tsHeld[:0]
	if r.part.variable {
		dst = nil
	}
	ts, err := r.tsDec.DecodeInt64Into(b, dst)
	if err != nil {
		return nil, err
	}
	r.tsB, r.tsHeld = b, ts
	return ts, nil
}
