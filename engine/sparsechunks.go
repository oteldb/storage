package engine

const sparseMetricChunkRows = 32

func (p *partStreamWriter) beginNumericGroup(sameSeries, pack bool, timestamp int64) (bool, error) {
	if sameSeries || !p.seriesChunks {
		return !sameSeries, nil
	}
	lo, hi := min(p.sparseMin, timestamp), max(p.sparseMax, timestamp)
	// The chunk directory stores the timestamp span as a signed varint.
	join := pack && p.sparseRows > 0 && p.sparseRows < p.sparseLimit &&
		uint64(hi)-uint64(lo) <= uint64(maxInt64)
	if !join {
		if len(p.runs) > 0 {
			if err := p.w.EndGroup(); err != nil {
				return false, err
			}
		}
		p.sparseRows = 0
		lo, hi = timestamp, timestamp
	}
	if pack {
		p.sparseRows++
		p.sparseMin, p.sparseMax = lo, hi
	}
	return !join, nil
}
