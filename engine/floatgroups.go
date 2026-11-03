package engine

import (
	"context"

	"github.com/oteldb/storage/block"
)

func (e *Engine) writeGroupedColumns(ctx context.Context, cols *flushColumns, comp compressProfile, rollup *block.Rollup) (*part, error) {
	writer, err := newPartStreamWriter(ctx, e, comp, 0, cols.sf != nil, e.cfg.AggregateStats, rollup)
	if err != nil {
		return nil, err
	}
	defer writer.abort()
	for lo := 0; lo < len(cols.series); {
		hi := lo + 1
		for hi < len(cols.series) && cols.series[hi] == cols.series[lo] {
			hi++
		}
		var weights []float64
		if cols.sf != nil {
			weights = cols.sf[lo:hi]
		}
		covered := cols.ts[hi-1]
		if cols.covered != nil {
			for _, ts := range cols.covered[lo:hi] {
				covered = max(covered, ts)
			}
		}
		if err := writer.appendSeries(cols.series[lo], cols.ts[lo:hi], cols.value[lo:hi], weights, covered); err != nil {
			return nil, err
		}
		lo = hi
	}
	return writer.finish(ctx)
}
