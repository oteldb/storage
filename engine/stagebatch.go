package engine

import "github.com/oteldb/storage/signal"

func (e *Engine) stageWALBatch(
	ids []signal.SeriesID, ts []int64, values, sf []float64, materialize func(i int) signal.Series, limits AppendLimits,
) (AppendResult, error) {
	var bi int
	mat := func() signal.Series { return materialize(bi) }
	var res AppendResult
	for i := range ids {
		bi = i
		w := float64(1)
		if sf != nil {
			w = sf[i]
		}
		res = res.with(e.walB.admit(e.head, ids[i], ts[i], values[i], w, e.cfg.OOOWindow, limits, mat))
	}
	if e.walB.empty() {
		return res, nil
	}
	if err := e.encodeWriteBatch(e.walB, false); err != nil {
		return AppendResult{}, err
	}
	return res, nil
}
