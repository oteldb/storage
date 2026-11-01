package engine

import "github.com/oteldb/storage/signal"

// A WAL samples frame commonly contains one point; runs batch across frame boundaries.
func (e *Engine) replayMemorySamples(id signal.SeriesID, ts []int64, values, sf []float64) error {
	if !e.head.series.Has(id) {
		return nil
	}
	b := e.walB
	for i, timestamp := range ts {
		if b.samples == maxPreparedBatchSamples {
			if err := e.publishReplayBatch(); err != nil {
				return err
			}
		}
		index, found := b.pos[id]
		if !found {
			newest, seen := e.head.committedNewest(id)
			index = b.start(id, walSeriesAcc{priorNewest: newest, hadNewest: seen})
		}
		weight := float64(1)
		if sf != nil {
			weight = sf[i]
		}
		b.add(index, timestamp, values[i], weight)
	}
	return nil
}

func (e *Engine) publishReplayBatch() error {
	b := e.walB
	if b.empty() {
		return nil
	}
	defer b.reset()
	if err := e.prepareMemoryPublication(b); err != nil {
		return err
	}
	e.publishMemoryBatch(b, false)
	return nil
}

func (e *Engine) finishMemoryReplay(replayErr error) error {
	if !e.head.memoryRuns {
		return replayErr
	}
	// Keep the already decoded prefix, including its final partial batch, on replay errors.
	flushErr := e.publishReplayBatch()
	if replayErr != nil {
		return replayErr
	}
	return flushErr
}
