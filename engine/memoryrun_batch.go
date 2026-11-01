package engine

import (
	"math"
	"slices"

	"github.com/go-faster/errors"
)

// Preparation owns its output independently of the WAL staging buffers and their recycler.
func memoryRunFromBatch(batch *walBatch, release func()) (*memoryRun, error) {
	if batch.samples <= 0 || uint64(batch.samples) > math.MaxUint32 {
		return nil, errors.New("memory run needs a bounded accepted batch")
	}
	var order []int
	if len(batch.sortedAccs) == len(batch.order) {
		order = batch.sortedAccs
	} else {
		order = make([]int, len(batch.order))
		for i := range order {
			order[i] = i
		}
		slices.SortFunc(order, func(i, j int) int { return batch.order[i].Compare(batch.order[j]) })
	}
	sampled := false
	for _, index := range order {
		sampled = sampled || batch.accs[index].sampled
	}
	if prepared := batch.prepared; prepared != nil && prepared.canTransferMemory(batch) {
		return prepared.transferMemoryRun(order, sampled, release)
	}
	entries := make([]memoryRunSeries, len(order))
	ts := make([]int64, batch.samples)
	values := make([]float64, batch.samples)
	var sf []float64
	if sampled {
		sf = make([]float64, batch.samples)
		for i := range sf {
			sf[i] = 1
		}
	}
	position := 0
	for i, index := range order {
		acc := &batch.accs[index]
		source := sampleBuf{ts: acc.ts, values: acc.values}
		if acc.sampled {
			source.sf = acc.sf
		}
		rowTS, rowValues, weights := source.ts, source.values, source.sf
		if !ascendingInt64(rowTS) {
			rowTS, rowValues, weights = sortedWindow(&source, minInt64, maxInt64, nil)
		}
		end := position + len(rowTS)
		if end > len(ts) {
			return nil, errors.New("accepted WAL rows exceed batch count")
		}
		copy(ts[position:end], rowTS)
		copy(values[position:end], rowValues)
		if weights != nil {
			copy(sf[position:end], weights)
		}
		entries[i] = memoryRunSeries{id: batch.order[index], start: uint32(position), end: uint32(end)}
		position = end
	}
	if position != len(ts) {
		return nil, errors.New("accepted WAL rows differ from batch count")
	}
	return newMemoryRun(entries, ts, values, sf, release)
}

func (p *preparedBatch) canTransferMemory(batch *walBatch) bool {
	for i := range batch.order {
		if !ascendingInt64(batch.accs[i].ts) {
			return false
		}
	}
	return true
}

// WAL encoding already copied the frames; the run takes the prepared columns without rewriting.
func (p *preparedBatch) transferMemoryRun(order []int, sampled bool, release func()) (*memoryRun, error) {
	entries := make([]memoryRunSeries, len(order))
	for i, index := range order {
		acc := &p.batch.accs[index]
		entries[i] = memoryRunSeries{id: p.batch.order[index], start: acc.rowOffset, end: acc.rowOffset + uint32(len(acc.ts))}
	}
	var sf []float64
	if sampled {
		sf = p.sf
	}
	run, err := newMemoryRun(entries, p.ts, p.values, sf, release)
	if err != nil {
		return nil, err
	}
	p.ts, p.values = nil, nil
	if sampled {
		p.sf = nil
	}
	return run, nil
}
