package engine

import (
	"context"
	"slices"

	"github.com/oteldb/storage/internal/mergestream"
)

const (
	memoryCoalesceDepth    = memoryDirectoryDepth - 2
	maxMemoryCoalesceBytes = 64 << 20
)

type memoryCohort struct {
	root   *memoryDirectoryNode
	prefix uint64
	depth  int
}

func closedMemoryCohort(node *memoryDirectoryNode, prefix, current uint64, depth int) memoryCohort {
	if node == nil || node.runs <= 1 {
		return memoryCohort{}
	}
	if depth < memoryCoalesceDepth {
		for i, child := range node.children {
			shift := uint((memoryDirectoryDepth - depth - 1) * memoryDirectoryBits)
			if candidate := closedMemoryCohort(child, prefix|uint64(i)<<shift, current, depth+1); candidate.root != nil {
				return candidate
			}
		}
	}
	if depth > 0 && node.bytes <= maxMemoryCoalesceBytes {
		last := prefix | (uint64(1)<<uint((memoryDirectoryDepth-depth)*memoryDirectoryBits) - 1)
		if last < current {
			return memoryCohort{root: node, prefix: prefix, depth: depth}
		}
	}
	return memoryCohort{}
}

func mergeMemoryCohort(ctx context.Context, root *memoryDirectoryNode) (*memoryRun, error) {
	adapters := make([]memoryMergeSource, 0, root.runs)
	sampled := false
	root.walk(minInt64, maxInt64, func(run *memoryRun) bool {
		adapters = append(adapters, memoryMergeSource{run: run, ordinal: len(adapters)})
		sampled = sampled || run.sf != nil
		return true
	})
	sources := make([]mergestream.Source, len(adapters))
	for i := range adapters {
		sources[i] = &adapters[i]
	}
	var keys mergestream.Keys
	keys.Reset(sources)
	entries := 0
	for keys.Next() {
		entries++
		if entries%1024 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
	}
	keys.Reset(sources)
	directory := make([]memoryRunSeries, 0, entries)
	ts, values := make([]int64, root.rows), make([]float64, root.rows)
	var sf []float64
	if sampled {
		sf = make([]float64, root.rows)
		for i := range sf {
			sf[i] = 1
		}
	}
	ranges := make([]memoryStreamRange, 0, len(adapters))
	runs := make([]tsRun, 0, len(adapters))
	positions := make([]int, len(adapters))
	merge := memorySampleHeap{indices: make([]int, 0, len(adapters))}
	position := 0
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		ranges = ranges[:0]
		if !keys.NextMatched(func(source mergestream.Source, index int) {
			input := source.(*memoryMergeSource)
			entry := input.run.series[index]
			ranges = append(ranges, memoryStreamRange{source: input, start: entry.start, end: entry.end})
		}) {
			break
		}
		slices.SortFunc(ranges, func(a, b memoryStreamRange) int { return a.source.ordinal - b.source.ordinal })
		runs = runs[:0]
		for _, source := range ranges {
			run := source.source.run
			var weights []float64
			if run.sf != nil {
				weights = run.sf[source.start:source.end]
			}
			runs = append(runs, tsRun{ts: run.ts[source.start:source.end], vals: run.values[source.start:source.end], sf: weights})
		}
		merge.reset(runs, positions)
		first := position
		position = copyMemorySeries(&merge, memorySamples{ts: ts, values: values, sf: sf}, position)
		directory = append(directory, memoryRunSeries{id: keys.Key(), start: uint32(first), end: uint32(position)})
	}
	return newMemoryRun(directory, ts, values, sf, nil)
}

func copyMemorySeries(merge *memorySampleHeap, dst memorySamples, position int) int {
	for {
		lead, lo, hi, ok := merge.next()
		if !ok {
			return position
		}
		run := &merge.runs[lead]
		end := position + hi - lo
		copy(dst.ts[position:end], run.ts[lo:hi])
		copy(dst.values[position:end], run.vals[lo:hi])
		if run.sf != nil {
			copy(dst.sf[position:end], run.sf[lo:hi])
		}
		position = end
	}
}

func (e *Engine) coalesceMemoryOnce(ctx context.Context) (bool, error) {
	e.viewMu.Lock()
	view := e.readView
	if view == nil || view.active == nil {
		e.viewMu.Unlock()
		return false, nil
	}
	view.acquire()
	current := e.runSequence
	e.viewMu.Unlock()
	defer view.release()
	cohort := closedMemoryCohort(view.active, 0, current, 0)
	source, prefix := cohort.root, cohort.prefix
	if source == nil {
		return false, nil
	}
	var release func()
	if e.cfg.FlushAdmission != nil {
		var err error
		release, err = e.cfg.FlushAdmission(ctx, source.bytes*2)
		if err != nil {
			return false, err
		}
		defer release()
	}
	run, err := mergeMemoryCohort(ctx, source)
	if err != nil {
		return false, err
	}
	e.ownMemoryRun(run)
	defer run.release()
	if e.coalescePrepared != nil {
		e.coalescePrepared()
	}
	prepared := e.newMemoryPath()
	last := prefix | (uint64(1)<<uint((memoryDirectoryDepth-cohort.depth)*memoryDirectoryBits) - 1)
	full := prepared.insert(nil, last, run)
	prepared.close()
	defer full.release()
	replacement := full.subtree(prefix, cohort.depth)
	install := e.newMemoryPath()
	defer install.close()
	next := &memoryReadView{}
	e.viewMu.Lock()
	old := e.readView
	if old == nil || old.active.subtree(prefix, cohort.depth) != source {
		e.viewMu.Unlock()
		return false, nil
	}
	next.inherit(old)
	updated := install.replaceSubtree(next.active, prefix, cohort.depth, 0, replacement)
	next.active.release()
	next.active = updated
	e.readView = next
	e.viewMu.Unlock()
	old.release()
	return true, nil
}
