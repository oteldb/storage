package engine

import (
	"context"
	"slices"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/internal/buildbudget"
	"github.com/oteldb/storage/internal/memsize"
	"github.com/oteldb/storage/internal/mergestream"
	"github.com/oteldb/storage/signal"
)

type memoryMergeSource struct {
	run     *memoryRun
	ordinal int
}

func (s *memoryMergeSource) Len() int                 { return len(s.run.series) }
func (s *memoryMergeSource) At(i int) signal.SeriesID { return s.run.series[i].id }

type memoryStreamRange struct {
	source     *memoryMergeSource
	start, end uint32
}

type memoryStreamSeries struct {
	id               signal.SeriesID
	ref              uint32
	first, end, rows int
}

func (e *Engine) streamMemoryRuns(ctx context.Context, root *memoryDirectoryNode) ([]*part, int, error) {
	if root == nil {
		return []*part{}, 0, nil
	}
	b := headPartBuilder{ctx: ctx, e: e, target: e.flushMemoryBytes(), direct: true}
	if err := b.reserve(b.target); err != nil {
		return nil, 0, err
	}
	defer func() {
		if b.writer != nil {
			b.writer.abort()
		}
		if b.release != nil {
			b.release()
		}
	}()
	e.mu.RLock()
	b.view = e.head.series.View()
	e.mu.RUnlock()
	b.refs = make([]uint32, 0, min(b.view.Len(), int(b.target/1024)))
	adapters := make([]memoryMergeSource, 0, root.runs)
	root.walk(minInt64, maxInt64, func(run *memoryRun) bool {
		adapters = append(adapters, memoryMergeSource{run: run, ordinal: len(adapters)})
		return true
	})
	sources := make([]mergestream.Source, len(adapters))
	for i := range adapters {
		sources[i] = &adapters[i]
	}
	var keys mergestream.Keys
	keys.Reset(sources)
	chunk := make([]memoryStreamSeries, 0, 256)
	ranges := make([]memoryStreamRange, 0, len(adapters))
	runs := make([]tsRun, 0, len(adapters))
	positions := make([]int, len(adapters))
	for {
		if err := ctx.Err(); err != nil {
			return nil, 0, err
		}
		chunk, ranges = chunk[:0], ranges[:0]
		for len(chunk) < cap(chunk) {
			first, rows := len(ranges), 0
			if !keys.NextMatched(func(source mergestream.Source, index int) {
				input := source.(*memoryMergeSource)
				entry := input.run.series[index]
				ranges = append(ranges, memoryStreamRange{source: input, start: entry.start, end: entry.end})
				rows += int(entry.end - entry.start)
			}) {
				break
			}
			chunk = append(chunk, memoryStreamSeries{id: keys.Key(), first: first, end: len(ranges), rows: rows})
		}
		if len(chunk) == 0 {
			break
		}
		b.orderWorkspace = memsize.Slice(adapters) + memsize.Slice(sources) + memsize.Slice(chunk) +
			memsize.Slice(ranges) + memsize.Slice(runs) + memsize.Slice(positions)
		b.orderWorkspace = buildbudget.Add(b.orderWorkspace, buildbudget.Mul(int64(len(adapters)), 128))
		if needed := max(b.target, b.baseBytes()); needed > b.reserved {
			if err := b.reserve(needed); err != nil {
				return nil, 0, err
			}
		}
		if err := e.resolveMemoryStreamRefs(&b, chunk); err != nil {
			return nil, 0, err
		}
		if err := streamMemoryChunk(&b, chunk, ranges, runs, positions); err != nil {
			return nil, 0, err
		}
	}
	if err := b.flushRun(); err != nil {
		return nil, 0, err
	}
	if int64(b.rows) != root.rows {
		return nil, 0, errors.New("memory flush lost accepted rows")
	}
	return b.parts, b.rows, nil
}

func (e *Engine) resolveMemoryStreamRefs(b *headPartBuilder, entries []memoryStreamSeries) error {
	e.mu.RLock()
	defer e.mu.RUnlock()
	for i := range entries {
		ref, found := e.head.series.Ref(entries[i].id)
		if !found || int(ref) >= b.view.Len() {
			return errors.New("memory run identity absent from captured registry")
		}
		entries[i].ref = ref
	}
	return nil
}

func appendMemoryRunSources(b *headPartBuilder, ref uint32, runs []tsRun, positions []int) error {
	for {
		lead, timestamp, rivalTS, rival := leadRun(runs, positions)
		if lead < 0 {
			return nil
		}
		run, lo := &runs[lead], positions[lead]
		hi := len(run.ts)
		if rival {
			if timestamp == rivalTS {
				hi = lo + upperBound(run.ts[lo:], timestamp)
			} else {
				hi = lo + lowerBound(run.ts[lo:], rivalTS)
			}
		}
		var sf []float64
		if run.sf != nil {
			sf = run.sf[lo:hi]
		}
		if err := b.append(ref, run.ts[lo:hi], run.vals[lo:hi], sf); err != nil {
			return err
		}
		positions[lead] = hi
	}
}

func streamMemoryChunk(b *headPartBuilder, chunk []memoryStreamSeries, ranges []memoryStreamRange, runs []tsRun, positions []int) error {
	for _, entry := range chunk {
		if err := b.addSeries(entry.ref, entry.rows, 0); err != nil {
			return err
		}
		selected := ranges[entry.first:entry.end]
		slices.SortFunc(selected, func(a, b memoryStreamRange) int { return a.source.ordinal - b.source.ordinal })
		runs = runs[:0]
		for _, source := range selected {
			run := source.source.run
			var sf []float64
			if run.sf != nil {
				sf = run.sf[source.start:source.end]
			}
			runs = append(runs, tsRun{ts: run.ts[source.start:source.end], vals: run.values[source.start:source.end], sf: sf})
		}
		clear(positions[:len(runs)])
		if err := appendMemoryRunSources(b, entry.ref, runs, positions[:len(runs)]); err != nil {
			return err
		}
	}
	return nil
}
