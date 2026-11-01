package engine

import (
	"slices"
	"sync/atomic"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/internal/memsize"
	"github.com/oteldb/storage/signal"
)

type memoryRunSeries struct {
	id         signal.SeriesID
	start, end uint32
}

type memoryRun struct {
	refs atomic.Int64

	series       []memoryRunSeries
	ts           []int64
	values, sf   []float64
	rows         int64
	duplicates   bool
	bytes        int64
	minTS, maxTS int64
	onRelease    func()
}

// Ownership of the columns transfers only on success; no numeric column is copied.
func newMemoryRun(
	series []memoryRunSeries, ts []int64, values, sf []float64, onRelease func(),
) (*memoryRun, error) {
	if len(series) == 0 || len(ts) != len(values) || sf != nil && len(sf) != len(ts) {
		return nil, errors.New("invalid memory run columns")
	}
	seen := make([]uint64, (len(ts)+63)/64)
	r := &memoryRun{series: series, ts: ts, values: values, sf: sf, onRelease: onRelease}
	for i, entry := range series {
		if i > 0 && series[i-1].id.Compare(entry.id) >= 0 {
			return nil, errors.New("memory run series are not unique and sorted")
		}
		if entry.start >= entry.end || uint64(entry.end) > uint64(len(ts)) {
			return nil, errors.New("memory run series has no valid accepted range")
		}
		if !slices.IsSorted(ts[entry.start:entry.end]) {
			return nil, errors.New("memory run timestamps are not sorted")
		}
		for row := entry.start; row < entry.end; row++ {
			if row > entry.start && ts[row] == ts[row-1] {
				r.duplicates = true
			}
			word, mask := row/64, uint64(1)<<uint(row%64)
			if seen[word]&mask != 0 {
				return nil, errors.New("memory run accepted ranges overlap")
			}
			seen[word] |= mask
			if r.rows == 0 {
				r.minTS, r.maxTS = ts[row], ts[row]
			} else {
				r.minTS, r.maxTS = min(r.minTS, ts[row]), max(r.maxTS, ts[row])
			}
			r.rows++
		}
	}
	r.bytes = memsize.Of[memoryRun]() + memsize.Slice(series) + memsize.Slice(ts) + memsize.Slice(values) + memsize.Slice(sf)
	r.refs.Store(1)
	return r, nil
}

func (r *memoryRun) acquire() {
	for {
		refs := r.refs.Load()
		if refs <= 0 {
			panic("acquire released memory run")
		}
		if r.refs.CompareAndSwap(refs, refs+1) {
			return
		}
	}
}

func (r *memoryRun) release() {
	refs := r.refs.Add(-1)
	if refs < 0 {
		panic("release unowned memory run")
	}
	if refs != 0 {
		return
	}
	r.series, r.ts, r.values, r.sf = nil, nil, nil, nil
	if release := r.onRelease; release != nil {
		r.onRelease = nil
		release()
	}
}

func (r *memoryRun) lookup(id signal.SeriesID) (rowRange, bool) {
	i, ok := slices.BinarySearchFunc(r.series, id, func(entry memoryRunSeries, target signal.SeriesID) int {
		return entry.id.Compare(target)
	})
	if !ok {
		return rowRange{}, false
	}
	entry := r.series[i]
	return rowRange{start: int(entry.start), end: int(entry.end)}, true
}
