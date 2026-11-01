package engine

import (
	"math"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMemoryDirectoryRetainsSnapshots(t *testing.T) {
	t.Parallel()
	var runsReleased, pathsReleased atomic.Int64
	r1 := testMemoryRun(t, 1, func() { runsReleased.Add(1) })
	p1 := newMemoryDirectoryPath(func() { pathsReleased.Add(1) })
	old := p1.insert(nil, 1, r1)
	p1.close()
	r1.release()
	old.acquire()
	r2 := testMemoryRun(t, 2, func() { runsReleased.Add(1) })
	p2 := newMemoryDirectoryPath(func() { pathsReleased.Add(1) })
	current := p2.insert(old, 2, r2)
	p2.close()
	r2.release()
	old.release()
	var values []int64
	old.walk(math.MinInt64, math.MaxInt64, func(r *memoryRun) bool {
		values = append(values, r.ts...)
		return true
	})
	require.Equal(t, []int64{1}, values)
	values = nil
	current.walk(math.MinInt64, math.MaxInt64, func(r *memoryRun) bool {
		values = append(values, r.ts...)
		return true
	})
	require.Equal(t, []int64{1, 2}, values)
	require.Equal(t, int64(2), current.rows)
	current.release()
	require.Equal(t, int64(1), runsReleased.Load())
	require.Equal(t, int64(1), pathsReleased.Load(), "the reader keeps the old path; the newer path can retire")
	old.release()
	require.Equal(t, int64(2), runsReleased.Load())
	require.Equal(t, int64(2), pathsReleased.Load())
}

func TestMemoryDirectoryReplacementAndPruning(t *testing.T) {
	t.Parallel()
	var root *memoryDirectoryNode
	for _, key := range []uint64{math.MaxUint64, 1 << 32, 0, 15, 16} {
		run := testMemoryRun(t, int64(key%100+1), nil)
		path := newMemoryDirectoryPath(nil)
		next := path.insert(root, key, run)
		path.close()
		run.release()
		root.release()
		root = next
	}
	var got []int64
	root.walk(1, 100, func(r *memoryRun) bool { got = append(got, r.ts[0]); return true })
	require.Equal(t, []int64{1, 16, 17, int64((uint64(1)<<32)%100 + 1), 16}, got)
	root.acquire()
	run := testMemoryRun(t, 200, nil)
	path := newMemoryDirectoryPath(nil)
	next := path.insert(root, 15, run)
	path.close()
	run.release()
	root.release()
	got = nil
	next.walk(150, 250, func(r *memoryRun) bool { got = append(got, r.ts[0]); return true })
	require.Equal(t, []int64{200}, got)
	require.Equal(t, 5, next.runs)
	next.release()
	root.release()
}

func TestMemoryDirectoryConcurrentPublication(t *testing.T) {
	t.Parallel()
	const writers, batches = 8, 32
	var runsReleased, pathsReleased atomic.Int64
	run := testMemoryRun(t, 0, func() { runsReleased.Add(1) })
	path := newMemoryDirectoryPath(func() { pathsReleased.Add(1) })
	root := path.insert(nil, 0, run)
	path.close()
	run.release()
	root.acquire()
	reader := root
	var mu sync.Mutex
	var revision uint64
	var wg sync.WaitGroup
	errors := make(chan error, writers)
	for writer := range writers {
		wg.Go(func() {
			for batch := range batches {
				value := int64(writer*batches + batch + 1)
				run, err := newMemoryRun([]memoryRunSeries{{end: 1}}, []int64{value}, []float64{float64(value)}, nil,
					func() { runsReleased.Add(1) })
				if err != nil {
					errors <- err
					return
				}
				path := newMemoryDirectoryPath(func() { pathsReleased.Add(1) })
				mu.Lock()
				revision++
				old := root
				root = path.insert(old, revision, run)
				mu.Unlock()
				path.close()
				run.release()
				old.release()
			}
		})
	}
	for range 128 {
		mu.Lock()
		snapshot := root
		snapshot.acquire()
		mu.Unlock()
		var rows int64
		snapshot.walk(math.MinInt64, math.MaxInt64, func(run *memoryRun) bool {
			require.Len(t, run.ts, 1)
			require.Equal(t, math.Float64bits(float64(run.ts[0])), math.Float64bits(run.values[0]))
			rows += run.rows
			return true
		})
		require.Equal(t, snapshot.rows, rows)
		snapshot.release()
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	var original []int64
	reader.walk(math.MinInt64, math.MaxInt64, func(run *memoryRun) bool {
		original = append(original, run.ts...)
		return true
	})
	require.Equal(t, []int64{0}, original, "a read keeps its original batch view")
	require.Equal(t, writers*batches+1, root.runs)
	reader.release()
	root.release()
	require.Equal(t, int64(writers*batches+1), runsReleased.Load())
	require.Equal(t, int64(writers*batches+1), pathsReleased.Load())
}

func TestMemoryDirectoryInsertionAllocatesNothing(t *testing.T) { //nolint:paralleltest // AllocsPerRun requires serial measurement.
	const count = 100
	paths := make([]*memoryDirectoryPath, count+1)
	for i := range paths {
		paths[i] = newMemoryDirectoryPath(nil)
	}
	run := testMemoryRun(t, 1, nil)
	var root *memoryDirectoryNode
	position := 0
	allocs := testing.AllocsPerRun(count, func() {
		path := paths[position]
		old := root
		root = path.insert(old, uint64(position), run)
		position++
		path.close()
		old.release()
	})
	require.Zero(t, allocs, "publication consumes nodes reserved before WAL success")
	require.Equal(t, count+1, root.runs)
	root.release()
	run.release()
}

func TestMemoryDirectoryAbortedReservation(t *testing.T) {
	t.Parallel()
	released := 0
	path := newMemoryDirectoryPath(func() { released++ })
	path.close()
	require.Equal(t, 1, released)
	require.Panics(t, path.close)
}

func TestMemoryDirectoryConcatenation(t *testing.T) {
	t.Parallel()
	var older, newer *memoryDirectoryNode
	var released atomic.Int64
	for i := range 256 {
		run := testMemoryRun(t, int64(i), func() { released.Add(1) })
		root := &older
		if i >= 120 {
			root = &newer
		}
		path := newMemoryDirectoryPath(nil)
		next := path.insert(*root, uint64(i), run)
		path.close()
		run.release()
		(*root).release()
		*root = next
	}
	path := newMemoryDirectoryPath(nil)
	joined := path.concat(older, newer)
	path.close()
	older.release()
	newer.release()
	require.Zero(t, released.Load())
	require.Equal(t, 256, joined.runs)
	index := 0
	joined.walk(math.MinInt64, math.MaxInt64, func(run *memoryRun) bool {
		require.Equal(t, int64(index), run.ts[0])
		index++
		return true
	})
	require.Equal(t, 256, index)
	joined.release()
	require.Equal(t, int64(256), released.Load())
}
