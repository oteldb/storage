package engine

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/backend/faultbackend"
	"github.com/oteldb/storage/internal/vfs/faultfs"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"
)

func TestDiskHeadEvictsHistoricalMetadata(t *testing.T) {
	t.Parallel()
	e := New(Config{Backend: backend.Memory(), Prefix: "bounded-head", DiskSeries: true})
	const generations = 20
	const perGeneration = 512
	for generation := range generations {
		ids := make([]signal.SeriesID, perGeneration)
		ts := make([]int64, perGeneration)
		values := make([]float64, perGeneration)
		for i := range ids {
			ids[i] = benchSeries(generation*perGeneration + i).Hash()
			ts[i] = 100
			values[i] = 1
		}
		result, err := e.AppendBatch(ids, ts, values, nil, func(i int) signal.Series { return benchSeries(generation*perGeneration + i) }, AppendLimits{})
		require.NoError(t, err)
		require.Equal(t, perGeneration, result.Accepted)
		require.NoError(t, e.Flush(t.Context()))
		require.Zero(t, e.head.series.Len())
		require.Zero(t, e.head.slots.seen)
		require.Empty(t, e.head.buffers.pages)
		require.LessOrEqual(t, len(e.head.disk.cache), diskHeadCacheEntries)
		require.EqualValues(t, (generation+1)*perGeneration, e.Stats().Series)
		require.True(t, e.Stats().SeriesComplete)
		require.Zero(t, e.identityOrder.bytes.Load())
		require.Less(t, e.IdentityBytes(), int64(3<<20))
	}
	// The first generation has left the bounded cache. Membership comes from immutable parts.
	s := benchSeries(0)
	_, cached := e.head.disk.cache[s.Hash()]
	require.False(t, cached)
	result, err := e.AppendBatch([]signal.SeriesID{s.Hash(), benchSeries(generations * perGeneration).Hash()}, []int64{101, 101}, []float64{2, 2}, nil, func(i int) signal.Series {
		if i == 0 {
			return s
		}
		return benchSeries(generations * perGeneration)
	}, AppendLimits{MaxSeries: generations * perGeneration})
	require.NoError(t, err)
	require.Equal(t, 1, result.Accepted)
	require.Equal(t, 1, result.RejectedCardinality)
	require.EqualValues(t, generations*perGeneration, e.Stats().Series)
}

func TestDiskHeadHistoricalWatermarks(t *testing.T) {
	t.Parallel()
	for _, cached := range []bool{true, false} {
		name := "disk"
		if cached {
			name = "cache"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			e := New(Config{Backend: backend.Memory(), Prefix: "ooo-head", DiskSeries: true, OOOWindow: 10})
			s := benchSeries(1)
			ok, err := e.Append(s, 100, 1)
			require.NoError(t, err)
			require.True(t, ok)
			require.NoError(t, e.Flush(t.Context()))
			if !cached {
				clear(e.head.disk.cache)
			}
			ok, err = e.Append(s, 89, 2)
			require.NoError(t, err)
			require.False(t, ok)
			require.Zero(t, e.head.series.Len())
			ok, err = e.Append(s, 95, 3)
			require.NoError(t, err)
			require.True(t, ok)
			newest, seen := e.head.watermark(s.Hash())
			require.True(t, seen)
			require.EqualValues(t, 100, newest, "older accepted sample preserves the historical watermark")
			ok, err = e.Append(s, 89, 4)
			require.NoError(t, err)
			require.False(t, ok)
			require.EqualValues(t, 1, e.Stats().Series)
		})
	}
}

func TestDiskHeadPressureFlush(t *testing.T) {
	t.Parallel()
	e := New(Config{Backend: backend.Memory(), Prefix: "pressure-head", DiskSeries: true})
	e.cfg.ResidentHeadBytes = e.IdentityBytes() + 1024
	const n = 100
	ids := make([]signal.SeriesID, n)
	ts := make([]int64, n)
	values := make([]float64, n)
	for i := range ids {
		ids[i] = benchSeries(i).Hash()
		ts[i] = 1
	}
	result, err := e.AppendBatch(ids, ts, values, nil, benchSeries, AppendLimits{})
	require.NoError(t, err)
	require.Equal(t, n, result.Accepted)
	require.Zero(t, e.PartCount(), "an atomic caller batch remains atomic even when over budget")
	ok, err := e.Append(benchSeries(n), 2, 1)
	require.NoError(t, err)
	require.True(t, ok)
	require.Positive(t, e.PartCount(), "next write flushes the prior head before admission")
	require.EqualValues(t, 1, e.Stats().HeadSamples)
	require.EqualValues(t, n+1, e.Stats().Series)
}

func TestDiskHeadFailedWALKeepsHistoricalState(t *testing.T) {
	t.Parallel()
	fsys := faultfs.New()
	w, err := wal.CreateFS(fsys, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	e := New(Config{WAL: w, Backend: backend.Memory(), Prefix: "failed-wal-head", DiskSeries: true, OOOWindow: 10})
	s := benchSeries(1)
	ok, err := e.Append(s, 100, 1)
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, e.Flush(t.Context()))
	clear(e.head.disk.cache)
	failNextWALWrite(fsys)
	ok, err = e.Append(s, 200, 2)
	require.ErrorIs(t, err, errInjectedWAL)
	require.False(t, ok)
	require.Zero(t, e.head.series.Len())
	require.EqualValues(t, 1, e.Stats().Series)
	require.Zero(t, e.Stats().HeadSamples)
	ok, err = e.Append(s, 95, 3)
	require.NoError(t, err)
	require.True(t, ok)
	newest, _ := e.head.watermark(s.Hash())
	require.EqualValues(t, 100, newest)
}

func TestDiskHeadReplayBudgetPreservesJournal(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w, err := wal.Create(dir, 0)
	require.NoError(t, err)
	e := New(Config{WAL: w})
	s := benchSeries(1)
	const n = 1024
	ids := make([]signal.SeriesID, n)
	ts := make([]int64, n)
	values := make([]float64, n)
	for i := range ids {
		ids[i] = s.Hash()
		ts[i] = int64(i)
	}
	_, err = e.AppendBatch(ids, ts, values, nil, func(int) signal.Series { return s }, AppendLimits{})
	require.NoError(t, err)
	require.NoError(t, w.Close())
	files, err := filepath.Glob(filepath.Join(dir, "*.wal"))
	require.NoError(t, err)
	require.Len(t, files, 1)
	before, err := os.ReadFile(files[0])
	require.NoError(t, err)
	recovered := New(Config{DiskSeries: true})
	recovered.cfg.ResidentHeadBytes = recovered.IdentityBytes() + 4096
	err = recovered.Replay(t.Context(), dir)
	require.ErrorContains(t, err, "WAL replay exceeds resident head budget")
	require.Zero(t, recovered.flushedEpoch)
	after, err := os.ReadFile(files[0])
	require.NoError(t, err)
	require.Equal(t, before, after)
	recovered = New(Config{DiskSeries: true})
	require.NoError(t, recovered.Replay(t.Context(), dir))
	require.EqualValues(t, n, recovered.Stats().HeadSamples)
	require.EqualValues(t, 1, recovered.Stats().Series)
}

func TestDiskHeadReopenExactCountAndHistoricalAdmission(t *testing.T) {
	t.Parallel()
	be := backend.Memory()
	cfg := Config{Backend: be, Prefix: "reopen-head", DiskSeries: true, OOOWindow: 10}
	original := New(cfg)
	s := benchSeries(1)
	for i := range 3 {
		ok, err := original.Append(s, int64(100+i), 1)
		require.NoError(t, err)
		require.True(t, ok)
		require.NoError(t, original.Flush(t.Context()))
	}
	reopened := New(cfg)
	require.NoError(t, reopened.LoadParts(t.Context()))
	require.Zero(t, reopened.head.series.Len())
	require.EqualValues(t, 1, reopened.Stats().Series, "overlapping part identities count once")
	result, err := reopened.AppendBatch([]signal.SeriesID{s.Hash(), s.Hash(), benchSeries(2).Hash()}, []int64{91, 95, 102}, []float64{1, 2, 3}, nil, func(i int) signal.Series {
		if i < 2 {
			return s
		}
		return benchSeries(2)
	}, AppendLimits{MaxSeries: 1})
	require.NoError(t, err)
	require.Equal(t, 1, result.RejectedOOO)
	require.Equal(t, 1, result.Accepted)
	require.Equal(t, 1, result.RejectedCardinality)
	require.EqualValues(t, 1, reopened.Stats().Series)
	newest, _ := reopened.head.watermark(s.Hash())
	require.EqualValues(t, 102, newest)
}

func TestDiskHeadOverflowHistoricalMembership(t *testing.T) {
	t.Parallel()
	e := New(Config{Backend: backend.Memory(), Prefix: "overflow-head", DiskSeries: true})
	overflow := stagedTestSeries("overflow")
	for _, s := range []signal.Series{benchSeries(1), overflow} {
		ok, err := e.Append(s, 100, 1)
		require.NoError(t, err)
		require.True(t, ok)
	}
	require.NoError(t, e.Flush(t.Context()))
	clear(e.head.disk.cache)
	fresh := benchSeries(2)
	result, err := e.AppendBatch([]signal.SeriesID{fresh.Hash()}, []int64{101}, []float64{2}, nil, func(int) signal.Series { return fresh }, AppendLimits{MaxSeries: 3, MaxSeriesSoft: 1, Overflow: func(signal.Series) signal.Series { return overflow }})
	require.NoError(t, err)
	require.Equal(t, 1, result.Accepted)
	require.Equal(t, 1, result.Overflowed)
	require.EqualValues(t, 2, e.Stats().Series, "re-registering an overflow identity does not grow global cardinality")
	require.NoError(t, e.Flush(t.Context()))
	payload := primaryPayload(t, seriesRun(fresh, []int64{102}, []float64{3}))
	clear(e.head.disk.cache)
	_, result, err = e.ApplyPrimary(payload, AppendLimits{MaxSeries: 3, MaxSeriesSoft: 1, Overflow: func(signal.Series) signal.Series { return overflow }})
	require.NoError(t, err)
	require.Equal(t, 1, result.Accepted)
	require.Equal(t, 1, result.Overflowed)
	require.EqualValues(t, 2, e.Stats().Series)
}

func TestDiskHeadReplicaHistoricalMembershipAndMalformed(t *testing.T) {
	t.Parallel()
	e := New(Config{Backend: backend.Memory(), Prefix: "replica-head", DiskSeries: true})
	s := benchSeries(1)
	payload := primaryPayload(t, seriesRun(s, []int64{100}, []float64{1}))
	require.NoError(t, e.ApplyReplicated(payload))
	require.NoError(t, e.Flush(t.Context()))
	clear(e.head.disk.cache)
	payload = primaryPayload(t, seriesRun(s, []int64{95}, []float64{2}), seriesRun(benchSeries(2), []int64{101}, []float64{3}))
	require.Error(t, e.ApplyReplicated(payload[:len(payload)-1]))
	require.Zero(t, e.head.series.Len())
	require.Zero(t, e.Stats().HeadSamples)
	require.EqualValues(t, 1, e.Stats().Series)
	require.NoError(t, e.ApplyReplicated(payload))
	require.EqualValues(t, 2, e.Stats().Series)
	require.EqualValues(t, 2, e.Stats().HeadSamples)
	newest, _ := e.head.watermark(s.Hash())
	require.EqualValues(t, 100, newest)
}

func TestDiskHeadCompactionPreservesActiveGeneration(t *testing.T) {
	t.Parallel()
	e := New(Config{DiskSeries: true, OOOWindow: 10})
	frozen, shared, active := benchSeries(1), benchSeries(2), benchSeries(3)
	for _, s := range []signal.Series{frozen, shared} {
		ok, err := e.Append(s, 100, 1)
		require.NoError(t, err)
		require.True(t, ok)
	}
	detached, _, _ := e.head.detach()
	e.flushing = detached
	for _, s := range []signal.Series{shared, active} {
		ok, err := e.Append(s, 105, 2)
		require.NoError(t, err)
		require.True(t, ok)
	}
	e.compactDiskHeadLocked()
	require.Equal(t, 3, e.head.series.Len(), "frozen identities remain resolvable until publication")
	before := e.head.samples[shared.Hash()]
	e.flushing = nil
	e.head.detachedBytes = 0
	e.compactDiskHeadLocked()
	require.Equal(t, 2, e.head.series.Len())
	require.Same(t, before, e.head.seriesState(shared.Hash()).buffer)
	require.Equal(t, []int64{105}, before.ts)
	require.False(t, e.head.seriesState(frozen.Hash()).known)
	require.EqualValues(t, 105, e.head.seriesState(shared.Hash()).newest)
	require.EqualValues(t, 3, e.Stats().Series)
	e.head.frozenMemory.release()
}

func TestDiskHeadFlushPublicationPreservesSnapshots(t *testing.T) {
	t.Parallel()
	be := faultbackend.Wrap(backend.Memory())
	e := New(Config{Backend: be, Prefix: "snapshot-head", DiskSeries: true})
	old, shared, active := benchSeries(1), benchSeries(2), benchSeries(3)
	for _, s := range []signal.Series{old, shared} {
		ok, err := e.Append(s, 100, 1)
		require.NoError(t, err)
		require.True(t, ok)
	}
	gate := faultbackend.NewGate()
	release := sync.OnceFunc(gate.Release)
	be.Add(gate.Rule(faultbackend.Write, func(op faultbackend.Op) bool { return strings.Contains(op.Key, "/c/") }))
	done := make(chan error, 1)
	finished := make(chan struct{})
	go func() { defer close(finished); done <- e.Flush(t.Context()) }()
	t.Cleanup(func() { release(); <-finished })
	gate.Await(t)
	for _, s := range []signal.Series{shared, active} {
		ok, err := e.Append(s, 105, 2)
		require.NoError(t, err)
		require.True(t, ok)
	}
	it, err := e.Fetch(t.Context(), fetch.Request{Start: 0, End: 200})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, it.Close()) })
	release()
	require.NoError(t, <-done)
	require.Equal(t, 2, e.head.series.Len())
	require.EqualValues(t, 3, e.Stats().Series)
	require.Equal(t, []int64{105}, e.head.samples[shared.Hash()].ts)
	rows, err := fetch.Drain(t.Context(), it)
	require.NoError(t, err)
	require.Len(t, rows, 3)
	for _, row := range rows {
		switch row.Series.Hash() {
		case old.Hash():
			require.Equal(t, []int64{100}, row.Timestamps)
		case shared.Hash():
			require.Equal(t, []int64{100, 105}, row.Timestamps)
		case active.Hash():
			require.Equal(t, []int64{105}, row.Timestamps)
		default:
			t.Fatal("unexpected series")
		}
	}
}

func TestDiskHeadUnknownMembershipDuringReadGap(t *testing.T) {
	t.Parallel()
	for _, gap := range []string{"wanted", "pending", "adopted", "foreign", "fenced"} {
		t.Run(gap, func(t *testing.T) {
			t.Parallel()
			e := New(Config{Backend: backend.Memory(), Prefix: "gap-head", DiskSeries: true})
			known, fresh := benchSeries(1), benchSeries(2)
			ok, err := e.Append(known, 100, 1)
			require.NoError(t, err)
			require.True(t, ok)
			before := e.head.disk
			want := bucketindex.Want{Prefix: "unreadable"}
			switch gap {
			case "wanted":
				e.wants = []bucketindex.Want{want}
			case "pending":
				e.pendingWants = []bucketindex.Want{want}
			case "adopted":
				e.adoptedWants = []bucketindex.Want{want}
			case "foreign":
				e.foreign = []bucketindex.Entry{{Prefix: "unreadable"}}
			case "fenced":
				e.loadErr = errInjectedWAL
			}
			ok, err = e.Append(fresh, 101, 2)
			require.ErrorIs(t, err, bucketindex.ErrFenced)
			require.False(t, ok)
			require.EqualValues(t, 1, e.Stats().Series)
			require.False(t, e.Stats().SeriesComplete)
			require.Same(t, before, e.head.disk)
			require.False(t, e.head.series.Has(fresh.Hash()))
			ok, err = e.Append(known, 101, 2)
			require.ErrorIs(t, err, bucketindex.ErrFenced)
			require.False(t, ok, "active identities also need complete historical OOO state")
			if gap != "fenced" {
				e.mu.Lock()
				err = e.rebuildDiskStateLocked(t.Context())
				e.mu.Unlock()
				require.NoError(t, err, "known gaps retain partial reads and repair progress")
				require.False(t, e.Stats().SeriesComplete)
				require.Same(t, before, e.head.disk, "incomplete recount preserves the last complete count and membership state")
			}

			e.mu.Lock()
			e.wants, e.pendingWants, e.adoptedWants = nil, nil, nil
			e.foreign = nil
			e.loadErr = nil
			err = e.rebuildDiskStateLocked(t.Context())
			e.mu.Unlock()
			require.NoError(t, err)
			require.True(t, e.Stats().SeriesComplete)
			ok, err = e.Append(fresh, 102, 3)
			require.NoError(t, err)
			require.True(t, ok)
			require.EqualValues(t, 2, e.Stats().Series)
		})
	}
}

func TestDiskHeadFailedPreloadReleasesBatchState(t *testing.T) {
	t.Parallel()
	be := faultbackend.Wrap(backend.Memory())
	e := New(Config{Backend: be, Prefix: "preload-head", DiskSeries: true})
	old, fresh := benchSeries(1), benchSeries(2)
	ok, err := e.Append(old, 100, 1)
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, e.Flush(t.Context()))
	clear(e.head.disk.cache)
	be.Add(faultbackend.Rule{Kind: faultbackend.Read, Err: errInjectedWAL})
	result, err := e.AppendBatch([]signal.SeriesID{fresh.Hash(), old.Hash()}, []int64{101, 101}, []float64{2, 2}, nil, func(i int) signal.Series {
		if i == 0 {
			return fresh
		}
		return old
	}, AppendLimits{})
	require.ErrorIs(t, err, errInjectedWAL)
	require.Zero(t, result.Accepted)
	require.Nil(t, e.head.disk.pending, "failed preloads do not retain per-batch historical state")
	require.Zero(t, e.head.series.Len())
	require.Zero(t, e.Stats().HeadSamples)
	require.EqualValues(t, 1, e.Stats().Series)
}

func TestDiskHeadBloomBoundedGrowth(t *testing.T) {
	t.Parallel()
	filter := newDiskHeadBloom(128, 2048)
	require.Zero(t, filter.sizeBytes(), "empty tenant filters allocate lazily")
	const admitted = 12000
	for i := range admitted {
		filter.add(signal.SeriesID{Lo: uint64(i)})
	}
	require.Greater(t, len(filter.segments), 1)
	for i := range admitted {
		require.True(t, filter.contains(signal.SeriesID{Lo: uint64(i)}), "growth never loses admitted IDs")
	}
	const probes = 20000
	falsePositives := 0
	for i := range probes {
		if filter.contains(signal.SeriesID{Lo: uint64(admitted + i)}) {
			falsePositives++
		}
	}
	require.Less(t, falsePositives, probes/10, "growing filters keep cold ID probes selective")
	// Exhaust the final segment without allocating beyond the configured geometric cap.
	for i := admitted; i < 100000; i++ {
		filter.add(signal.SeriesID{Lo: uint64(i)})
	}
	require.Len(t, filter.segments, 5)
	require.LessOrEqual(t, filter.sizeBytes(), int64(32<<10))
	for i := range 100000 {
		require.True(t, filter.contains(signal.SeriesID{Lo: uint64(i)}))
	}
}

func TestDiskHeadHistoricalRegistrationDoesNotFillBloom(t *testing.T) {
	t.Parallel()
	e := New(Config{Backend: backend.Memory(), Prefix: "bloom-head", DiskSeries: true})
	s := benchSeries(1)
	for i := range 4 {
		ok, err := e.Append(s, int64(i), 1)
		require.NoError(t, err)
		require.True(t, ok)
		require.NoError(t, e.Flush(t.Context()))
	}
	require.Len(t, e.head.disk.bloom.segments, 1)
	require.EqualValues(t, 1, e.head.disk.bloom.segments[0].inserted)
	require.EqualValues(t, 1, e.Stats().Series)
}
