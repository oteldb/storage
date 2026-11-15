package engine

import (
	"math"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/faultbackend"
	"github.com/oteldb/storage/internal/heaptest"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

func TestSampleArenaBuffersDoNotOverlap(t *testing.T) {
	t.Parallel()
	var a sampleArena
	defer a.release()
	buffers := make([]*sampleBuf, sampleHeaderPageSize+1)
	for i := range buffers {
		b := a.buffer()
		a.initColumns(b, 4)
		for j := range 4 {
			b.appendSample(int64(i*4+j), math.Float64frombits(uint64(i*4+j)), 1)
		}
		buffers[i] = b
	}
	for i, b := range buffers {
		b.appendSample(-1, -1, 2)
		for j := range 4 {
			require.EqualValues(t, i*4+j, b.ts[j])
			require.Equal(t, uint64(i*4+j), math.Float64bits(b.values[j]))
			require.Equal(t, math.Float64bits(1), math.Float64bits(b.sf[j]))
		}
		require.Equal(t, math.Float64bits(2), math.Float64bits(b.sf[4]))
	}
}

func TestSampleArenaReattachRetainsBothGenerations(t *testing.T) {
	t.Parallel()
	h := newHead()
	s := benchSeries(1)
	_, ok, _ := h.append(s, 1, 10, 0)
	require.True(t, ok)
	frozen, bytes, since := h.detach()
	first := h.frozenMemory.headers
	require.NotNil(t, first)
	_, ok, _ = h.append(s, 2, 20, 0)
	require.True(t, ok)
	second := h.memory.headers
	require.NotNil(t, second)
	require.NotSame(t, first, second)
	h.reattach(frozen, bytes, since)
	require.Nil(t, h.frozenMemory.headers)
	require.Nil(t, h.memory.headers)
	require.Nil(t, first.next)
	require.Nil(t, second.next)
	require.Equal(t, sampleBuf{}, second.bufs[0])
	require.Equal(t, []int64{1, 2}, h.samples[s.Hash()].ts)
	require.Equal(t, []float64{10, 20}, h.samples[s.Hash()].values)
	h.memory.release()
}

func TestSampleArenaFlushPreservesFrozenSnapshot(t *testing.T) {
	t.Parallel()
	be := faultbackend.Wrap(backend.Memory())
	e := New(Config{Backend: be, Prefix: "metrics"})
	s := benchSeries(1)
	_, err := e.Append(s, 1, 10)
	require.NoError(t, err)
	gate := faultbackend.NewGate()
	release := sync.OnceFunc(gate.Release)
	be.Add(gate.Rule(faultbackend.Write, func(op faultbackend.Op) bool { return strings.Contains(op.Key, "/c/") }))
	done := make(chan error, 1)
	finished := make(chan struct{})
	go func() { defer close(finished); done <- e.Flush(t.Context()) }()
	t.Cleanup(func() { release(); <-finished })
	gate.Await(t)
	it, err := e.Fetch(t.Context(), fetch.Request{Start: 0, End: 100})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, it.Close()) })
	release()
	require.NoError(t, <-done)
	require.Nil(t, e.head.frozenMemory.headers)
	be.Reset()
	for i := range sampleFreeHeaderPages + 1 {
		_, err := e.Append(s, int64(i+2), float64(i+20))
		require.NoError(t, err)
		require.NoError(t, e.Flush(t.Context()))
	}
	batches, err := fetch.Drain(t.Context(), it)
	require.NoError(t, err)
	require.Len(t, batches, 1)
	require.Equal(t, []int64{1}, batches[0].Timestamps)
	require.Equal(t, []float64{10}, batches[0].Values)
}

func TestSampleArenaFailedPreparedWriteDoesNotPublishMemory(t *testing.T) {
	t.Parallel()
	e, fsys := newFaultWALEngine(t, 0)
	s := stagedTestSeries("known")
	e.head.registerSeries(s)
	pool := newBatchPreparations()
	p := pool.acquire()
	defer func() { pool.release(p) }()
	ids := []signal.SeriesID{s.Hash()}
	p.prepare(ids)
	failNextWALWrite(fsys)
	_, err := e.appendPrepared(ids, []int64{1}, []float64{10}, nil,
		func(int) signal.Series { panic("known series materialized") }, AppendLimits{}, p)
	require.ErrorIs(t, err, errInjectedWAL)
	require.Empty(t, e.head.samples)
	require.Nil(t, e.head.memory.headers)
	require.Len(t, p.created, 1)
	require.NotNil(t, p.created[0])
	require.Equal(t, []int64{1}, p.created[0].ts)
	pool.release(p)
	p = pool.acquire()
	require.Nil(t, p.memory.headers)
	require.Nil(t, p.memory.numeric)
	require.Empty(t, p.created[0], "released scratch cannot retain a private compact header")
}

//nolint:paralleltest // Temporarily replaces the process-wide recyclers.
func TestSampleArenaRecyclerBound(t *testing.T) {
	// Isolate the recycler before this package's parallel tests start.
	oldNumerics, oldHeaders := freeSampleNumerics, freeSampleHeaders
	freeSampleNumerics = make(chan *sampleNumericPage, sampleFreeNumericPages)
	freeSampleHeaders = make(chan *sampleHeaderPage, sampleFreeHeaderPages)
	defer func() { freeSampleNumerics, freeSampleHeaders = oldNumerics, oldHeaders }()
	var a sampleArena
	for range sampleFreeNumericPages + 3 {
		a.columns(sampleNumericPageSize)
	}
	for range (sampleFreeHeaderPages + 3) * sampleHeaderPageSize {
		b := a.buffer()
		b.sf = []float64{2}
	}
	a.release()
	require.Nil(t, a.headers)
	require.Nil(t, a.numeric)
	require.Len(t, freeSampleNumerics, sampleFreeNumericPages)
	require.Len(t, freeSampleHeaders, sampleFreeHeaderPages)
	for range sampleFreeNumericPages {
		numeric := <-freeSampleNumerics
		require.Zero(t, numeric.used)
		require.Nil(t, numeric.next)
	}
	for range sampleFreeHeaderPages {
		headers := <-freeSampleHeaders
		require.Zero(t, headers.used)
		require.Nil(t, headers.next)
		for _, b := range headers.bufs {
			require.Equal(t, sampleBuf{}, b)
		}
	}
}

func TestPreparedSamplePageGeometry(t *testing.T) {
	t.Parallel()
	for _, size := range []int{1024, 4096} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			t.Parallel()
			p := &preparedBatch{batch: &walBatch{
				order: make([]signal.SeriesID, size), accs: make([]walSeriesAcc, size),
			}}
			defer p.memory.release()
			for i := range p.batch.accs {
				p.batch.accs[i] = walSeriesAcc{ts: []int64{1}, values: []float64{1}}
			}
			p.prepareTails()
			pages, samples := 0, 0
			for page := p.memory.numeric; page != nil; page = page.next {
				pages++
				samples += page.used
			}
			require.Equal(t, size/sampleNumericPageSize, pages)
			require.Equal(t, size, samples)
		})
	}
}

//nolint:paralleltest // Measures process-wide live heap across repeated flush failures.
func TestSampleArenaFailedFlushMemoryBound(t *testing.T) {
	heaptest.SkipUnsupported(t)
	ClearSampleArenaPoolForTest()
	const count, rounds = 256, 128
	h := newHead()
	batch := &walBatch{order: make([]signal.SeriesID, count), accs: make([]walSeriesAcc, count)}
	ts, values, sf := []int64{0}, []float64{1}, []float64{1}
	for i := range count {
		s := benchSeries(i)
		h.registerSeries(s)
		batch.order[i] = s.Hash()
		batch.accs[i] = walSeriesAcc{knownRef: true, identityRef: uint32(i), ts: ts, values: values, sf: sf}
	}
	batch.apply(h)
	base := int64(heaptest.Live())
	for round := range rounds {
		frozen, bytes, since := h.detach()
		ts[0] = int64(round + 1)
		batch.apply(h)
		live := h.samples[batch.order[0]]
		h.reattach(frozen, bytes, since)
		require.Equal(t, sampleBuf{}, *live)
		require.Nil(t, h.memory.headers)
		require.Nil(t, h.memory.numeric)
		require.Nil(t, h.frozenMemory.headers)
		require.Len(t, h.samples[batch.order[0]].ts, round+2)
	}
	grew := int64(heaptest.Live()) - base
	logical := int64(count * (rounds + 1) * SampleBytes)
	runtime.KeepAlive(h)
	runtime.KeepAlive(batch)
	require.Less(t, grew, logical*4+(1<<20), "failed generations cannot retain a page chain per retry")
}

//nolint:paralleltest // Measures process-wide heap for sparse failed generations.
func TestSampleArenaSparseFailedFlushMemoryBound(t *testing.T) {
	heaptest.SkipUnsupported(t)
	ClearSampleArenaPoolForTest()
	const count = 256
	h := newHead()
	series := make([]signal.Series, count+1)
	for i := range series {
		series[i] = benchSeries(i)
		h.registerSeries(series[i])
	}
	_, accepted, _ := h.append(series[0], 0, 0, 0)
	require.True(t, accepted)
	base := int64(heaptest.Live())
	for round := range count {
		frozen, bytes, since := h.detach()
		_, accepted, _ := h.append(series[round+1], int64(round+1), float64(round+1), 0)
		require.True(t, accepted)
		live := h.samples[series[round+1].Hash()]
		h.reattach(frozen, bytes, since)
		require.Equal(t, sampleBuf{}, *live)
		require.NotSame(t, live, h.samples[series[round+1].Hash()])
	}
	grew := int64(heaptest.Live()) - base
	runtime.KeepAlive(h)
	runtime.KeepAlive(series)
	require.Less(t, grew, int64(1<<20), "one new identity per failure cannot retain a mostly empty page per generation")
	for i := range series {
		require.Equal(t, []int64{int64(i)}, h.samples[series[i].Hash()].ts)
	}
}
