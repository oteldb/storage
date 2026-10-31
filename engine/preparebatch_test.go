package engine

import (
	"slices"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

func TestPreparedBatchAdmission(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		limits AppendLimits
		fail   bool
	}{
		{name: "unlimited"},
		{name: "cardinality", limits: AppendLimits{MaxSeries: 2}},
		{name: "bytes", limits: AppendLimits{MaxInFlightBytes: 5 * SampleBytes}},
		{name: "inactive overflow", limits: AppendLimits{Overflow: func(signal.Series) signal.Series { panic("inactive overflow") }}},
		{name: "failed WAL", fail: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			prepared, fsys := newFaultWALEngine(t, 10)
			direct := New(Config{OOOWindow: 10})
			a, b, c := stagedTestSeries("a"), stagedTestSeries("b"), stagedTestSeries("c")
			for _, e := range []*Engine{prepared, direct} {
				ok, err := e.Append(a, 100, 1)
				require.NoError(t, err)
				require.True(t, ok)
			}
			idents := []signal.Series{a, b, a, c, b, a, c, a}
			ids := make([]signal.SeriesID, len(idents))
			for i := range ids {
				ids[i] = idents[i].Hash()
			}
			ts := []int64{80, 1, 95, 100, -20, 200, 110, 100}
			values := []float64{1, 2, 3, 4, 5, 6, 7, 8}
			sf := []float64{1, 2, 1, 3, 2, 1, 4, 2}
			var calls []int
			pool := newBatchPreparations()
			p := pool.acquire()
			require.NotNil(t, p)
			p.prepare(ids)
			if tc.fail {
				failNextWALWrite(fsys)
			}
			before := snapshotHead(prepared.head)
			got, err := prepared.appendPrepared(ids, ts, values, sf, func(i int) signal.Series {
				calls = append(calls, i)
				return idents[i]
			}, tc.limits, p)
			pool.release(p)
			if tc.fail {
				require.ErrorIs(t, err, errInjectedWAL)
				assert.Equal(t, AppendResult{}, got)
				assert.Equal(t, before, snapshotHead(prepared.head))
				p = pool.acquire()
				p.prepare(ids)
				calls = nil
				got, err = prepared.appendPrepared(ids, ts, values, sf, func(i int) signal.Series {
					calls = append(calls, i)
					return idents[i]
				}, tc.limits, p)
				pool.release(p)
			}
			require.NoError(t, err)
			var wantCalls []int
			want, err := direct.AppendBatch(ids, ts, values, sf, func(i int) signal.Series {
				wantCalls = append(wantCalls, i)
				return idents[i]
			}, tc.limits)
			require.NoError(t, err)
			assert.Equal(t, wantCalls, calls)
			assert.Equal(t, want, got)
			assert.Equal(t, snapshotHead(direct.head), snapshotHead(prepared.head))
			assert.Equal(t, snapshotHead(prepared.head), snapshotHead(replayedHead(t, fsys)))
		})
	}
}

func TestBatchPreparationBounded(t *testing.T) {
	t.Parallel()
	pool := newBatchPreparations()
	batches := make([]*preparedBatch, 0, maxPreparedBatches)
	for range maxPreparedBatches {
		p := pool.acquire()
		require.NotNil(t, p)
		batches = append(batches, p)
	}
	assert.Nil(t, pool.acquire(), "saturated preparation falls back instead of queueing")
	for _, p := range batches {
		pool.release(p)
	}
	p := pool.acquire()
	require.NotNil(t, p)
	assert.Contains(t, batches, p)
	pool.release(p)
}

func TestPreparedBatchReleasesReferences(t *testing.T) {
	t.Parallel()
	pool := newBatchPreparations()
	p := pool.acquire()
	s := stagedTestSeries("borrowed")
	p.prepare([]signal.SeriesID{s.Hash()})
	e, _ := newFaultWALEngine(t, 0)
	_, err := e.appendPrepared([]signal.SeriesID{s.Hash()}, []int64{1}, []float64{1}, nil,
		func(int) signal.Series { return s }, AppendLimits{}, p)
	require.NoError(t, err)
	p.batch.accs[0].attrs = slices.Clone(s.Attributes)
	p.batch.frames.Grow(maxPreparedFrameBytes + 1)
	pool.release(p)
	assert.Zero(t, p.batch.frames.Cap())
	for _, a := range p.batch.accs {
		assert.Equal(t, signal.Series{}, a.series)
		assert.Nil(t, a.buffer)
		for _, attr := range a.attrs[:cap(a.attrs)] {
			assert.Equal(t, signal.KeyValue{}, attr)
		}
	}
	p = pool.acquire()
	p.prepare([]signal.SeriesID{s.Hash()})
	_, err = e.appendPrepared([]signal.SeriesID{s.Hash()}, []int64{2}, []float64{2}, nil,
		func(int) signal.Series { return s }, AppendLimits{}, p)
	require.NoError(t, err)
	pool.release(p)
}

func TestPreparedBatchAttributeCacheBound(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		shrink bool
	}{
		{name: "full batch"},
		{name: "smaller batch", shrink: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pool := newBatchPreparations()
			p := pool.acquire()
			p.batch.accs = make([]walSeriesAcc, 4)
			for i := range p.batch.accs {
				p.batch.accs[i].attrs = make(signal.Attributes, maxPreparedAttributes/2)
				p.batch.accs[i].attrs[0] = signal.KeyValue{Key: []byte("borrowed"), Value: signal.StringValue([]byte("value"))}
			}
			if tc.shrink {
				p.batch.accs = p.batch.accs[:1]
			}
			pool.release(p)
			total := 0
			for _, acc := range p.batch.accs[:cap(p.batch.accs)] {
				total += cap(acc.attrs)
				if cap(acc.attrs) > 0 {
					require.Equal(t, signal.KeyValue{}, acc.attrs[:cap(acc.attrs)][0])
				}
			}
			assert.LessOrEqual(t, total, maxPreparedAttributes)
			assert.Positive(t, total, "bounded owned scratch remains reusable")
		})
	}
}

func TestPreparationDuringBlockedWriter(t *testing.T) {
	t.Parallel()
	e, _ := newFaultWALEngine(t, 0)
	e.mu.LockWriter()
	defer e.mu.UnlockWriter()
	pool := newBatchPreparations()
	var wg sync.WaitGroup
	for n := range maxPreparedBatches {
		wg.Go(func() {
			p := pool.acquire()
			if p == nil {
				t.Error("preparation slot unavailable")
				return
			}
			ids := make([]signal.SeriesID, maxPreparedBatchSamples)
			for i := range ids {
				ids[i] = stagedTestSeries(string(rune(i + n))).Hash()
			}
			p.prepare(ids)
			pool.release(p)
		})
	}
	wg.Wait()
	assert.Zero(t, e.head.bytes, "private preparation does not change the blocked engine")
}

func FuzzPreparedAdmissionMatchesDirect(f *testing.F) {
	f.Add([]byte{0, 100, 1, 1, 0, 80, 2, 5, 0, 110, 1, 30}, uint8(10), uint8(2), uint8(5), true)
	f.Add([]byte{1, 50, 1, 0, 2, 100, 3, 90}, uint8(0), uint8(0), uint8(0), false)
	f.Fuzz(func(t *testing.T, data []byte, ooo, maxSeries, bytes uint8, detach bool) {
		if len(data) > 512 {
			data = data[:512]
		}
		limits := AppendLimits{MaxSeries: int64(maxSeries % 8), MaxInFlightBytes: int64(bytes) * SampleBytes}
		staged, _ := newFaultWALEngine(t, int64(ooo))
		direct := New(Config{OOOWindow: int64(ooo)})
		pool := newBatchPreparations()
		for chunk := range slices.Chunk(data, 32) {
			if detach {
				staged.head.detach()
				direct.head.detach()
			}
			var ids []signal.SeriesID
			var idents []signal.Series
			var ts []int64
			var values, sf []float64
			for i := 0; i+1 < len(chunk); i += 2 {
				s := signal.Series{Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("id"), Value: signal.IntValue(int64(chunk[i] % 8))})}
				idents = append(idents, s)
				ids = append(ids, s.Hash())
				ts = append(ts, int64(chunk[i+1]))
				values = append(values, float64(chunk[i]))
				sf = append(sf, float64(1+chunk[i]%3))
			}
			p := pool.acquire()
			p.prepare(ids)
			materialize := func(i int) signal.Series { return idents[i] }
			got, err := staged.appendPrepared(ids, ts, values, sf, materialize, limits, p)
			pool.release(p)
			require.NoError(t, err)
			want, err := direct.AppendBatch(ids, ts, values, sf, materialize, limits)
			require.NoError(t, err)
			require.Equal(t, want, got)
			require.Equal(t, snapshotHead(direct.head), snapshotHead(staged.head))
		}
	})
}
