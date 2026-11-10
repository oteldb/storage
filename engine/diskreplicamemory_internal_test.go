package engine

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"
)

func replicaArenaPages(arena sampleArena) (headers, numerics, rows int) {
	for p := arena.headers; p != nil; p = p.next {
		headers++
		rows += p.used
	}
	for p := arena.numeric; p != nil; p = p.next {
		numerics++
	}
	return headers, numerics, rows
}

func TestDiskReplicaRefreshReclaimsSampleArenas(t *testing.T) {
	t.Parallel()
	for _, retainedOOO := range []bool{false, true} {
		name := "empty"
		if retainedOOO {
			name = "retained OOO"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			be := backend.Memory()
			owner := New(Config{Backend: be, Prefix: "replica-memory", DiskSeries: true, WriterID: "primary"})
			replica := New(Config{Backend: be, Prefix: "replica-memory", DiskSeries: true, WriterID: "replica", Term: func() uint64 { return 0 }})
			old := benchSeries(0)
			oldValue := math.Float64frombits(0x7ff8000000000013)
			if retainedOOO {
				ok, err := owner.Append(old, 100, 1)
				require.NoError(t, err)
				require.True(t, ok)
				require.NoError(t, owner.Flush(t.Context()))
				require.NoError(t, replica.LoadPartsUnclaimed(t.Context()))
				require.NoError(t, replica.ApplyReplicated(replicaSamplePayload(t, old, replicaTestSample{95, oldValue, 3.25})))
			}
			const generations = 24
			const width = sampleHeaderPageSize + 1
			for generation := range generations {
				payload := primaryPayload(t, func(w *wal.Writer) error {
					for i := range width {
						s := benchSeries(generation*width + i + 1)
						if err := w.WriteSeries(s.Hash(), s); err != nil {
							return err
						}
						if err := w.WriteSamplesSF(s.Hash(), []int64{int64(200 + generation)}, []float64{math.Copysign(0, -1)}, []float64{2}); err != nil {
							return err
						}
					}
					return nil
				})
				require.NoError(t, owner.ApplyReplicated(payload))
				require.NoError(t, owner.Flush(t.Context()))
				require.NoError(t, replica.ApplyReplicated(payload))
				require.NoError(t, replica.RefreshReplica(t.Context()))
				headers, numerics, rows := replicaArenaPages(replica.head.memory)
				if retainedOOO {
					require.Equal(t, 1, headers, "generation %d cannot retain old header pages", generation)
					require.Equal(t, 1, numerics, "generation %d cannot retain old numeric pages", generation)
					require.Equal(t, 1, rows)
					require.Len(t, replica.head.samples, 1)
					buf := replica.head.samples[old.Hash()]
					require.NotNil(t, buf)
					require.Equal(t, []int64{95}, buf.ts)
					require.Equal(t, math.Float64bits(oldValue), math.Float64bits(buf.values[0]))
					require.Equal(t, math.Float64bits(3.25), math.Float64bits(bufSF(buf, 0)))
					newest, seen := replica.head.watermark(old.Hash())
					require.True(t, seen)
					require.EqualValues(t, 100, newest)
					require.Same(t, buf, replica.head.seriesState(old.Hash()).buffer)
					require.EqualValues(t, 1, replica.Stats().HeadSamples)
				} else {
					require.Zero(t, headers)
					require.Zero(t, numerics)
					require.Zero(t, rows)
					require.Empty(t, replica.head.samples)
					require.Empty(t, replica.head.buffers.pages)
					require.Zero(t, replica.Stats().HeadSamples)
				}
				require.Zero(t, replica.head.replicaTrimmedBytes)
			}
			require.Zero(t, replica.flushedEpoch, "the secondary never flushed its own head")
		})
	}
}

func TestDiskReplicaArenaCompactionAmortizesPartialDrains(t *testing.T) {
	t.Parallel()
	e := New(Config{DiskSeries: true})
	s := benchSeries(1)
	for i := range 100 {
		ok, err := e.Append(s, int64(i), float64(i))
		require.NoError(t, err)
		require.True(t, ok)
	}
	original := e.head.memory.headers
	for drain := range 4 {
		e.mu.Lock()
		before := e.head.bytes
		buf := e.head.samples[s.Hash()]
		buf.ts, buf.values = buf.ts[:len(buf.ts)-10], buf.values[:len(buf.values)-10]
		e.finishDiskReplicaTrimLocked(before)
		e.mu.Unlock()
		if drain < 3 {
			require.Same(t, original, e.head.memory.headers)
		}
	}
	require.NotSame(t, original, e.head.memory.headers)
	require.Zero(t, e.head.replicaTrimmedBytes)
	require.EqualValues(t, 60, e.Stats().HeadSamples)
	require.Len(t, e.head.samples[s.Hash()].ts, 60)
	e.mu.Lock()
	e.head.replicaTrimmedBytes = SampleBytes
	e.head.detach()
	e.mu.Unlock()
	require.Zero(t, e.head.replicaTrimmedBytes, "detach starts a fresh arena generation")
	e.head.memory.release()
	e.head.frozenMemory.release()
}

func TestDiskReplicaArenaRepackPreservesBitsAndOrder(t *testing.T) {
	t.Parallel()
	e := New(Config{DiskSeries: true})
	s := benchSeries(1)
	values := []float64{math.Float64frombits(0x7ff8000000000007), math.Copysign(0, -1), math.Inf(1)}
	weights := []float64{1.25, 3.5, 2.125}
	times := []int64{3, 1, 2}
	ids := []signal.SeriesID{s.Hash(), s.Hash(), s.Hash()}
	result, err := e.AppendBatch(ids, times, values, weights, func(int) signal.Series { return s }, AppendLimits{})
	require.NoError(t, err)
	require.Equal(t, 3, result.Accepted)
	before := e.head.samples[s.Hash()]
	e.mu.Lock()
	e.head.repackReplicaSamples()
	e.mu.Unlock()
	after := e.head.samples[s.Hash()]
	require.NotSame(t, before, after)
	require.Equal(t, times, after.ts)
	for i := range values {
		require.Equal(t, math.Float64bits(values[i]), math.Float64bits(after.values[i]))
		require.Equal(t, math.Float64bits(weights[i]), math.Float64bits(after.sf[i]))
	}
	newest, seen := e.head.watermark(s.Hash())
	require.True(t, seen)
	require.EqualValues(t, 3, newest)
	require.Same(t, after, e.head.seriesState(s.Hash()).buffer)
	e.head.memory.release()
}
