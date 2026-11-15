package engine

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

func TestDiskHeadIdentityGenerationOwnership(t *testing.T) {
	t.Parallel()
	for _, abort := range []bool{false, true} {
		name := "publish"
		if abort {
			name = "abort and retry"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				e, fsys := newFaultWALEngine(t, 10)
				files, err := backendfile.New(t.TempDir())
				require.NoError(t, err)
				gate := &parallelPartGate{File: files, entered: make(chan struct{}), other: make(chan struct{}), resume: make(chan struct{})}
				resume := sync.OnceFunc(func() { close(gate.resume) })
				defer resume()
				e.cfg.Backend, e.cfg.Prefix = gate, "generations"
				e.cfg.DiskSeries, e.cfg.MetricSeriesChunks = true, true
				e.cfg.ResidentHeadBytes = 1 << 30
				e.head.disk = newDiskHeadStateWithCache(1 << 20)
				input := nodeExporterInputBatch(17, 0)
				for i := range input.ts {
					input.ts[i] = 100
				}
				accepted, err := e.AppendBatch(input.ids[:16], input.ts[:16], input.values[:16], nil,
					func(i int) signal.Series { return input.series[i] }, AppendLimits{MaxSeries: 17})
				require.NoError(t, err)
				require.Equal(t, 16, accepted.Accepted)
				originalCardinality := e.Cardinality(0)
				frozenIndex := e.head.series
				identityBytes := e.head.residentIdentityBytes()
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				done := make(chan error, 1)
				go func() { done <- e.Flush(ctx) }()
				<-gate.entered
				require.Equal(t, originalCardinality, e.Cardinality(0), "frozen resident cardinality remains visible")
				e.mu.RLock()
				activeIndex := e.head.series
				assert.NotSame(t, frozenIndex, activeIndex, "active and frozen registries must differ")
				assert.Zero(t, activeIndex.Len(), "the new active generation begins empty")
				assert.GreaterOrEqual(t, e.head.residentIdentityBytes(), identityBytes, "frozen identities remain charged")
				e.mu.RUnlock()
				for _, i := range []int{0, 16} {
					r, err := e.AppendBatch(input.ids[i:i+1], []int64{200}, input.values[i:i+1], nil,
						func(int) signal.Series { return input.series[i] }, AppendLimits{MaxSeries: 17})
					require.NoError(t, err)
					require.Equal(t, 1, r.Accepted)
				}
				rejected, err := e.AppendBatch(input.ids[:1], []int64{50}, input.values[:1], nil,
					func(int) signal.Series { t.Error("late input materialized"); return input.series[0] }, AppendLimits{MaxSeries: 17})
				require.NoError(t, err)
				require.Equal(t, 1, rejected.RejectedOOO)
				e.mu.RLock()
				assert.Equal(t, 16, frozenIndex.Len(), "frozen identities cannot grow")
				assert.Equal(t, 2, activeIndex.Len())
				assert.GreaterOrEqual(t, e.head.residentIdentityBytes(), identityBytes+activeIndex.SizeBytes())
				e.mu.RUnlock()
				expected := New(Config{})
				for _, s := range input.series {
					_, err := expected.Append(s, 100, 1)
					require.NoError(t, err)
				}
				cardinality := expected.Cardinality(0)
				cardinality.IndexScope = "resident_head"
				require.Equal(t, cardinality, e.Cardinality(0), "overlapping identities and symbols count once")
				checkGenerationQueries(t, e)
				if abort {
					cancel()
				}
				resume()
				synctest.Wait()
				err = <-done
				if abort {
					require.ErrorIs(t, err, context.Canceled)
					require.Equal(t, 18, e.HeadSampleCount())
					require.Len(t, replayedHead(t, fsys).samples, 17)
					checkGenerationQueries(t, e)
					require.NoError(t, e.Flush(t.Context()))
				} else {
					require.NoError(t, err)
					assert.Same(t, activeIndex, e.head.series, "publication must not rebuild active identities")
					require.Equal(t, 2, e.head.series.Len())
					require.Equal(t, 2, e.HeadSampleCount())
					require.Len(t, replayedHead(t, fsys).samples, 2, "checkpoint preserves concurrent writes")
				}
				require.NoError(t, e.Flush(t.Context()))
				late, err := e.AppendBatch(input.ids[:1], []int64{150}, input.values[:1], nil,
					func(int) signal.Series { t.Error("retired late identity materialized"); return input.series[0] }, AppendLimits{MaxSeries: 17})
				require.NoError(t, err)
				require.Equal(t, 1, late.RejectedOOO, "retirement preserves the latest accepted watermark")
				checkGenerationQueries(t, e)
				require.Equal(t, 17, e.SeriesCount())
				require.NoError(t, e.CloseWAL())
			})
		})
	}
}

func checkGenerationQueries(t *testing.T, e *Engine) {
	t.Helper()
	r := fetch.Request{Start: 0, End: 300}
	it, err := e.Fetch(t.Context(), r)
	require.NoError(t, err)
	batches, err := fetch.Drain(t.Context(), it)
	require.NoError(t, err)
	require.Len(t, batches, 17)
	var samples int
	for _, b := range batches {
		samples += len(b.Timestamps)
	}
	require.Equal(t, 18, samples)
	count, err := e.Count(t.Context(), r)
	require.NoError(t, err)
	require.Equal(t, 17, count)
	grouped, err := e.CountBy(t.Context(), r, []byte("instance"))
	require.NoError(t, err)
	require.Equal(t, map[string]int{"node-00000:9100": 8, "node-00001:9100": 8, "node-00002:9100": 1}, grouped)
	grid, err := e.CountGrid(t.Context(), r, fetch.WindowGrid{Start: 100, Step: 100, NumSteps: 2, Width: 100}, []byte("instance"))
	require.NoError(t, err)
	require.Equal(t, map[string][]int{"node-00000:9100": {8, 1}, "node-00001:9100": {8, 0}, "node-00002:9100": {0, 1}}, grid)
	matcher := &fetch.EqualMatcher{Name: "mode", Value: "user"}
	matched, err := e.Count(t.Context(), fetch.Request{Start: 0, End: 300,
		Matchers: []fetch.Matcher{{Name: []byte("mode"), Match: matcher.Predicate(), Spec: matcher}}})
	require.NoError(t, err)
	require.Equal(t, 2, matched)
	values, err := e.LabelValues(t.Context(), r, []byte("instance"))
	require.NoError(t, err)
	require.Equal(t, []string{"node-00000:9100", "node-00001:9100", "node-00002:9100"}, values)
}

func TestDiskGenerationWithoutWALPreservesKnownSeriesLimits(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		files, err := backendfile.New(t.TempDir())
		require.NoError(t, err)
		gate := &parallelPartGate{File: files, entered: make(chan struct{}), other: make(chan struct{}), resume: make(chan struct{})}
		resume := sync.OnceFunc(func() { close(gate.resume) })
		defer resume()
		e := New(Config{Backend: gate, Prefix: "no-wal-generations", DiskSeries: true, SeriesCacheBytes: -1,
			OOOWindow: 10, ResidentHeadBytes: 1 << 30, MetricSeriesChunks: true})
		input := nodeExporterInputBatch(4, 0)
		for _, i := range []int{0, 1} {
			_, err := e.Append(input.series[i], -100, float64(i))
			require.NoError(t, err)
		}
		// A recovered series record may precede its first sample.
		e.head.registerSeries(input.series[2])
		done := make(chan error, 1)
		go func() { done <- e.Flush(t.Context()) }()
		<-gate.entered
		for _, point := range []struct {
			series int
			ts     int64
		}{{series: 0, ts: -90}, {series: 2, ts: -500}} {
			r, err := e.AppendBatch(input.ids[point.series:point.series+1], []int64{point.ts}, []float64{1}, nil,
				func(int) signal.Series { return input.series[point.series] }, AppendLimits{MaxSeries: 3})
			require.NoError(t, err)
			require.Equal(t, 1, r.Accepted)
		}
		rejected, err := e.AppendBatch(input.ids[3:4], []int64{0}, []float64{1}, nil,
			func(int) signal.Series { t.Error("cardinality reject materialized"); return input.series[3] }, AppendLimits{MaxSeries: 3})
		require.NoError(t, err)
		require.Equal(t, 1, rejected.RejectedCardinality)
		require.Equal(t, 3, e.SeriesCount())
		resume()
		synctest.Wait()
		require.NoError(t, <-done)
		require.NoError(t, e.Flush(t.Context()))
		count, err := e.Count(t.Context(), fetch.Request{Start: -1000, End: 0})
		require.NoError(t, err)
		require.Equal(t, 3, count)
		require.NoError(t, e.Close(t.Context()))
	})
}

func TestRetiredGenerationRejectsStaleLookupAfterCacheEviction(t *testing.T) {
	t.Parallel()
	e, _, _ := newDiskLookupEngine(t)
	input := []signal.SeriesID{benchSeries(0).Hash()}
	p := preparedFor(input)
	defer p.memory.release()
	p.prepareOwnership()
	require.NoError(t, e.captureParallelDiskSeries(input, p))
	_, err := e.Append(benchSeries(0), 200, 2)
	require.NoError(t, err)
	e.mu.LockWriter()
	e.head.disk.cache.clear()
	e.mu.UnlockWriter()
	e.mu.RLockWriter()
	_, valid, err := e.parallelSnapshots(p, nil)
	e.mu.RUnlockWriter()
	require.NoError(t, err)
	require.True(t, valid)
	entry, hit := e.head.disk.cache.get(input[0])
	require.True(t, hit)
	require.EqualValues(t, 200, entry.newest)
	require.NoError(t, e.Flush(t.Context()))
	r, err := e.AppendBatch(input, []int64{150}, []float64{1}, nil,
		func(int) signal.Series { t.Error("late identity materialized"); return benchSeries(0) }, AppendLimits{})
	require.NoError(t, err)
	require.Equal(t, 1, r.RejectedOOO)
}

func TestFrozenWatermarkBatchesReleaseWriterOwnership(t *testing.T) {
	t.Parallel()
	for _, abort := range []bool{false, true} {
		name := "publish"
		if abort {
			name = "cancel cache pass"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				e, fs := newFaultWALEngine(t, 10)
				e.cfg.Backend, e.cfg.Prefix, e.cfg.DiskSeries = backend.Memory(), "cache-batch", true
				e.cfg.ResidentHeadBytes = 1 << 30
				e.head.disk = newDiskHeadStateWithCache(1 << 20)
				for i := range 512 {
					_, err := e.Append(benchSeries(i), 100, float64(i))
					require.NoError(t, err)
				}
				ready, proceed := make(chan struct{}), make(chan struct{})
				resume := sync.OnceFunc(func() { close(proceed) })
				defer resume()
				var once sync.Once
				e.parallel.hooks = &parallelHeadHooks{afterFrozenCacheBatch: func() {
					once.Do(func() { close(ready); <-proceed })
				}}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				done := make(chan error, 1)
				go func() { done <- e.Flush(ctx) }()
				<-ready
				appended := make(chan error, 1)
				go func() { _, err := e.Append(benchSeries(0), 200, 2); appended <- err }()
				synctest.Wait()
				progressed := false
				select {
				case err := <-appended:
					require.NoError(t, err)
					progressed = true
				default:
				}
				if abort {
					cancel()
				}
				resume()
				synctest.Wait()
				err := <-done
				if !progressed {
					require.NoError(t, <-appended)
				}
				require.True(t, progressed, "cache maintenance must release ownership between batches")
				if abort {
					require.ErrorIs(t, err, context.Canceled)
					require.Equal(t, 513, e.HeadSampleCount())
					require.Len(t, replayedHead(t, fs).samples, 512)
				} else {
					require.NoError(t, err)
					require.Equal(t, 1, e.HeadSampleCount())
					require.Len(t, replayedHead(t, fs).samples, 1)
				}
				e.parallel.hooks = nil
				require.NoError(t, e.Flush(t.Context()))
				require.Equal(t, 512, e.SeriesCount())
				require.NoError(t, e.CloseWAL())
			})
		})
	}
}
