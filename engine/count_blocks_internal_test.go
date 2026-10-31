package engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/block"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

func TestCountBlockAdmission(t *testing.T) {
	t.Parallel()
	for _, cache := range []struct {
		name  string
		bytes int64
	}{{"uncached", 0}, {"cached", 128}} {
		t.Run(cache.name, func(t *testing.T) {
			t.Parallel()
			e := New(Config{Backend: backend.Memory(), Prefix: "count-budget", DecodeCacheBytes: cache.bytes, DecodeBudget: NewDecodeBudget(4096), MetricBlockRows: 32})
			for i := range 1024 {
				_, err := e.Append(apiSeries(), int64(i), 1)
				require.NoError(t, err)
			}
			require.NoError(t, e.Flush(t.Context()))
			for _, window := range []struct {
				name       string
				start, end int64
				bytes      int64
			}{{"edge", 500, 600, 1024}, {"covered", 0, 1023, 0}} {
				t.Run(window.name, func(t *testing.T) {
					req := fetch.Request{Start: window.start, End: window.end}
					ids, plan, err := e.planCount(t.Context(), req, false)
					require.NoError(t, err)
					var lookupBytes int64
					for _, positions := range plan.partPositions {
						lookupBytes += int64(cap(positions)) * 4
					}
					require.Equal(t, window.bytes+lookupBytes, plan.budgetBytes, "count reserves timestamp blocks and lookup metadata without result-copy space")
					got, err := plan.activeFlags(t.Context(), ids, e)
					require.NoError(t, err)
					require.Equal(t, []bool{true}, got)
					plan.releaseParts()
					require.Zero(t, e.budget.inFlight())
					if e.blockCache != nil {
						require.Zero(t, e.blockCache.inflight.Load())
						e.blockCache.mu.Lock()
						var refs int
						for _, entry := range e.blockCache.items {
							refs += entry.Value.(*blockEntry).refs
						}
						e.blockCache.mu.Unlock()
						require.Zero(t, refs)
					}
				})
			}
		})
	}
}

func TestCountLegacyScratchRecycled(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	be := backend.Memory()
	e := New(Config{Backend: be, Prefix: "count-legacy", DecodeCacheBytes: 1 << 20})
	s := apiSeries()
	ts := make([]int64, 0, 100)
	values := make([]float64, 0, 100)
	ids := make([]chunk.U128, 0, 100)
	for i := range 100 {
		_, err := e.Append(s, int64(i), float64(i))
		require.NoError(t, err)
		ts = append(ts, int64(i))
		values = append(values, float64(i))
		ids = append(ids, idToU128(s.Hash()))
	}
	require.NoError(t, e.Flush(ctx))
	w := block.NewPartWriter(block.WithSortKey(colTs), block.WithCompression(compress.AlgorithmZSTD))
	require.NoError(t, w.AddColumn(block.Column{Name: colSeries, Kind: block.KindInt128, Int128: ids}))
	require.NoError(t, w.AddColumn(block.Column{Name: colTs, Kind: block.KindInt64, Codec: chunk.CodecDoD, Int64: ts}))
	require.NoError(t, w.AddColumn(block.Column{Name: colValue, Kind: block.KindFloat64, Float64: values}))
	require.NoError(t, block.WritePart(ctx, be, "count-legacy/unblocked", w))
	p, err := openPart(ctx, be, "count-legacy/unblocked", e.cfg.Obs.Corruption)
	require.NoError(t, err)
	p.minTime, p.maxTime = 0, 99
	e.parts = []*part{p}
	req := fetch.Request{Start: 50, End: 55}
	got, err := e.Count(ctx, req)
	require.NoError(t, err)
	require.Equal(t, 1, got)
	scratch := e.decPool.Get()
	require.NotNil(t, scratch, "legacy count must return its decode scratch")
	require.GreaterOrEqual(t, cap(scratch.ts), 100)
	e.decPool.Put(scratch)
	by, err := e.CountBy(ctx, req, []byte("job"))
	require.NoError(t, err)
	require.Equal(t, map[string]int{"api": 1}, by)
	require.Same(t, scratch, e.decPool.Get(), "grouped count must return the borrowed scratch too")
}

func TestCountErrorReleasesReaders(t *testing.T) {
	t.Parallel()
	be := backend.Memory()
	e := New(Config{Backend: be, Prefix: "count-error", DecodeCacheBytes: 1 << 20, DecodeBudget: NewDecodeBudget(1 << 20), MetricBlockRows: 32})
	for _, job := range []string{"first", "second"} {
		s := signal.Series{Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("job"), Value: signal.StringValue([]byte(job))})}
		for i := range 100 {
			_, err := e.Append(s, int64(i), float64(i))
			require.NoError(t, err)
		}
		require.NoError(t, e.Flush(t.Context()))
	}
	require.Len(t, e.parts, 2)
	require.NoError(t, be.Delete(t.Context(), e.parts[1].prefix+"/c/1"))
	_, err := e.Count(t.Context(), fetch.Request{Start: 50, End: 60})
	require.Error(t, err)
	require.Zero(t, e.budget.inFlight())
	require.Zero(t, e.blockCache.inflight.Load())
	for _, part := range e.parts {
		require.Zero(t, part.refs.Load())
	}
	e.blockCache.mu.Lock()
	var refs int
	for _, entry := range e.blockCache.items {
		refs += entry.Value.(*blockEntry).refs
	}
	e.blockCache.mu.Unlock()
	require.Zero(t, refs, "a later read error must release earlier timestamp pins")
}
