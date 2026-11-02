package engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/query/fetch"
)

func requireSortedNumericPins(t *testing.T, r *seriesBlockReader) {
	t.Helper()
	for i, entry := range r.pins {
		require.Equal(t, 1, entry.refs, "one owned reference per key")
		if i == 0 {
			continue
		}
		prev := r.pins[i-1].key
		require.True(t, prev.blk < entry.key.blk || prev.blk == entry.key.blk && prev.col < entry.key.col, "unique sorted numeric keys")
	}
}

func TestNumericPinReuseSurvivesEvictionAndBackwardReads(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		cache int64
	}{{"cached", 1 << 20}, {"private", 1}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := newBlockCache(tc.cache)
			r := &seriesBlockReader{engine: &Engine{blockCache: c}, part: &part{prefix: "p"}, tsB: -1, valB: -1, sfB: -1}
			for b := range 3 {
				for _, column := range []colID{colTsID, colValID, colSFID} {
					entry := &blockEntry{key: blockKey{prefix: "p", col: column, blk: b}, bytes: 16}
					if column == colTsID {
						entry.i64 = []int64{int64(b * 100), int64(b*100 + 1)}
					} else {
						entry.f64 = []float64{float64(b*100) + float64(column), float64(b*100) + float64(column) + 0.5}
					}
					owned := c.insert(entry)
					if owned.evicted {
						r.pins = append(r.pins, owned)
					} else {
						c.release(owned)
					}
				}
			}
			read := func(b int) {
				t.Helper()
				ts, err := r.tsBlock(t.Context(), b)
				require.NoError(t, err)
				require.Equal(t, []int64{int64(b * 100), int64(b*100 + 1)}, ts)
				value, err := r.valBlock(t.Context(), b)
				require.NoError(t, err)
				require.Equal(t, []float64{float64(b*100) + 1, float64(b*100) + 1.5}, value)
				sf, err := r.sfBlock(t.Context(), b)
				require.NoError(t, err)
				require.Equal(t, []float64{float64(b*100) + 2, float64(b*100) + 2.5}, sf)
				requireSortedNumericPins(t, r)
			}
			for _, b := range []int{2, 0, 1, 2} {
				read(b)
			}
			require.Len(t, r.pins, 9)
			r.releaseSeriesPins()
			require.Len(t, r.pins, 3)
			requireSortedNumericPins(t, r)
			if tc.cache > 16 {
				for _, b := range []int{0, 1, 0, 2} {
					read(b)
				}
			}
			c.evictPrefix("p")
			read(2)
			held := append([]*blockEntry(nil), r.pins...)
			r.releasePins()
			require.Empty(t, r.pins)
			assert.Zero(t, c.stats().Bytes)
			for _, entry := range held {
				assert.Zero(t, entry.refs)
				assert.Nil(t, entry.i64)
				assert.Nil(t, entry.f64)
			}
		})
	}
}

func TestNumericPinsSurvivePartialWarmError(t *testing.T) {
	t.Parallel()
	e := New(Config{Backend: backend.Memory(), Prefix: "partial-warm", MetricBlockRows: 4, DecodeCacheBytes: 1 << 20})
	defer func() { require.NoError(t, e.Close(context.Background())) }()
	for row := range 8 {
		_, err := e.Append(apiSeries(), int64(row+1), float64(row*row))
		require.NoError(t, err)
	}
	require.NoError(t, e.Flush(t.Context()))
	e.mu.Lock()
	e.head.ensureIndexSorted()
	plan := e.planFetch(e.head.resolve(nil), fetch.Request{Start: 0, End: 100})
	e.mu.Unlock()
	defer plan.releaseParts()
	require.Len(t, plan.blockReaders, 1)
	reader := plan.blockReaders[e.parts[0]]
	require.NotNil(t, reader)
	require.Error(t, reader.warm(t.Context(), []int{0, 1 << 20}))
	require.Len(t, reader.pins, 2)
	requireSortedNumericPins(t, reader)
	for _, b := range []int{1, 0, 1} {
		ts, err := reader.tsBlock(t.Context(), b)
		require.NoError(t, err)
		require.Equal(t, int64(b*4+1), ts[0])
		_, err = reader.valBlock(t.Context(), b)
		require.NoError(t, err)
		requireSortedNumericPins(t, reader)
	}
	require.Len(t, reader.pins, 4)
}
