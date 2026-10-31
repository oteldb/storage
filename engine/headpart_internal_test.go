package engine

import (
	"math"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/signal"
)

func TestStreamedHeadMatchesFlatRows(t *testing.T) {
	t.Parallel()
	e := New(Config{Backend: backend.Memory(), Prefix: "metrics", MaxPartBytes: 33 * partRowBytes,
		FlushMemoryBytes: 9 << 20, AggregateStats: true})
	samples := make(map[signal.SeriesID]*sampleBuf)
	before := make(map[signal.SeriesID]sampleBuf)
	for i := range 5 {
		s := signal.Series{Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("id"), Value: signal.IntValue(int64(i))})}
		e.head.registerSeries(s)
		buf := &sampleBuf{}
		for row := range 2*DefaultMetricBlockRows + 3 {
			ts := int64(row / 2)
			if i%2 != 0 {
				ts = int64((2*DefaultMetricBlockRows + 3 - row) / 2)
			}
			weight := float64(1)
			if i%3 == 0 {
				weight = float64(1 + row%3)
			}
			buf.appendSample(ts, float64(row), weight)
		}
		samples[s.Hash()] = buf
		before[s.Hash()] = sampleBuf{ts: slices.Clone(buf.ts), values: slices.Clone(buf.values), sf: slices.Clone(buf.sf)}
	}
	type row struct {
		ts            int64
		value, weight uint64
	}
	want := make(map[signal.SeriesID][]row)
	flat := buildFlushColumns(samples)
	for i, id := range flat.series {
		weight := float64(1)
		if flat.sf != nil {
			weight = flat.sf[i]
		}
		want[signal.SeriesID(id)] = append(want[signal.SeriesID(id)], row{flat.ts[i], math.Float64bits(flat.value[i]), math.Float64bits(weight)})
	}
	parts, rows, err := e.streamHead(t.Context(), samples)
	require.NoError(t, err)
	assert.Equal(t, len(flat.ts), rows)
	got := make(map[signal.SeriesID][]row)
	for _, p := range parts {
		assert.LessOrEqual(t, p.rows(), 33)
		d, err := decodePart(t.Context(), p)
		require.NoError(t, err)
		require.NoError(t, p.index.forEachRange(t.Context(), func(id signal.SeriesID, rg rowRange) {
			for i := rg.start; i < rg.end; i++ {
				weight := float64(1)
				if d.sf != nil {
					weight = d.sf[i]
				}
				got[id] = append(got[id], row{d.ts[i], math.Float64bits(d.vals[i]), math.Float64bits(weight)})
			}
		}))
	}
	for id, expected := range want {
		require.Len(t, got[id], len(expected))
		assert.Equal(t, expected, got[id], "persisted rows, including duplicate timestamps, retain the flat builder's order")
		assert.Equal(t, before[id], *samples[id], "sealing never mutates buffers visible to readers or rollback")
	}
}
