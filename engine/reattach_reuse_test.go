package engine

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

type reattachPoint struct {
	ts     int64
	weight float64
}

func TestReattachReusesLargerMap(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name                 string
		frozen, active, want map[uint64][]reattachPoint
	}{
		{
			name:   "larger frozen map with overlap and empty entry",
			frozen: map[uint64][]reattachPoint{1: {{1, 1}}, 2: {{3, 1}}, 3: {}, 4: {{5, 1}}},
			active: map[uint64][]reattachPoint{1: {{2, 2}}, 5: {{7, 1}}},
			want:   map[uint64][]reattachPoint{1: {{1, 1}, {2, 2}}, 2: {{3, 1}}, 4: {{5, 1}}, 5: {{7, 1}}},
		},
		{
			name:   "larger active map and weighted frozen sample",
			frozen: map[uint64][]reattachPoint{1: {{1, 3}}},
			active: map[uint64][]reattachPoint{1: {{2, 1}}, 2: {{4, 1}}, 3: {}},
			want:   map[uint64][]reattachPoint{1: {{1, 3}, {2, 1}}, 2: {{4, 1}}, 3: {}},
		},
		{
			name:   "no active samples",
			frozen: map[uint64][]reattachPoint{1: {{1, 1}}, 2: {}},
			want:   map[uint64][]reattachPoint{1: {{1, 1}}},
		},
		{
			name:   "empty frozen buffer preserves active buffer",
			frozen: map[uint64][]reattachPoint{1: {}},
			active: map[uint64][]reattachPoint{1: {{2, 1}}, 2: {{4, 1}}},
			want:   map[uint64][]reattachPoint{1: {{2, 1}}, 2: {{4, 1}}},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			makeSamples := func(values map[uint64][]reattachPoint) (map[signal.SeriesID]*sampleBuf, int64) {
				out := make(map[signal.SeriesID]*sampleBuf, len(values))
				var bytes int64
				for key, points := range values {
					buf := &sampleBuf{}
					for _, p := range points {
						buf.appendSample(p.ts, float64(p.ts)*10, p.weight)
						bytes += SampleBytes
					}
					out[signal.SeriesID{Lo: key}] = buf
				}
				return out, bytes
			}
			active, liveBytes := makeSamples(tt.active)
			frozen, frozenBytes := makeSamples(tt.frozen)
			oldest := time.Unix(1, 0)
			h := &head{samples: active, bytes: liveBytes, detachedBytes: frozenBytes, since: time.Unix(2, 0)}
			h.reattach(frozen, frozenBytes, oldest)
			require.Equal(t, liveBytes+frozenBytes, h.bytes)
			require.Zero(t, h.detachedBytes)
			require.Equal(t, oldest, h.since)
			require.Len(t, h.samples, len(tt.want))
			for key, points := range tt.want {
				buf, ok := h.samples[signal.SeriesID{Lo: key}]
				require.True(t, ok)
				require.Len(t, buf.ts, len(points))
				for i, p := range points {
					require.Equal(t, p.ts, buf.ts[i])
					require.InDelta(t, float64(p.ts)*10, buf.values[i], 1e-12)
					require.InDelta(t, p.weight, bufSF(buf, i), 1e-12)
				}
			}
		})
	}
}
