package recordengine

import (
	"container/heap"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

// limitWatermarkNaive is the reference implementation the incremental heap replaced: it rebuilds a
// bounded heap over every accumulated row. Kept test-only, to pin that feeding the heap incrementally
// (one part's rows at a time) yields the same watermark.
func limitWatermarkNaive(accs map[signal.SeriesID]*recordCols, ids []signal.SeriesID, limit int, reverse bool) (int64, bool) {
	if limit <= 0 {
		return 0, false
	}

	h := &tsHeap{reverse: reverse, limit: limit, ts: make([]int64, 0, limit)}

	for _, id := range ids {
		acc := accs[id]
		if acc == nil {
			continue
		}

		for _, t := range acc.ts {
			switch {
			case len(h.ts) < limit:
				heap.Push(h, t)
			case h.better(t, h.ts[0]):
				h.ts[0] = t
				heap.Fix(h, 0)
			}
		}
	}

	if len(h.ts) < limit {
		return 0, false
	}

	return h.ts[0], true
}

// TestLimitWatermarkIncrementalMatchesNaive property-tests the incremental heap against the naive
// rebuild over random part/row sequences: after every simulated part the watermark must be identical,
// for every direction and limit.
func TestLimitWatermarkIncrementalMatchesNaive(t *testing.T) {
	t.Parallel()

	rng := rand.New(rand.NewPCG(7, 11))

	for range 200 {
		var (
			streams = 1 + rng.IntN(4)
			parts   = 1 + rng.IntN(8)
			limit   = rng.IntN(20) // includes 0: a non-top-N request
			reverse = rng.IntN(2) == 0

			ids  = make([]signal.SeriesID, streams)
			accs = make(map[signal.SeriesID]*recordCols, streams)
		)

		for i := range ids {
			ids[i] = signal.SeriesID{Lo: uint64(i + 1)}
			accs[ids[i]] = &recordCols{}
		}

		// Seed as planFetch does, from the head rows already accumulated.
		for _, id := range ids {
			for range rng.IntN(5) {
				accs[id].ts = append(accs[id].ts, int64(rng.IntN(50)))
			}
		}

		h := newLimitWatermark(accs, ids, limit, reverse)

		gotTS, gotOK := h.watermark()
		wantTS, wantOK := limitWatermarkNaive(accs, ids, limit, reverse)
		require.Equal(t, wantOK, gotOK)
		assert.Equal(t, wantTS, gotTS)

		for range parts {
			for _, id := range ids {
				acc := accs[id]
				n := len(acc.ts)

				for range rng.IntN(10) {
					acc.ts = append(acc.ts, int64(rng.IntN(50)))
				}

				h.add(acc.ts[n:])
			}

			gotTS, gotOK = h.watermark()
			wantTS, wantOK = limitWatermarkNaive(accs, ids, limit, reverse)
			require.Equalf(t, wantOK, gotOK, "limit=%d reverse=%v", limit, reverse)
			assert.Equalf(t, wantTS, gotTS, "limit=%d reverse=%v", limit, reverse)
		}
	}
}
