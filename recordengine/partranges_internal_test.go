package recordengine

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// rangeFixture flushes the requested parts over the requested streams, each part holding only a third
// of them (so most streams are absent from most parts), and returns the engine, the stream ids in
// ingest order, and every stream's ingested timestamps as the brute-force reference.
func rangeFixture(t *testing.T, parts, streams, rowsPerStream int) (*Engine, []signal.SeriesID, map[signal.SeriesID][]int64) {
	t.Helper()

	ctx := context.Background()
	schema := NewSchema(
		Column{Name: "sev", Kind: KindInt64, Codec: chunk.CodecT64},
		Column{Name: "body", Kind: KindBytes, Codec: chunk.CodecDict},
	)

	e := New(Config{Schema: schema, Backend: backend.Memory(), Prefix: "t/recs"})

	ids := make([]signal.SeriesID, streams)
	series := make([]signal.Series, streams)

	for s := range streams {
		series[s] = signal.Series{Resource: signal.Resource{Attributes: signal.NewAttributes(
			signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue(fmt.Appendf(nil, "svc-%d", s))},
		)}}
		ids[s] = series[s].Hash()
	}

	want := make(map[signal.SeriesID][]int64, streams)

	for p := range parts {
		for s := range streams {
			if (s+p)%3 != 0 {
				continue
			}

			ts := make([]int64, rowsPerStream)
			sev := make([]int64, rowsPerStream)
			bodies := make([][]byte, rowsPerStream)

			for i := range rowsPerStream {
				ts[i] = int64(p*1000 + i*10 + s)
				sev[i] = int64(i)
				bodies[i] = fmt.Appendf(nil, "p%d-s%d-r%d", p, s, i)
			}

			want[ids[s]] = append(want[ids[s]], ts...)

			_, err := e.AppendBatch(&Batch{
				Stream: ids[s], Identity: func() signal.Series { return series[s] },
				Ts: ts, Ints: [][]int64{sev}, Bytes: [][][]byte{bodies},
			}, AppendLimits{})
			require.NoError(t, err)
		}

		require.NoError(t, e.Flush(ctx))
	}

	require.Len(t, e.parts, parts)

	return e, ids, want
}

func sortedIDsOf(ids []signal.SeriesID) []signal.SeriesID {
	out := slices.Clone(ids)
	slices.SortFunc(out, signal.SeriesID.Compare)

	return out
}

// bruteRanges rebuilds a part's stream → row-span index straight from its stream column, with no
// help from part.ranges: the reference every resolution is checked against.
func bruteRanges(t *testing.T, p *part) map[signal.SeriesID]rowRange {
	t.Helper()

	col, err := p.reader.Column(context.Background(), colStream)
	require.NoError(t, err)

	raw, err := col.ID128(nil)
	require.NoError(t, err)

	out := make(map[signal.SeriesID]rowRange)

	for i, u := range raw {
		id := u128ToID(u)
		rng, ok := out[id]
		if !ok {
			rng = rowRange{start: i, end: i}
		}

		rng.end = i + 1
		out[id] = rng
	}

	return out
}

func TestPartRangesSortedAndLookup(t *testing.T) {
	t.Parallel()

	e, ids, _ := rangeFixture(t, 6, 30, 3)

	for _, p := range e.parts {
		brute := bruteRanges(t, p)

		assert.True(t, slices.IsSortedFunc(p.ranges, func(a, b streamRange) int { return a.id.Compare(b.id) }),
			"a part's stream index must be sorted by id — the merge-join depends on it")
		assert.Len(t, p.ranges, len(brute))

		var rows int

		for _, sr := range p.ranges {
			assert.Equal(t, brute[sr.id], sr.rowRange, "span of %v", sr.id)
			rows += sr.end - sr.start
		}

		assert.Equal(t, p.reader.RowCount(), rows, "the spans must partition the part's rows")

		for _, id := range ids {
			got, ok := p.lookup(id)
			want, wantOK := brute[id]
			assert.Equal(t, wantOK, ok, "holds %v", id)
			assert.Equal(t, want, got, "lookup %v", id)
		}
	}
}

func TestPartHeldStreamsMatchesBruteForce(t *testing.T) {
	t.Parallel()

	e, ids, _ := rangeFixture(t, 6, 30, 3)

	absent := []signal.SeriesID{{Hi: 1 << 40, Lo: 7}, {Hi: 0, Lo: 1}}
	rng := rand.New(rand.NewPCG(3, 4))

	sets := map[string][]signal.SeriesID{
		"empty":       nil,
		"all":         slices.Clone(ids),
		"single":      {ids[0]},
		"absentOnly":  absent,
		"withAbsent":  append(append([]signal.SeriesID{}, absent...), ids[1], ids[4], ids[29]),
		"duplicates":  {ids[3], ids[3], ids[6]},
		"belowRange":  {{Hi: 0, Lo: 0}},
		"singleOther": {ids[len(ids)-1]},
	}

	subset := make([]signal.SeriesID, 0, len(ids))
	for _, id := range ids {
		if rng.IntN(2) == 0 {
			subset = append(subset, id)
		}
	}

	sets["random"] = subset

	for name, set := range sets {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			sorted := sortedIDsOf(set)

			for pi, p := range e.parts {
				brute := bruteRanges(t, p)

				var want []streamRange

				for _, id := range sorted {
					if r, ok := brute[id]; ok {
						want = append(want, streamRange{id: id, rowRange: r})
					}
				}

				got := p.heldStreams(nil, sorted)
				assert.Equal(t, want, got, "part %d", pi)
			}
		})
	}
}

// TestPlanFetchResolvesEveryRow is the row-loss backstop: the plan's per-stream accumulators, after
// the part scan, must hold exactly the ingested records — over a store where most streams are absent
// from most parts, and for whole, single-stream and empty requests alike.
func TestPlanFetchResolvesEveryRow(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e, ids, want := rangeFixture(t, 8, 24, 4)

	read := func(t *testing.T, ask []signal.SeriesID) map[signal.SeriesID][]int64 {
		t.Helper()

		e.mu.RLock()
		plan, err := e.planFetch(ctx, ask, fetch.Request{Signal: signal.Log, Start: 0, End: maxInt64})
		e.mu.RUnlock()

		defer plan.releaseParts()

		require.NoError(t, err)
		require.NoError(t, plan.readParts(ctx))

		got := make(map[signal.SeriesID][]int64, len(ask))

		for _, id := range ask {
			acc := plan.accs[id]
			require.NotNil(t, acc)

			ts := slices.Clone(acc.ts)
			slices.Sort(ts)
			got[id] = ts
		}

		return got
	}

	t.Run("all", func(t *testing.T) {
		t.Parallel()

		got := read(t, ids)
		for _, id := range ids {
			expect := slices.Clone(want[id])
			slices.Sort(expect)
			assert.Equal(t, expect, got[id], "stream %v", id)
		}
	})

	t.Run("single", func(t *testing.T) {
		t.Parallel()

		for _, id := range []signal.SeriesID{ids[0], ids[5], ids[len(ids)-1]} {
			expect := slices.Clone(want[id])
			slices.Sort(expect)
			assert.Equal(t, expect, read(t, []signal.SeriesID{id})[id], "stream %v", id)
		}
	})

	t.Run("absent", func(t *testing.T) {
		t.Parallel()

		got := read(t, []signal.SeriesID{{Hi: 1 << 40, Lo: 3}})
		for _, ts := range got {
			assert.Empty(t, ts)
		}
	})

	t.Run("empty", func(t *testing.T) {
		t.Parallel()

		e.mu.RLock()
		plan, err := e.planFetch(ctx, nil, fetch.Request{Signal: signal.Log, Start: 0, End: maxInt64})
		e.mu.RUnlock()

		defer plan.releaseParts()

		require.NoError(t, err)
		assert.Empty(t, plan.liveParts)
		require.NoError(t, plan.readParts(ctx))
	})
}
