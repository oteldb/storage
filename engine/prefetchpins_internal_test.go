package engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/internal/memsize"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

type prefetchNumericFootprint struct {
	bytes                 int64
	arrays, pins, entries int
}

func prefetchNumericBacking(plan *enginePlan, ts []int64, values, sf []float64) prefetchNumericFootprint {
	ints := make(map[*int64]int64)
	floats := make(map[*float64]int64)
	entries := make(map[*blockEntry]struct{})
	var out prefetchNumericFootprint
	addInts := func(x []int64) {
		if cap(x) > 0 {
			ints[&x[:cap(x)][0]] = int64(cap(x)) * 8
		}
	}
	addFloats := func(x []float64) {
		if cap(x) > 0 {
			floats[&x[:cap(x)][0]] = int64(cap(x)) * 8
		}
	}
	for _, r := range plan.blockReaders {
		for _, entry := range r.pins {
			out.pins++
			entries[entry] = struct{}{}
			addInts(entry.i64)
			addFloats(entry.f64)
		}
	}
	addInts(ts)
	addFloats(values)
	addFloats(sf)
	for _, n := range ints {
		out.bytes += n
	}
	for _, n := range floats {
		out.bytes += n
	}
	out.arrays = len(ints) + len(floats)
	out.entries = len(entries)
	return out
}

func TestPrefetchNumericPinsStayWithinEstimate(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		cache    int64
		weighted bool
	}{
		{name: "resident", cache: 1 << 20},
		{name: "pressure", cache: 128},
		{name: "weighted_resident", cache: 1 << 20, weighted: true},
		{name: "weighted_pressure", cache: 128, weighted: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			const blockRows = 32
			e := New(Config{Backend: backend.Memory(), Prefix: "audit/private-prefetch", DecodeCacheBytes: tc.cache, DecodeBudget: NewDecodeBudget(1 << 20), MetricBlockRows: blockRows})
			defer func() { require.NoError(t, e.Close(context.Background())) }()
			s := apiSeries()
			wantTs := make([]int64, 0, 4*blockRows)
			wantValues := make([]float64, 0, 4*blockRows)
			var wantWeights []float64
			for part := range 2 {
				for row := range 2 * blockRows {
					ts := int64(part*1000 + row + 1)
					value := float64((part+1)*100 + row*row)
					if tc.weighted {
						weight := float64(2 + row%3)
						_, err := e.AppendBatch([]signal.SeriesID{s.Hash()}, []int64{ts}, []float64{value}, []float64{weight}, func(int) signal.Series { return s }, AppendLimits{})
						require.NoError(t, err)
						wantWeights = append(wantWeights, weight)
					} else {
						_, err := e.Append(s, ts, value)
						require.NoError(t, err)
					}
					wantTs = append(wantTs, ts)
					wantValues = append(wantValues, value)
				}
				require.NoError(t, e.Flush(t.Context()))
			}
			require.Len(t, e.parts, 2, "production prefetch needs at least two touched parts")
			for _, part := range e.parts {
				require.True(t, part.blockSliceable())
				require.Equal(t, tc.weighted, part.hasSF)
				columns := []struct {
					name string
					id   colID
				}{{colTs, colTsID}, {colValue, colValID}}
				if tc.weighted {
					columns = append(columns, struct {
						name string
						id   colID
					}{colSF, colSFID})
				}
				for _, column := range columns {
					_, pin, err := e.partBlockDecoder(t.Context(), part, column.name, column.id)
					require.NoError(t, err)
					e.blockCache.release(pin)
				}
				require.NotEmpty(t, e.partGranuleTimes(t.Context(), part))
			}

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			it, err := e.Fetch(ctx, fetch.Request{Start: 0, End: 1 << 30, Scope: fetch.NewScope()})
			require.NoError(t, err)
			defer func() { require.NoError(t, it.Close()) }()
			plan := it.(*fetchIter).plan
			require.Len(t, plan.blockReaders, 2)
			columns := 2
			if tc.weighted {
				columns = 3
			}
			for _, reader := range plan.blockReaders {
				require.Len(t, reader.pins, 2*columns, "two prefetched blocks per column")
			}
			warmed := prefetchNumericBacking(plan, nil, nil, nil)
			merge, err := plan.mergeSeries(ctx, s.Hash())
			require.NoError(t, err)
			scanned := prefetchNumericBacking(plan, nil, nil, nil)
			ts, values, sf := merge.collect(nil, nil)
			require.Equal(t, wantWeights, sf)
			require.Equal(t, wantTs, ts)
			require.Equal(t, wantValues, values)
			collected := prefetchNumericBacking(plan, ts, values, sf)
			lookupBytes := int64(0)
			for _, positions := range plan.partPositions {
				lookupBytes += int64(cap(positions)) * 4
			}
			cursorScratch := int64(0)
			if len(merge.runs) > 1 {
				cursorScratch = int64(max(64, len(merge.runs))) * memsize.Of[int]()
			}
			t.Logf("warm=%+v scan=%+v collect=%+v estimate=%d numeric_estimate=%d lookup=%d collect_cursor_stack_or_heap=%d", warmed, scanned, collected, plan.budgetBytes, plan.budgetBytes-lookupBytes, lookupBytes, cursorScratch)
			require.Equal(t, 4*columns, warmed.arrays)
			require.Equal(t, warmed, scanned, "scan borrows the prefetch pins without new refs or backing arrays")
			assert.LessOrEqual(t, collected.bytes, plan.budgetBytes,
				"independently retained numeric backings exceed the accepted decode reservation")

			held := make(map[*blockEntry]struct{})
			for _, reader := range plan.blockReaders {
				for _, entry := range reader.pins {
					held[entry] = struct{}{}
				}
			}
			cancel()
			_, err = it.Next(ctx)
			require.ErrorIs(t, err, context.Canceled)
			require.NoError(t, it.Close())
			assert.Zero(t, e.budget.inFlight())
			assert.Zero(t, e.blockCache.inflight.Load())
			assert.Zero(t, blockCachePins(e.blockCache))
			for entry := range held {
				assert.Zero(t, entry.refs)
				if entry.evicted {
					assert.Nil(t, entry.i64)
					assert.Nil(t, entry.f64)
				}
			}
			for _, reader := range plan.blockReaders {
				assert.Empty(t, reader.pins)
				assert.Nil(t, reader.tsHeld)
				assert.Nil(t, reader.valHeld)
			}
			require.Equal(t, wantTs, ts, "caller-owned collect output survives cancellation/recycling")
			require.Equal(t, wantValues, values)
			require.Equal(t, wantWeights, sf)
		})
	}
}
