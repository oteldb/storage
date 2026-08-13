//go:build !race

// The merge-memory guard asserts on allocation counters, which the race detector inflates with its
// own shadow allocations — so it is measured only in an uninstrumented build.

package engine_test

import (
	"context"
	"math/rand/v2"
	"runtime"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/signal"
)

// mergeCorpus flushes `parts` parts of `series` series × `samples` samples and returns the engine
// and the total row count.
func mergeCorpus(t *testing.T, series, samples, parts int, value func(r *rand.Rand, s, i int) float64) (*engine.Engine, int) {
	t.Helper()

	ctx := context.Background()
	e := engine.New(engine.Config{Backend: backend.Memory(), Prefix: "m", MaxPartBytes: 0})

	r := rand.New(rand.NewPCG(1, 2))

	ser := make([]signal.Series, series)
	ids := make([]signal.SeriesID, series)

	for i := range series {
		ser[i] = mkSeries("__name__", "node_cpu_seconds_total",
			"instance", "host-"+strconv.Itoa(i), "job", "node", "cpu", strconv.Itoa(i%64))
		ids[i] = ser[i].Hash()
	}

	n := series * samples
	batch := make([]signal.SeriesID, n)
	ts := make([]int64, n)
	vals := make([]float64, n)

	for p := range parts {
		k := 0
		for i := range series {
			for s := range samples {
				batch[k] = ids[i]
				ts[k] = int64((p*samples+s)*5000 + i%7)
				vals[k] = value(r, i, p*samples+s)
				k++
			}
		}

		resolve := func(i int) signal.Series { return ser[i/samples] }
		_, err := e.AppendBatch(batch, ts, vals, nil, resolve, engine.AppendLimits{})
		require.NoError(t, err)
		require.NoError(t, e.Flush(ctx))
	}

	return e, series * samples * parts
}

// TestMergeAllocatesBelowRawRows pins the property the streaming merge exists for: a merge must not
// allocate on the order of its output part's uncompressed rows, which is what used to couple part
// granularity to peak merge memory.
//
// The bound is deliberately loose — total bytes allocated, not peak resident. It is here to catch a
// regression to whole-column buffering, which overshoots it by an order of magnitude.
//
//nolint:paralleltest // measures process-wide allocation counters, so it must not run concurrently
func TestMergeAllocatesBelowRawRows(t *testing.T) {
	const (
		series, samples, parts = 4000, 64, 8
		partRowBytes           = 32
	)

	for _, tc := range []struct {
		name string
		// budget is the allowed allocation as a multiple of the merged rows' raw size.
		budget float64
		value  func(r *rand.Rand, s, i int) float64
	}{
		{"counter", 5, func(_ *rand.Rand, s, i int) float64 { return float64(s*1000 + i) }},
		{"noisy gauge", 5, func(r *rand.Rand, _, _ int) float64 { return r.Float64() * 1e6 }},
	} {
		//nolint:paralleltest // see above
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			e, rows := mergeCorpus(t, series, samples, parts, tc.value)

			var before, after runtime.MemStats

			runtime.GC()
			runtime.ReadMemStats(&before)

			require.NoError(t, e.Merge(ctx, 0))

			runtime.ReadMemStats(&after)

			var (
				raw     = float64(rows * partRowBytes)
				alloced = float64(after.TotalAlloc - before.TotalAlloc)
			)

			t.Logf("rows=%d raw=%.1f MiB alloced=%.1f MiB ratio=%.2fx", rows, raw/(1<<20), alloced/(1<<20), alloced/raw)

			assert.Less(t, alloced, raw*tc.budget,
				"merge allocated %.1f MiB for %.1f MiB of raw rows (%.1fx); the output should stream, "+
					"not accumulate whole columns", alloced/(1<<20), raw/(1<<20), alloced/raw)

			assert.Equal(t, 1, e.PartCount(), "the merge should have produced a single part")
		})
	}
}
