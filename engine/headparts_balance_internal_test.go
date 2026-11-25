package engine

import (
	"io"
	"math"
	"slices"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"

	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/internal/memlimit"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/readbudget"
	"github.com/oteldb/storage/signal"
)

func TestBalancedHeadJobsPreserveTwoParts(t *testing.T) {
	t.Parallel()
	file, err := backendfile.New(t.TempDir())
	require.NoError(t, err)
	pool := memlimit.NewPool(384 << 20)
	releaseMerge, err := pool.Acquire(t.Context(), 192<<20)
	require.NoError(t, err)
	defer releaseMerge()
	e := New(Config{Backend: file, Prefix: "balanced", DiskSeries: true, MetricSeriesChunks: true,
		ResidentHeadBytes: 1 << 30, FlushMemoryBytes: 64 << 20,
		FlushAdmission: pool.Acquire, TryFlushAdmission: pool.TryAcquire})
	defer func() { require.NoError(t, e.CloseWAL()) }()
	const count = 32000
	input := nodeExporterInputBatch(count, 0)
	r := input.append(e)
	require.NoError(t, r.err)
	require.Equal(t, count, r.result.Accepted)
	parts, rows, err := e.streamHead(t.Context(), e.head.samples)
	require.NoError(t, err)
	defer closePartReaders(parts)
	require.Equal(t, count, rows)
	require.Len(t, parts, 2, "balancing must not add output parts")
	var ids []signal.SeriesID
	for _, p := range parts {
		n := p.index.seriesCount()
		require.GreaterOrEqual(t, n, count*45/100)
		require.LessOrEqual(t, n, count*55/100)
		require.NoError(t, p.index.forEachID(t.Context(), func(id signal.SeriesID) { ids = append(ids, id) }))
	}
	want := slices.Clone(input.ids)
	slices.SortFunc(want, signal.SeriesID.Compare)
	require.Equal(t, want, ids, "canonical ranges remain contiguous without lost/duplicated IDs")
	reader := New(Config{Backend: file, DiskSeries: true, MetricSeriesChunks: true})
	reader.parts = parts
	// This full-fixture read verifies the output, independent of the host's query allowance.
	ctx := readbudget.With(t.Context(), readbudget.New(2<<30))
	it, err := reader.Fetch(ctx, fetch.Request{Start: cpuFixtureBase, End: cpuFixtureBase})
	require.NoError(t, err)
	defer func() { require.NoError(t, it.Close()) }()
	values := make(map[signal.SeriesID]float64, count)
	for i, id := range input.ids {
		values[id] = input.values[i]
	}
	seen := 0
	for {
		batch, err := it.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		require.Equal(t, batch.ID, batch.Series.Hash())
		require.Equal(t, []int64{cpuFixtureBase}, batch.Timestamps)
		require.Equal(t, []float64{values[batch.ID]}, batch.Values)
		batch.Release()
		seen++
	}
	require.Equal(t, count, seen)
	require.Zero(t, pool.Waiting())
	releaseMerge()
	release, ok := pool.TryAcquire(pool.Total())
	require.True(t, ok, "all build grants must return")
	release()
}

func TestHeadBalanceCutKeepsBounds(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                  string
		work                  []uint16
		rows                  []int
		original, limit, want int
	}{
		{name: "uniform tiny tail", work: []uint16{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, original: 9, want: 5},
		{name: "unequal work", work: []uint16{7, 1, 1, 1}, rows: []int{7, 1, 1, 1}, original: 3, want: 1},
		{name: "row cap blocks extra output", work: []uint16{1, 8, 6}, rows: []int{8, 1, 1}, original: 1, limit: 8, want: 1},
		{name: "same work without row cap", work: []uint16{1, 8, 6}, rows: []int{8, 1, 1}, original: 1, want: 2},
		{name: "giant indivisible series", work: []uint16{math.MaxUint16, 1, 1}, original: 2, want: 2},
		{name: "unknown work", work: []uint16{0, 1, 1}, original: 2, want: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p := headPartPlan{parts: 2}
			for i, work := range tc.work {
				rows := 1
				if tc.rows != nil {
					rows = tc.rows[i]
				}
				p.entries = append(p.entries, headRunEntry{work: work, buf: &sampleBuf{ts: make([]int64, rows)}})
			}
			p.jobs = []headPartJob{{entries: p.entries[:tc.original]}, {entries: p.entries[tc.original:]}}
			require.Equal(t, tc.want, p.balanceCut(tc.limit))
		})
	}
}
