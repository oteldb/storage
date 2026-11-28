package engine

import (
	"slices"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/internal/memlimit"
	"github.com/oteldb/storage/signal"
)

func TestHeadShapeStopsAtReservationTarget(t *testing.T) {
	t.Parallel()
	h := newHead()
	for i, rows := range []int{2, 0, 3} {
		s := benchSeries(i)
		id := s.Hash()
		h.registerSeries(s)
		if rows > 0 {
			h.bufFor(id).ts = make([]int64, rows)
		}
	}
	order := detachedIdentityOrder{view: h.series.View(), runs: [][]uint32{{2, 1}, {0}}}
	const base = 128
	full := base + 2*512 + 4*(order.view.EncodingBytes(0)+order.view.EncodingBytes(2)) + 3*48
	for _, tc := range []struct {
		name   string
		target int64
		want   int64
	}{
		{name: "base meets target", target: base, want: base},
		{name: "target reached before last run", target: 512, want: 512},
		{name: "full estimate required", target: full + 1, want: full},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := headPartBuilder{target: tc.target}
			for _, frozen := range []*head{nil, h} {
				order := order
				order.frozen = frozen
				got, err := b.headShapeBytes(t.Context(), order, h.samples, base)
				require.NoError(t, err)
				require.Equal(t, tc.want, got)
			}
		})
	}
}

func TestHeadJobsUseAggregateReservation(t *testing.T) {
	t.Parallel()
	const mib = 1 << 20
	for _, tc := range []struct {
		name     string
		peaks    []int64
		merge    int64
		parallel bool
		requests []int64
	}{
		{name: "reuse planner credit", peaks: []int64{24, 16}, merge: 192, parallel: true},
		{name: "incremental grant only", peaks: []int64{64, 32}, merge: 144, parallel: true, requests: []int64{39}},
		{name: "large pair stays serial", peaks: []int64{64, 64}, merge: 144, requests: []int64{71, 7}},
		{name: "largest job is not first", peaks: []int64{16, 24, 8}, merge: 192, parallel: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				pool := memlimit.NewPool(256 * mib)
				merge, err := pool.Acquire(t.Context(), tc.merge*mib)
				require.NoError(t, err)
				defer merge()
				parent, err := pool.Acquire(t.Context(), 64*mib)
				require.NoError(t, err)
				defer parent()
				file, err := backendfile.New(t.TempDir())
				require.NoError(t, err)
				be := &parallelPartGate{File: file, entered: make(chan struct{}), other: make(chan struct{}), resume: make(chan struct{})}
				resume := sync.OnceFunc(func() { close(be.resume) })
				defer resume()
				var requested []int64
				e := New(Config{Backend: be, Prefix: "aggregate", DiskSeries: true, MetricSeriesChunks: true,
					ResidentHeadBytes: 1 << 30, FlushMemoryBytes: 64 * mib, MaxPartBytes: 2 * partRowBytes,
					TryFlushAdmission: func(bytes int64) (func(), bool) {
						requested = append(requested, bytes/mib)
						return pool.TryAcquire(bytes)
					}})
				in := nodeExporterInputBatch(2*len(tc.peaks), 0)
				r := in.append(e)
				require.NoError(t, r.err)
				plan := headPartPlan{shared: 6 * mib, retained: mib}
				for i := range tc.peaks {
					entries := make([]headRunEntry, 0, 2)
					for _, id := range in.ids[2*i : 2*i+2] {
						ref, ok := e.head.series.Ref(id)
						require.True(t, ok)
						entries = append(entries, headRunEntry{ref: ref, buf: e.head.samples[id], ordered: true})
					}
					slices.SortFunc(entries, func(a, b headRunEntry) int {
						return e.head.series.View().ID(int(a.ref)).Compare(e.head.series.View().ID(int(b.ref)))
					})
					plan.jobs = append(plan.jobs, headPartJob{
						entries: entries, rows: 2, privateBytes: tc.peaks[i] * mib, retainedBytes: mib / int64(len(tc.peaks)),
					})
				}
				planner := headPartBuilder{ctx: t.Context(), e: e, view: e.head.series.View(), target: 64 * mib, reserved: 64 * mib}
				done := make(chan error, 1)
				var parts []*part
				go func() { var err error; parts, _, err = plan.build(t.Context(), &planner); done <- err }()
				<-be.entered
				synctest.Wait()
				overlapped := false
				select {
				case <-be.other:
					overlapped = true
				default:
				}
				resume()
				synctest.Wait()
				require.NoError(t, <-done)
				defer closePartReaders(parts)
				require.Equal(t, tc.parallel, overlapped)
				require.Equal(t, tc.requests, requested, "only missing aggregate bytes may be requested")
				require.Len(t, parts, len(tc.peaks))
				for i, p := range parts {
					require.NoError(t, p.index.forEachID(t.Context(), func(id signal.SeriesID) {
						require.Contains(t, in.ids[2*i:2*i+2], id, "output order follows the plan")
					}))
				}
				require.Zero(t, pool.Waiting())
				parent()
				merge()
				release, ok := pool.TryAcquire(pool.Total())
				require.True(t, ok, "no leaked or duplicated grants")
				release()
				require.NoError(t, e.CloseWAL())
			})
		})
	}
}
