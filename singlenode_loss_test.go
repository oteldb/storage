package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/log"
)

// holeConfirmations mirrors the engines' constant: the consecutive passes of definitive absence a
// hole needs.
const holeConfirmations = 3

// repairView is what a test asserts about one engine's repair state, for either engine type.
type repairView struct {
	wanted    bool
	holes     int
	lostParts uint64
	lost      int64
	failed    int64
}

// lossCase is one engine type under the single-node loss tests.
type lossCase struct {
	sig signal.Signal
	// write ingests one row at ts.
	write func(t *testing.T, s *Storage, ts int64)
	// rows reads [start, end] and counts what came back.
	rows func(s *Storage, start, end int64) (int, error)
	// enumerate runs the facade's non-fetch reads over [start, end]: listings and aggregates.
	enumerate func(s *Storage, start, end int64) []error
	repair    func(t *testing.T, s *Storage) repairView
}

func lossCases() []lossCase {
	ctx := context.Background()

	return []lossCase{
		{
			sig: signal.Metric,
			write: func(t *testing.T, s *Storage, ts int64) {
				t.Helper()

				_, err := s.WriteMetrics(ctx, gaugeBatch("api", "http.requests", []int64{ts}, []float64{1}))
				require.NoError(t, err)
			},
			rows: func(s *Storage, start, end int64) (int, error) {
				it, err := s.Fetcher("default").Fetch(ctx, fetch.Request{
					Start: start, End: end, Matchers: []fetch.Matcher{nameMatcher("http.requests")},
				})
				if err != nil {
					return 0, err
				}

				got, err := fetch.Drain(ctx, it)

				return logRows(got), err
			},
			enumerate: func(s *Storage, start, end int64) []error {
				r := fetch.Request{Start: start, End: end, Matchers: []fetch.Matcher{nameMatcher("http.requests")}}
				_, series := s.MetricSeries(ctx, "default", r.Matchers, start, end)
				_, agg := s.AggregateMetrics(ctx, "default", r)
				_, step := s.AggregateMetricsStepNamed(ctx, "default", r, 100)
				_, window := s.AggregateMetricsWindowNamed(ctx, "default", r, engine.WindowSpec{Step: 100})

				return []error{series, agg, step, window}
			},
			repair: func(t *testing.T, s *Storage) repairView {
				t.Helper()

				eng, ok := s.lookupEngine("default")
				require.True(t, ok)

				st := eng.RepairStats()

				return repairView{
					wanted: eng.HasWants(), holes: len(eng.Holes()), lostParts: eng.LostParts(),
					lost: st.Lost, failed: st.Failed,
				}
			},
		},
		{
			sig: signal.Log,
			write: func(t *testing.T, s *Storage, ts int64) {
				t.Helper()

				_, err := s.WriteLogs(ctx, logBatch("api", [3]any{int(ts), 9, "line"}))
				require.NoError(t, err)
			},
			rows: func(s *Storage, start, end int64) (int, error) {
				got, err := fetchLogs(ctx, s, start, end)

				return logRows(got), err
			},
			enumerate: func(s *Storage, start, end int64) []error {
				_, series := s.LogSeries(ctx, "default", nil, start, end)
				_, keys := s.LogKeys(ctx, "default", start, end)
				_, values := s.ColumnValues(ctx, "default", ValuesRequest{
					Signal: signal.Log, Column: log.ColBody, Start: start, End: end,
				})

				return []error{series, keys, values}
			},
			repair: func(t *testing.T, s *Storage) repairView {
				t.Helper()

				eng, ok := s.lookupRecordEngine(signal.Log, "default")
				require.True(t, ok)

				st := eng.RepairStats()

				return repairView{
					wanted: eng.HasWants(), holes: len(eng.Holes()), lostParts: eng.LostParts(),
					lost: st.Lost, failed: st.Failed,
				}
			},
		},
	}
}

// seedLoss writes two single-row parts of c's signal, at 100 and 300, into a durable directory and
// destroys every object of the first behind the store's back. The next open finds the index naming
// a part the backend says does not exist — the one fact a want is minted from.
func seedLoss(t *testing.T, c lossCase) (be *flakyBackend, dir, lost string) {
	t.Helper()
	ctx := context.Background()
	dir = t.TempDir()

	fb, err := file.New(dir)
	require.NoError(t, err)

	be = &flakyBackend{Backend: fb}

	s, err := Open(ctx, Options{}, WithBackend(be), WithFlushInterval(-1))
	require.NoError(t, err)

	for _, ts := range []int64{100, 300} {
		c.write(t, s, ts)
		require.NoError(t, s.Admin().Flush(ctx, "default", c.sig))
	}

	parts := s.Parts("default", c.sig)
	require.Len(t, parts, 2)

	for _, p := range parts {
		if p.MinTime == 100 {
			lost = p.ID
		}
	}

	require.NotEmpty(t, lost)
	require.NoError(t, s.Close(ctx))

	keys, err := be.List(ctx, lost+"/")
	require.NoError(t, err)
	require.NotEmpty(t, keys)

	for _, k := range keys {
		require.NoError(t, be.Delete(ctx, k))
	}

	return be, dir, lost
}

func openLoss(t *testing.T, be *flakyBackend) *Storage {
	t.Helper()

	s, err := Open(context.Background(), Options{}, WithBackend(be), WithFlushInterval(-1))
	require.NoError(t, err)

	return s
}

func maintainTimes(t *testing.T, s *Storage, n int) {
	t.Helper()

	for range n {
		require.NoError(t, s.Admin().MaintainNow(context.Background()))
	}
}

// TestSingleNodeAcknowledgesItsOwnLoss is #539's acceptance test. A store without a cluster layer
// that loses a part fails every read reaching into it while the loss is unconfirmed, then — its
// owner set being itself — commits the hole after the same consecutive confirmations a cluster
// needs, counts the loss once, and serves the rest of the shard again.
func TestSingleNodeAcknowledgesItsOwnLoss(t *testing.T) {
	t.Parallel()

	for _, c := range lossCases() {
		t.Run(c.sig.String(), func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()

			be, _, _ := seedLoss(t, c)
			s := openLoss(t, be)

			v := c.repair(t, s)
			require.True(t, v.wanted, "the open recorded the want")

			_, err := c.rows(s, 0, 1<<62)
			require.ErrorIs(t, err, cluster.ErrShardIncomplete, "a read over the lost part fails, not short")

			for i, err := range c.enumerate(s, 0, 1<<62) {
				require.ErrorIs(t, err, cluster.ErrShardIncomplete, "enumeration %d", i)
			}

			n, err := c.rows(s, 250, 400)
			require.NoError(t, err, "a read clear of the lost part is served")
			assert.Equal(t, 1, n)

			maintainTimes(t, s, holeConfirmations-1)

			v = c.repair(t, s)
			require.True(t, v.wanted, "one confirmation short of a hole")
			require.Zero(t, v.holes)
			require.Zero(t, v.lostParts)

			_, err = c.rows(s, 0, 1<<62)
			require.ErrorIs(t, err, cluster.ErrShardIncomplete, "still failing inside the confirmation window")

			maintainTimes(t, s, 1)

			v = c.repair(t, s)
			assert.False(t, v.wanted, "the hole discharged the want")
			assert.Equal(t, 1, v.holes)
			assert.Equal(t, uint64(1), v.lostParts)
			assert.Equal(t, int64(1), v.lost)

			n, err = c.rows(s, 0, 1<<62)
			require.NoError(t, err, "reads resume once the loss is acknowledged")
			assert.Equal(t, 1, n, "and return what survived")

			for i, err := range c.enumerate(s, 0, 1<<62) {
				require.NoError(t, err, "enumeration %d", i)
			}

			maintainTimes(t, s, holeConfirmations)

			v = c.repair(t, s)
			assert.Equal(t, uint64(1), v.lostParts, "the loss is counted once")
			assert.Equal(t, int64(1), v.lost)
			require.NoError(t, s.Close(ctx))

			s = openLoss(t, be)
			t.Cleanup(func() { require.NoError(t, s.Close(ctx)) })

			v = c.repair(t, s)
			assert.False(t, v.wanted, "the acknowledgement is durable")
			assert.Equal(t, 1, v.holes)
			assert.Equal(t, uint64(1), v.lostParts)

			n, err = c.rows(s, 0, 1<<62)
			require.NoError(t, err)
			assert.Equal(t, 1, n)
		})
	}
}

// TestSingleNodeTransientErrorIsNeverEvidence pins the other half of the evidence rule: a backend
// that cannot answer says nothing about whether the part exists, however many passes it lasts.
func TestSingleNodeTransientErrorIsNeverEvidence(t *testing.T) {
	t.Parallel()

	for _, c := range lossCases() {
		t.Run(c.sig.String(), func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()

			be, _, lost := seedLoss(t, c)
			s := openLoss(t, be)
			t.Cleanup(func() { require.NoError(t, s.Close(ctx)) })

			be.failUnder(lost + "/")
			maintainTimes(t, s, 2*holeConfirmations)

			v := c.repair(t, s)
			assert.True(t, v.wanted)
			assert.Zero(t, v.holes, "a transient error must never become a hole")
			assert.Zero(t, v.lostParts)
			assert.GreaterOrEqual(t, v.failed, int64(2*holeConfirmations), "each pass counts a failure")

			_, err := c.rows(s, 0, 1<<62)
			require.ErrorIs(t, err, cluster.ErrShardIncomplete)

			be.failUnder("")
			maintainTimes(t, s, holeConfirmations-1)
			require.Zero(t, c.repair(t, s).holes, "the failed passes contributed no evidence")

			maintainTimes(t, s, 1)
			assert.Equal(t, 1, c.repair(t, s).holes)
		})
	}
}

// TestReadOnlyStoreNeverAcknowledgesALoss is the read-only fallback: a handle that must not mutate
// its backend cannot commit a hole, so it keeps the want and fails the overlapping read instead.
func TestReadOnlyStoreNeverAcknowledgesALoss(t *testing.T) {
	t.Parallel()

	for _, c := range lossCases() {
		t.Run(c.sig.String(), func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()

			_, dir, _ := seedLoss(t, c)
			before := backendDigest(t, dir)

			fb, err := file.New(dir)
			require.NoError(t, err)

			ro, err := Open(ctx, Options{}, WithBackend(fb), WithReadOnly())
			require.NoError(t, err)

			_, err = c.rows(ro, 0, 1<<62)
			require.ErrorIs(t, err, cluster.ErrShardIncomplete, "a read-only handle fails the read, not short")

			for i, err := range c.enumerate(ro, 0, 1<<62) {
				require.ErrorIs(t, err, cluster.ErrShardIncomplete, "enumeration %d", i)
			}

			n, err := c.rows(ro, 250, 400)
			require.NoError(t, err)
			assert.Equal(t, 1, n)

			require.ErrorIs(t, ro.Admin().MaintainNow(ctx), ErrReadOnly)

			v := c.repair(t, ro)
			assert.True(t, v.wanted)
			assert.Zero(t, v.holes)
			assert.Zero(t, v.lostParts)

			require.NoError(t, ro.Close(ctx))
			assert.Equal(t, before, backendDigest(t, dir), "no hole, no want, no byte written")
		})
	}
}
