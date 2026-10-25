package engine_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-faster/errors"
	"golang.org/x/sync/errgroup"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

func sparseCountEngine(tb testing.TB, series int, cache int64, constant bool) (*engine.Engine, fetch.Request) {
	tb.Helper()
	ctx := context.Background()
	ser, ids := buildNamedSeries(series, "count_sparse")
	e := engine.New(engine.Config{Backend: backend.Memory(), Prefix: "count-sparse", DecodeCacheBytes: cache})
	flushCorpus(tb, ctx, e, ser, ids, 1024, 1,
		func(_, _, sample int) int64 { return int64(sample) },
		func(_, i, sample int) float64 {
			if constant {
				return 1
			}
			return float64(i + sample%13)
		})
	return e, fetch.Request{Start: 500, End: 600, Matchers: []fetch.Matcher{eqMatcher("instance", "host-0")}}
}

func TestCountSparseWindows(t *testing.T) {
	t.Parallel()
	for _, cache := range []struct {
		name  string
		bytes int64
	}{{"uncached", 0}, {"cached", 1 << 20}} {
		for _, constant := range []bool{false, true} {
			name := cache.name + "/variable-values"
			if constant {
				name = cache.name + "/constant-values"
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				e, req := sparseCountEngine(t, 4, cache.bytes, constant)
				for _, window := range []struct {
					start, end int64
					want       int
				}{{-2, -1, 0}, {0, 0, 1}, {500, 600, 1}, {1023, 1023, 1}, {1024, 1050, 0}} {
					req.Start, req.End = window.start, window.end
					got, err := e.Count(t.Context(), req)
					require.NoError(t, err)
					require.Equal(t, window.want, got)
					by, err := e.CountBy(t.Context(), req, []byte("instance"))
					require.NoError(t, err)
					if window.want == 0 {
						require.Empty(t, by)
					} else {
						require.Equal(t, map[string]int{"host-0": 1}, by)
					}
				}
			})
		}
	}
}

func TestCountSampledShortBlocks(t *testing.T) {
	t.Parallel()
	for _, cache := range []int64{0, 1 << 20} {
		t.Run(fmt.Sprintf("cache=%d", cache), func(t *testing.T) {
			t.Parallel()
			e := engine.New(engine.Config{Backend: backend.Memory(), Prefix: "count-short", MetricBlockRows: 4, DecodeCacheBytes: cache})
			s := mkSeries("__name__", "short_count", "instance", "one")
			ts := []int64{-5, -1, 0, 0, 2, 10, 11, 12, 13}
			ids := make([]signal.SeriesID, len(ts))
			vals := make([]float64, len(ts))
			sf := make([]float64, len(ts))
			for i := range ts {
				ids[i] = s.Hash()
				vals[i] = float64(i)
				sf[i] = 2
			}
			_, err := e.AppendBatch(ids, ts, vals, sf, func(int) signal.Series { return s }, engine.AppendLimits{})
			require.NoError(t, err)
			require.NoError(t, e.Flush(t.Context()))
			for _, w := range []struct {
				start, end int64
				want       int
			}{{-5, -5, 1}, {0, 0, 1}, {3, 9, 0}, {12, 12, 1}, {13, 13, 1}, {14, 15, 0}} {
				req := fetch.Request{Start: w.start, End: w.end}
				n, err := e.Count(t.Context(), req)
				require.NoError(t, err)
				require.Equal(t, w.want, n)
				by, err := e.CountBy(t.Context(), req, []byte("instance"))
				require.NoError(t, err)
				if w.want == 0 {
					require.Empty(t, by)
				} else {
					require.Equal(t, map[string]int{"one": 1}, by)
				}
			}
		})
	}
}

func TestCountConcurrentSmallCache(t *testing.T) {
	t.Parallel()
	e, req := sparseCountEngine(t, 64, 1, false)
	var group errgroup.Group
	for range 8 {
		group.Go(func() error {
			for range 16 {
				n, err := e.Count(t.Context(), req)
				if err != nil {
					return err
				}
				if n != 1 {
					return errors.New("unexpected sparse count")
				}
				by, err := e.CountBy(t.Context(), req, []byte("instance"))
				if err != nil {
					return err
				}
				if len(by) != 1 || by["host-0"] != 1 {
					return errors.New("unexpected sparse grouped count")
				}
			}
			return nil
		})
	}
	require.NoError(t, group.Wait())
}
