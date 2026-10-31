package engine_test

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/faultbackend"
	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/internal/reproduce"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

func TestChunkedHeadPartRoundTrip(t *testing.T) {
	t.Parallel()
	for _, weighted := range []bool{false, true} {
		for _, unordered := range []bool{false, true} {
			t.Run("weighted="+strconv.FormatBool(weighted)+"/unordered="+strconv.FormatBool(unordered), func(t *testing.T) {
				t.Parallel()
				be, err := file.New(t.TempDir())
				require.NoError(t, err)
				cfg := engine.Config{Backend: be, Prefix: "metrics", AggregateStats: true,
					MaxPartBytes: (engine.DefaultMetricBlockRows + 1) * 32, FlushMemoryBytes: 16 << 20}
				e := engine.New(cfg)
				t.Cleanup(func() { require.NoError(t, e.Close(context.Background())) })
				const rows = 2*engine.DefaultMetricBlockRows + 3
				s := mkSeries("job", "chunked")
				ids := make([]signal.SeriesID, rows)
				ts := make([]int64, rows)
				values := make([]float64, rows)
				var sf []float64
				if weighted {
					sf = make([]float64, rows)
				}
				for i := range rows {
					ids[i], ts[i], values[i] = s.Hash(), int64(i), float64(i)
					if unordered {
						ts[i] = int64(rows - i)
					}
					if weighted {
						sf[i] = float64(1 + i%3)
					}
				}
				_, err = e.AppendBatch(ids, ts, values, sf, func(int) signal.Series { return s }, engine.AppendLimits{})
				require.NoError(t, err)
				req := fetch.Request{Start: 0, End: rows}
				before := fetchAll(t, e, req)
				require.NoError(t, e.Flush(t.Context()))
				after := fetchAll(t, e, req)
				require.Len(t, after, len(before))
				require.Len(t, after[0].Timestamps, len(before[0].Timestamps), "chunks preserve samples and weights")
				assert.Equal(t, before, after)
				require.NoError(t, e.Close(t.Context()))
				reopened := engine.New(cfg)
				t.Cleanup(func() { require.NoError(t, reopened.Close(context.Background())) })
				require.NoError(t, reopened.LoadParts(t.Context()))
				assert.Equal(t, before, fetchAll(t, reopened, req))
			})
		}
	}
}

func TestHeadPartWorkspaceSplitsIdentityRuns(t *testing.T) {
	t.Parallel()
	const workspace = 9 << 20
	var reservations []int64
	e := engine.New(engine.Config{Backend: backend.Memory(), Prefix: "metrics", FlushMemoryBytes: workspace,
		FlushAdmission: func(_ context.Context, bytes int64) (func(), error) {
			reservations = append(reservations, bytes)
			return func() {}, nil
		}})
	ids, series, ts, values := distinctSeries(4096)
	_, err := e.AppendBatch(ids, ts, values, nil, func(i int) signal.Series { return series[i] }, engine.AppendLimits{})
	require.NoError(t, err)
	require.NoError(t, e.Flush(t.Context()))
	assert.Greater(t, e.PartCount(), 1, "identity workspace, not just rows, seals bounded output parts")
	assert.Equal(t, []int64{workspace}, reservations, "normal runs fit the fixed workspace independent of head cardinality")
	assert.Len(t, fetchAll(t, e, fetch.Request{Start: 0, End: 1 << 62}), len(ids))
}

func TestHeadPartFailureAfterCompletedOutput(t *testing.T) {
	t.Parallel()
	be := faultbackend.Wrap(backend.Memory())
	completed := 0
	be.Add(faultbackend.Rule{Kind: faultbackend.Write, Err: errWriteFailed, Match: func(op faultbackend.Op) bool {
		if strings.HasSuffix(op.Key, "/sidx") {
			completed++
			return completed == 2
		}
		return false
	}})
	e := engine.New(engine.Config{Backend: be, Prefix: "metrics", MaxPartBytes: 32 * 3})
	s := mkSeries("job", "retry")
	for i := range 10 {
		_, err := e.Append(s, int64(i), float64(i))
		require.NoError(t, err)
	}
	req := fetch.Request{Start: 0, End: 10}
	before := fetchAll(t, e, req)
	require.Error(t, e.Flush(t.Context()))
	assert.Equal(t, 0, e.PartCount(), "completed candidates remain private until the entire generation is built")
	assert.Equal(t, 10, e.HeadSampleCount(), "all input rows return to the active head")
	assert.Equal(t, before, fetchAll(t, e, req))
	be.Reset()
	require.NoError(t, e.Flush(t.Context()))
	assert.Equal(t, before, fetchAll(t, e, req), "retry must not publish the private prefix twice")
}

func TestDuplicateTimestampStableAcrossFlush(t *testing.T) {
	reproduce.Unfixed(t, 572, "head fetch deduplicates equal timestamps, but the part fast path returns their raw rows")
	t.Parallel()
	e := engine.New(engine.Config{Backend: backend.Memory(), Prefix: "metrics", MaxPartBytes: 3 * 32})
	s := mkSeries("job", "redelivery")
	for i := range 5 {
		_, err := e.Append(s, int64(i/2), float64(i+10))
		require.NoError(t, err)
	}
	req := fetch.Request{Start: 0, End: 2}
	before := fetchAll(t, e, req)
	require.Len(t, before, 1)
	require.Len(t, before[0].Timestamps, 3)
	require.NoError(t, e.Flush(t.Context()))
	after := fetchAll(t, e, req)
	require.Len(t, after, 1)
	require.Len(t, after[0].Timestamps, 3)
	assert.Equal(t, before[0].Values, after[0].Values)
}

func TestHeadPartOversizedAdmissionFailure(t *testing.T) {
	t.Parallel()
	const workspace = 9 << 20
	var releases int
	e := engine.New(engine.Config{Backend: backend.Memory(), Prefix: "metrics", FlushMemoryBytes: workspace,
		FlushAdmission: func(_ context.Context, bytes int64) (func(), error) {
			if bytes > workspace {
				return nil, errWriteFailed
			}
			return func() { releases++ }, nil
		}})
	small := mkSeries("job", "small")
	large := mkSeries("job", "large")
	large.Resource.SchemaURL = []byte(strings.Repeat("x", 2<<20))
	for _, s := range []signal.Series{small, large} {
		_, err := e.Append(s, 1, 1)
		require.NoError(t, err)
	}
	require.Error(t, e.Flush(t.Context()))
	assert.Equal(t, 1, releases, "the original reservation is released before the failed upgrade")
	assert.Equal(t, 2, e.HeadSampleCount())
	assert.Zero(t, e.PartCount(), "a completed private run must not publish on a later admission failure")
}
