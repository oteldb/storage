package engine_test

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"
)

// distinctSeries builds n distinct series (label id=i) plus the parallel id/ts/value slices.
func distinctSeries(n int) (ids []signal.SeriesID, series []signal.Series, ts []int64, vals []float64) {
	ids = make([]signal.SeriesID, n)
	series = make([]signal.Series, n)
	ts = make([]int64, n)
	vals = make([]float64, n)

	for i := range series {
		series[i] = mkSeries("id", strconv.Itoa(i))
		ids[i] = series[i].Hash()
		ts[i], vals[i] = int64(i+1), float64(i)
	}

	return ids, series, ts, vals
}

func TestSoftCardinalityBudgetOverflow(t *testing.T) {
	t.Parallel()

	e := engine.New(engine.Config{Backend: backend.Memory(), Prefix: "default/metrics"})

	overflowSeries := mkSeries("__name__", "ovf")
	limits := engine.AppendLimits{
		MaxSeries:     100,
		MaxSeriesSoft: 2,
		Overflow:      func(signal.Series) signal.Series { return overflowSeries },
	}

	ids, series, ts, vals := distinctSeries(5)

	res, err := e.AppendBatch(ids, ts, vals, nil, func(i int) signal.Series { return series[i] }, limits)
	require.NoError(t, err)

	// All 5 retained: 2 under the soft budget, 3 routed to the (single) overflow series.
	assert.Equal(t, 5, res.Accepted, "overflowed points still count as accepted")
	assert.Equal(t, 3, res.Overflowed)
	assert.Equal(t, 0, res.RejectedCardinality, "nothing rejected — overflow absorbed the spike")
	assert.Equal(t, 3, e.SeriesCount(), "2 normal series + 1 collapsed overflow series")

	// The overflow series carries the 3 redirected samples.
	got := fetchAll(t, e, fetch.Request{Start: 0, End: 1 << 62, Matchers: []fetch.Matcher{eqMatcher("__name__", "ovf")}})
	require.Len(t, got, 1)
	assert.Len(t, got[0].Timestamps, 3, "three samples collapsed into the overflow series")
}

func TestSoftBudgetDisabledHardRejects(t *testing.T) {
	t.Parallel()

	e := engine.New(engine.Config{Backend: backend.Memory(), Prefix: "default/metrics"})

	ids, series, ts, vals := distinctSeries(3)

	// No Overflow callback ⇒ a hard reject at MaxSeries (today's behavior, unchanged).
	res, err := e.AppendBatch(ids, ts, vals, nil, func(i int) signal.Series { return series[i] },
		engine.AppendLimits{MaxSeries: 2})
	require.NoError(t, err)
	assert.Equal(t, 2, res.Accepted)
	assert.Equal(t, 1, res.RejectedCardinality)
	assert.Equal(t, 0, res.Overflowed)
}

// TestWALReplayRestoresOverflowSeries proves overflow routing is WAL-consistent: replay reconstructs
// the overflow series (under its own id), not the original over-budget identities.
func TestWALReplayRestoresOverflowSeries(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	sw, err := wal.Create(dir, 0)
	require.NoError(t, err)

	src := engine.New(engine.Config{WAL: sw})

	overflowSeries := mkSeries("__name__", "ovf")
	limits := engine.AppendLimits{
		MaxSeries:     100,
		MaxSeriesSoft: 1,
		Overflow:      func(signal.Series) signal.Series { return overflowSeries },
	}

	ids, series, ts, vals := distinctSeries(3) // 1 normal + 2 overflowed
	_, err = src.AppendBatch(ids, ts, vals, nil, func(i int) signal.Series { return series[i] }, limits)
	require.NoError(t, err)
	require.NoError(t, sw.Close())

	restored := engine.New(engine.Config{})
	require.NoError(t, restored.Replay(dir))
	require.NoError(t, err)

	// The overflow series survives recovery with its redirected samples.
	got := fetchAll(t, restored, fetch.Request{
		Start: 0, End: 1 << 62, Matchers: []fetch.Matcher{eqMatcher("__name__", "ovf")},
	})
	require.Len(t, got, 1)
	assert.Len(t, got[0].Timestamps, 2, "two overflowed samples recovered under the overflow id")
	assert.Equal(t, 2, restored.SeriesCount(), "1 normal + 1 overflow series after replay")
}
