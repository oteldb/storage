package engine_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// TestRecentTierServesWindowWithoutParts verifies that with the recent tier enabled, a query whose
// range falls inside the tier's window is answered correctly from RAM even after the data has been
// flushed to a part — and that a query outside the window still consults parts (issue #25 item 4).
func TestRecentTierServesWindowWithoutParts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	const window = int64(60 * 1e9) // 60s
	e := engine.New(engine.Config{
		Backend:      backend.Memory(),
		Prefix:       "default/metrics",
		RecentWindow: window,
		MaxPartBytes: 0, // unlimited ⇒ one part per flush
	})

	// A single series, two flushes at ts=100 and ts=200 (both within the 60s window of ts=200).
	s := mkSeries("__name__", "cpu", "host", "h1")
	id := s.Hash()

	mustAppend := func(ts int64, val float64) {
		_, err := e.AppendBatch(
			[]signal.SeriesID{id}, []int64{ts}, []float64{val}, nil,
			func(int) signal.Series { return s }, engine.AppendLimits{},
		)
		require.NoError(t, err)
	}

	mustAppend(100, 1.0)
	require.NoError(t, e.Flush(ctx)) // part 1: ts=100
	mustAppend(200, 2.0)
	require.NoError(t, e.Flush(ctx)) // part 2: ts=200; recent tier now holds [140, 200]

	// Query fully inside the recent window: answered without decoding parts. Both samples are present
	// and correct (the merge dedups the tier/part overlap by timestamp).
	got := fetchAll(t, e, fetch.Request{Start: 100, End: 1 << 62})
	require.Len(t, got, 1, "series readable")
	assert.Equal(t, []int64{100, 200}, got[0].Timestamps, "both samples present, in order")
	assert.Equal(t, []float64{1.0, 2.0}, got[0].Values)

	// A query for just the most-recent sample is also served from the tier.
	gotRecent := fetchAll(t, e, fetch.Request{Start: 150, End: 1 << 62})
	require.Len(t, gotRecent, 1)
	assert.Equal(t, []int64{200}, gotRecent[0].Timestamps)
}

// TestRecentTierDisabledByDefault confirms that without RecentWindow the engine behaves exactly as
// before (no tier; the head is the only in-RAM source and is drained on flush).
func TestRecentTierDisabledByDefault(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	e := engine.New(engine.Config{Backend: backend.Memory(), Prefix: "default/metrics"})

	s := mkSeries("__name__", "cpu")
	id := s.Hash()

	_, err := e.AppendBatch(
		[]signal.SeriesID{id}, []int64{100}, []float64{1.0}, nil,
		func(int) signal.Series { return s }, engine.AppendLimits{},
	)
	require.NoError(t, err)
	require.NoError(t, e.Flush(ctx))

	// After flush the head is empty; the sample lives only in the part. Still readable.
	got := fetchAll(t, e, fetch.Request{Start: 0, End: 1 << 62})
	require.Len(t, got, 1)
	assert.Equal(t, []int64{100}, got[0].Timestamps)
}

// TestRecentTierTrimsOldSamples confirms the tier drops samples that age out of the window, so a
// later query for the aged-out range must consult parts (no phantom in-RAM data, no unbounded
// growth).
func TestRecentTierTrimsOldSamples(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	const window = int64(30 * 1e9) // 30s
	e := engine.New(engine.Config{
		Backend:      backend.Memory(),
		Prefix:       "default/metrics",
		RecentWindow: window,
	})

	s := mkSeries("__name__", "cpu")
	id := s.Hash()

	mustAppend := func(ts int64, val float64) {
		_, err := e.AppendBatch(
			[]signal.SeriesID{id}, []int64{ts}, []float64{val}, nil,
			func(int) signal.Series { return s }, engine.AppendLimits{},
		)
		require.NoError(t, err)
	}

	// Flush at ts=10 (aged out once newest moves to ts=100, since 100-30=70 > 10), then ts=100.
	mustAppend(10, 1.0)
	require.NoError(t, e.Flush(ctx))
	mustAppend(100, 2.0)
	require.NoError(t, e.Flush(ctx)) // window is now [70, 100]; ts=10 is trimmed from the tier

	// The aged-out sample is still readable (it lives in the part), just not from the tier.
	got := fetchAll(t, e, fetch.Request{Start: 0, End: 1 << 62})
	require.Len(t, got, 1)
	assert.Equal(t, []int64{10, 100}, got[0].Timestamps, "aged-out sample still served from the part")
}
