package engine_test

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
)

// metricsEngine builds an engine on a fresh memory backend and returns both, so a test can inspect
// the on-disk objects directly.
func metricsEngine() (*engine.Engine, backend.Backend) {
	b := backend.Memory()

	return engine.New(engine.Config{Backend: b, Prefix: "default/metrics"}), b
}

// backendBytes sums every stored object under the metrics prefix — the on-disk footprint.
func backendBytes(t *testing.T, b backend.Backend) int {
	t.Helper()

	total := 0
	for _, k := range listKeys(t, b) {
		obj, err := b.Read(context.Background(), k)
		require.NoError(t, err)

		total += len(obj)
	}

	return total
}

// noisyValues are full-precision (high-entropy) floats so the lossless codec is large and a lossy
// precision tier has room to compress — the realistic case precision tiers target.
func noisyValues(n int) []float64 {
	out := make([]float64, n)
	for i := range out {
		out[i] = math.Sqrt(float64(i)+0.123456789) * 1000
	}

	return out
}

// TestMergeWithPrecisionLossyOldData drives the headline behavior: a fully-cold part is re-encoded
// lossily at merge (smaller, values within tolerance, not exact), and a second merge with the same
// policy is a fixed point — no churn — proving the precision budget recorded in the manifest stops
// repeated rewrites.
func TestMergeWithPrecisionLossyOldData(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	vals := noisyValues(200)

	// A lossless baseline: same data merged with no precision policy.
	base, baseB := metricsEngine()
	bs := mkSeries("job", "api")
	for i, v := range vals {
		mustAppend(t, base, bs, int64(10+i), v)
	}
	require.NoError(t, base.Flush(ctx))
	require.NoError(t, base.MergeWith(ctx, engine.MergeOptions{}))
	losslessBytes := backendBytes(t, baseB)

	// The lossy engine: identical data, with a precision tier covering all of it (Before past the
	// newest sample) at a low bit budget.
	e, b := metricsEngine()
	s := mkSeries("job", "api")
	for i, v := range vals {
		mustAppend(t, e, s, int64(10+i), v)
	}
	require.NoError(t, e.Flush(ctx))

	opts := engine.MergeOptions{Precision: []engine.PrecisionTier{{Before: 1_000, Bits: 16}}}
	require.NoError(t, e.MergeWith(ctx, opts))
	require.Equal(t, 1, e.PartCount())

	lossyBytes := backendBytes(t, b)
	assert.Less(t, lossyBytes, losslessBytes, "lossy old data must be smaller than lossless")

	got := fetchAll(t, e, fetch.Request{Start: 0, End: 1_000, Matchers: []fetch.Matcher{eqMatcher("job", "api")}})
	require.Len(t, got, 1)
	require.Len(t, got[0].Values, len(vals))

	changed := 0
	for i, v := range vals {
		assert.InEpsilonf(t, v, got[0].Values[i], 1e-2, "value[%d] within lossy tolerance", i)

		if got[0].Values[i] != v {
			changed++
		}
	}
	assert.Positive(t, changed, "lossy encoding must actually perturb some values")

	// Fixed point: the same policy must not rewrite the already-coarsened part.
	keysBefore := listKeys(t, b)
	require.NoError(t, e.MergeWith(ctx, opts))
	assert.Equal(t, 1, e.PartCount())
	assert.Equal(t, keysBefore, listKeys(t, b), "re-merge at the same precision is a no-op (no churn)")
}

// TestMergeWithPrecisionKeepsRecentLossless confirms precision is per-part by age: a part whose
// newest sample is younger than the tier stays fully lossless (bit-exact), even if it also holds
// old samples — only fully-cold parts are coarsened.
func TestMergeWithPrecisionKeepsRecentLossless(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	e, _ := metricsEngine()
	s := mkSeries("job", "api")

	vals := noisyValues(50)
	for i, v := range vals {
		mustAppend(t, e, s, int64(10+i), v) // old samples …
	}
	mustAppend(t, e, s, 5_000, 42.5) // … plus one recent sample, so the part is not fully cold
	require.NoError(t, e.Flush(ctx))

	opts := engine.MergeOptions{Precision: []engine.PrecisionTier{{Before: 1_000, Bits: 16}}}
	require.NoError(t, e.MergeWith(ctx, opts))

	got := fetchAll(t, e, fetch.Request{Start: 0, End: 10_000, Matchers: []fetch.Matcher{eqMatcher("job", "api")}})
	require.Len(t, got, 1)
	require.Len(t, got[0].Values, len(vals)+1)

	for i, v := range vals {
		assert.Equalf(t, v, got[0].Values[i], "value[%d] must stay bit-exact (part not fully cold)", i)
	}
}
