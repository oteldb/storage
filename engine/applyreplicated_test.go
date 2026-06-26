package engine_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/wal"
)

func walOf(t *testing.T, fn func(w *wal.Writer)) []byte {
	t.Helper()
	var buf bytes.Buffer
	fn(wal.NewWriter(&buf))

	return buf.Bytes()
}

// TestApplyPrimaryRejectsOutOfOrder: the primary OOO-checks, and the accepted payload it returns
// excludes rejected samples, so secondaries (which apply verbatim) converge on the same data.
func TestApplyPrimaryRejectsOutOfOrder(t *testing.T) {
	t.Parallel()

	s := mkSeries("job", "api")
	id := s.Hash()
	data := walOf(t, func(w *wal.Writer) {
		require.NoError(t, w.WriteSeries(id, s))
		require.NoError(t, w.WriteSamples(id, []int64{100, 200}, []float64{1, 2})) // newest => 200
		require.NoError(t, w.WriteSamples(id, []int64{120}, []float64{3}))         // 120 < 200-50 => OOO
	})

	primary := engine.New(engine.Config{OOOWindow: 50})
	accepted, rejected, err := primary.ApplyPrimary(data)
	require.NoError(t, err)
	assert.Equal(t, 1, rejected, "the out-of-order sample is rejected once, at the primary")

	want := []int64{100, 200}
	got := fetchAll(t, primary, fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("job", "api")}})
	require.Len(t, got, 1)
	assert.Equal(t, want, got[0].Timestamps, "primary kept only the in-order samples")

	// A secondary applies the accepted payload verbatim (no OOO) and converges on the same data.
	secondary := engine.New(engine.Config{OOOWindow: 50})
	require.NoError(t, secondary.ApplyReplicated(accepted))
	got = fetchAll(t, secondary, fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("job", "api")}})
	require.Len(t, got, 1)
	assert.Equal(t, want, got[0].Timestamps, "secondary holds the primary's accepted set")
}
