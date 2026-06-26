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

func TestApplyReplicatedOutOfOrder(t *testing.T) {
	t.Parallel()

	e := engine.New(engine.Config{OOOWindow: 50})

	s := mkSeries("job", "api")
	id := s.Hash()

	var buf bytes.Buffer
	w := wal.NewWriter(&buf)
	require.NoError(t, w.WriteSeries(id, s))
	require.NoError(t, w.WriteSamples(id, []int64{100, 200}, []float64{1, 2})) // newest ⇒ 200
	require.NoError(t, w.WriteSamples(id, []int64{120}, []float64{3}))         // 120 < 200-50 ⇒ OOO

	rejected, err := e.ApplyReplicated(buf.Bytes())
	require.NoError(t, err)
	assert.Equal(t, 1, rejected, "the out-of-order sample is rejected, like a local ingest")
	assert.Equal(t, 1, e.SeriesCount())

	got := fetchAll(t, e, fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("job", "api")}})
	require.Len(t, got, 1)
	assert.Equal(t, []int64{100, 200}, got[0].Timestamps, "only the in-order samples landed")
}
