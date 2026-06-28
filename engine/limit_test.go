package engine_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/query/fetch"
)

// TestMetricFetchIgnoresLimit documents that the metric engine ignores fetch Limit/Reverse: a PromQL
// range read needs every sample, so the limit pushdown is a record-signal-only optimization.
func TestMetricFetchIgnoresLimit(t *testing.T) {
	t.Parallel()

	e := flushEngine()
	s := mkSeries("job", "api")
	mustAppend(t, e, s, 100, 1.0)
	mustAppend(t, e, s, 200, 2.0)
	mustAppend(t, e, s, 300, 3.0)

	got := fetchAll(t, e, fetch.Request{Start: 0, End: 1000, Limit: 1, Reverse: true})
	require.Len(t, got, 1)
	assert.Equal(t, []int64{100, 200, 300}, got[0].Timestamps, "every sample returned despite Limit:1")
}
