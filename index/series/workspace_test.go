package series

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

func TestSelectedEncodingBudget(t *testing.T) {
	t.Parallel()
	index := New()
	makeSeries := func(value string) signal.Series {
		return signal.Series{Resource: signal.Resource{SchemaURL: []byte("shared-schema")},
			Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("key"), Value: signal.StringValue([]byte(value))})}
	}
	index.Add(makeSeries("small"))
	index.Add(makeSeries(strings.Repeat("large", 1<<16)))
	view := index.View()
	var budget EncodingBudget
	budget.Add(view, 0)
	first := budget.Bytes()
	require.Positive(t, first)
	assert.Less(t, first, int64(4096), "unselected payload must not enter the workspace")
	assert.Greater(t, view.EncodingWorkspace(), int64(1<<20))
	budget.Add(view, 0)
	assert.Equal(t, first, budget.Bytes(), "shared symbols and context are charged once")
	budget.Add(view, 1)
	assert.Greater(t, budget.Bytes(), int64(1<<20))
	assert.GreaterOrEqual(t, view.IdentityWorkspace(0)+view.IdentityWorkspace(1), budget.Bytes())
	budget.Reset()
	assert.Less(t, budget.Bytes(), int64(4096), "old payload charges expire while estimator capacity remains charged")
	budget.Add(view, 0)
	assert.Less(t, budget.Bytes(), int64(4096))
}

func TestSelectedEncodingBudgetSharedContext(t *testing.T) {
	t.Parallel()
	index := New()
	resource := signal.Resource{Attributes: signal.NewAttributes(signal.KeyValue{
		Key: []byte("shared"), Value: signal.StringValue([]byte(strings.Repeat("x", 1<<16))),
	})}
	for _, value := range []int64{1, 2} {
		index.Add(signal.Series{Resource: resource, Attributes: signal.NewAttributes(signal.KeyValue{
			Key: []byte("id"), Value: signal.IntValue(value),
		})})
	}
	view := index.View()
	var budget EncodingBudget
	budget.Add(view, 0)
	first := budget.Bytes()
	budget.Add(view, 1)
	assert.Greater(t, first, int64(1<<16))
	assert.Less(t, budget.Bytes()-first, int64(1024), "adding a series must not repeat its context payload")
}
