package fetch_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

func TestSliceIteratorAndDrain(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	batches := []*fetch.Batch{
		{ID: signal.SeriesID{Lo: 1}, Timestamps: []int64{1, 2}, Values: []float64{1, 2}},
		{ID: signal.SeriesID{Lo: 2}, Timestamps: []int64{3}, Values: []float64{3}},
	}

	it := fetch.NewSliceIterator(batches)
	got, err := fetch.Drain(ctx, it)
	require.NoError(t, err)
	assert.Equal(t, batches, got)
	require.NoError(t, it.Close())

	// Exhausted iterator yields nothing more.
	empty, err := fetch.Drain(ctx, fetch.NewSliceIterator(nil))
	require.NoError(t, err)
	assert.Empty(t, empty)
}
