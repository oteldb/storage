package readbudget_test

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/readbudget"
)

func TestBudgetReserveAndRelease(t *testing.T) {
	t.Parallel()

	b := readbudget.New(100)

	require.NoError(t, b.Reserve(60))
	assert.Equal(t, int64(40), b.Remaining())

	require.ErrorIs(t, b.Reserve(60), readbudget.ErrExceeded, "the second reservation does not fit")
	assert.Equal(t, int64(40), b.Remaining(), "a refused reservation charges nothing")

	b.Release(60)
	assert.Equal(t, int64(100), b.Remaining())
	require.NoError(t, b.Reserve(60), "the freed room is reusable")
}

// Unlike the metric decode budget, an over-budget request is refused rather than admitted alone:
// this bound exists precisely for the query that is too big on its own.
func TestBudgetRefusesOversizeRequest(t *testing.T) {
	t.Parallel()

	b := readbudget.New(100)
	require.ErrorIs(t, b.Reserve(500), readbudget.ErrExceeded)
	assert.Equal(t, int64(100), b.Remaining())
}

func TestBudgetNilIsUnbounded(t *testing.T) {
	t.Parallel()

	var b *readbudget.Budget

	require.NoError(t, b.Reserve(1<<40), "a nil budget bounds nothing")
	b.Release(1 << 40)
	assert.Zero(t, b.Remaining())
	assert.Zero(t, b.Peak())

	assert.Nil(t, readbudget.New(0), "a non-positive limit is unbounded")
	assert.Nil(t, readbudget.New(-1))
}

// A double release is a bug, but not one worth taking the process down for on a read path.
func TestBudgetReleaseClamps(t *testing.T) {
	t.Parallel()

	b := readbudget.New(100)
	require.NoError(t, b.Reserve(10))

	b.Release(10)
	b.Release(10)

	assert.Equal(t, int64(100), b.Remaining(), "the budget never exceeds its own limit")
}

func TestBudgetPeak(t *testing.T) {
	t.Parallel()

	b := readbudget.New(100)
	require.NoError(t, b.Reserve(80))
	b.Release(80)
	require.NoError(t, b.Reserve(10))

	assert.Equal(t, int64(80), b.Peak(), "peak remembers the high-water mark, not the current hold")
}

func TestBudgetContext(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	assert.Nil(t, readbudget.From(ctx), "an unbudgeted context reads as unbounded")

	b := readbudget.New(100)
	assert.Same(t, b, readbudget.From(readbudget.With(ctx, b)))

	assert.Nil(t, readbudget.From(readbudget.With(ctx, nil)),
		"installing no budget leaves the context alone")
}

func TestBudgetConcurrent(t *testing.T) {
	t.Parallel()

	const (
		workers = 8
		each    = 100
	)

	b := readbudget.New(workers * 10)

	var wg sync.WaitGroup

	for range workers {
		wg.Go(func() {
			for range each {
				if err := b.Reserve(10); err == nil {
					b.Release(10)
				}
			}
		})
	}

	wg.Wait()

	assert.Equal(t, int64(workers*10), b.Remaining(), "every reservation was balanced by its release")
}
