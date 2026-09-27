package profile

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func countingLoad(loads *atomic.Int64) func(context.Context) (map[string][]byte, error) {
	return func(context.Context) (map[string][]byte, error) {
		loads.Add(1)

		return map[string][]byte{"stacks": encodeTable(goldenEntry(), storageCompressor)}, nil
	}
}

func TestSymbolCache(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	c := NewSymbolCache(1 << 20)

	var loads atomic.Int64

	first, err := c.Get(ctx, "p/1", countingLoad(&loads))
	require.NoError(t, err)
	requireSameTable(t, goldenEntry(), first.t.t[tableStacks])

	second, err := c.Get(ctx, "p/1", countingLoad(&loads))
	require.NoError(t, err)
	assert.Same(t, first, second)
	assert.Equal(t, int64(1), loads.Load())

	_, err = c.Get(ctx, "p/2", countingLoad(&loads))
	require.NoError(t, err)
	assert.Equal(t, int64(2), loads.Load(), "keyed by object")

	st := c.Stats()
	assert.Equal(t, int64(1), st.Hits)
	assert.Equal(t, int64(2), st.Misses)
	assert.Equal(t, 2, st.Items)
	assert.Equal(t, 2*first.Size(), st.Bytes)
}

func TestSymbolCacheFailedLoadNotCached(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	c := NewSymbolCache(1 << 20)
	boom := errors.New("boom")

	_, err := c.Get(ctx, "p", func(context.Context) (map[string][]byte, error) { return nil, boom })
	require.ErrorIs(t, err, boom)

	_, err = c.Get(ctx, "p", func(context.Context) (map[string][]byte, error) {
		return map[string][]byte{"stacks": []byte("garbage")}, nil
	})
	require.ErrorIs(t, err, ErrCorruptSymbols)

	var loads atomic.Int64

	_, err = c.Get(ctx, "p", countingLoad(&loads))
	require.NoError(t, err)
	assert.Equal(t, int64(1), loads.Load())
}

func TestSymbolCacheOversized(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	c := NewSymbolCache(1)

	var loads atomic.Int64

	for range 2 {
		got, err := c.Get(ctx, "p", countingLoad(&loads))
		require.NoError(t, err)
		requireSameTable(t, goldenEntry(), got.t.t[tableStacks])
	}

	assert.Equal(t, int64(2), loads.Load(), "an entry above the budget is not kept")
	assert.Zero(t, c.Stats().Items)
}

func TestSymbolCacheDisabled(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	var c *SymbolCache
	require.Nil(t, NewSymbolCache(0))

	var loads atomic.Int64

	for range 2 {
		_, err := c.Get(ctx, "p", countingLoad(&loads))
		require.NoError(t, err)
	}

	assert.Equal(t, int64(2), loads.Load())
	assert.Equal(t, CacheStats{}, c.Stats())
}

func TestSymbolCacheConcurrent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	c := NewSymbolCache(1 << 20)

	var (
		loads atomic.Int64
		wg    sync.WaitGroup
	)

	got := make([]*Tables, 8)
	errs := make([]error, len(got))

	for i := range got {
		wg.Go(func() { got[i], errs[i] = c.Get(ctx, "p", countingLoad(&loads)) })
	}

	wg.Wait()

	for i := range got {
		require.NoError(t, errs[i])
		assert.Len(t, got[i].t.t[tableStacks], 1)
	}

	assert.GreaterOrEqual(t, loads.Load(), int64(1))
}
