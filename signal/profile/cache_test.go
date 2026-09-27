package profile

import (
	"context"
	"fmt"
	"math"
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

// TestSymbolCacheHugeEntries checks a budget above what otter's uint32 weights express still holds no
// entry whose true size its weight cannot carry: two 6 GiB tables must not both sit under 8 GiB.
func TestSymbolCacheHugeEntries(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	c := NewSymbolCache(8 << 30)
	require.Equal(t, int64(math.MaxUint32-1), c.maxBytes)

	var loads atomic.Int64

	huge := func(context.Context) (*Tables, error) {
		loads.Add(1)

		return &Tables{t: newSymTables(), size: 6 << 30}, nil
	}

	for _, key := range []string{"a", "b", "a", "b"} {
		got, err := c.get(ctx, key, huge)
		require.NoError(t, err)
		assert.Equal(t, int64(6<<30), got.Size(), "served uncached")
	}

	assert.Equal(t, int64(4), loads.Load(), "never kept")

	st := c.Stats()
	assert.Zero(t, st.Items)
	assert.Zero(t, st.Bytes)

	small := func(context.Context) (*Tables, error) {
		return &Tables{t: newSymTables(), size: math.MaxUint32 - 1}, nil
	}

	_, err := c.get(ctx, "fits", small)
	require.NoError(t, err)
	assert.Equal(t, int64(math.MaxUint32-1), c.Stats().Bytes, "an entry that fits is weighed exactly")
}

// TestSymbolCacheChargesRetainedCapacity checks the cache charges the backing arrays decoded entries
// keep alive, not just their length: zstd decompression reserves a slack far larger than a small
// table, and many cached parts must not hold it beyond the budget.
func TestSymbolCacheChargesRetainedCapacity(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	stored := make(map[string][]byte, len(tableNames))
	for _, name := range tableNames {
		stored[name] = encodeTable(fixtureTable(), storageCompressor)
	}

	raw, err := tableBodyOf(stored["stacks"])
	require.NoError(t, err)
	require.Greater(t, cap(raw)-len(raw), len(raw), "decompression over-reserves a small table")

	const (
		parts  = 200
		budget = 64 << 20
	)

	c := NewSymbolCache(budget)

	var charged int64

	for i := range parts {
		got, err := c.Get(ctx, fmt.Sprintf("p/%d", i), func(context.Context) (map[string][]byte, error) {
			return stored, nil
		})
		require.NoError(t, err)

		var retained, entries int64

		for ti, body := range got.bodies {
			retained += int64(cap(body))
			entries += int64(len(got.t.t[ti]))
			assert.Equal(t, len(body), cap(body), "no decompression slack retained")
		}

		assert.Equal(t, retained+entries*entryOverhead, got.Size(), "charged by retained capacity")

		charged += got.Size()
	}

	st := c.Stats()
	assert.Equal(t, parts, st.Items)
	assert.Equal(t, charged, st.Bytes)
	assert.LessOrEqual(t, st.Bytes, int64(budget))
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
