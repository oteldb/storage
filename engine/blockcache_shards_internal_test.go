package engine

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/block"
)

func TestBlockCacheShardsShareWholeBudget(t *testing.T) {
	t.Parallel()
	c := newBlockCacheWithShards(4096, 8)
	key := blockKey{prefix: "large-metadata", col: colTsID, blk: -1}
	metadata := c.insert(&blockEntry{key: key, decoder: &block.Decoder{}, bytes: 3000})
	c.release(metadata)
	got, ok := c.get(key)
	require.True(t, ok, "an entry larger than one eighth of the budget still fits")
	require.Same(t, metadata, got)
	c.release(got)
	require.Equal(t, int64(3000), c.stats().Bytes)
	c.evictPrefix(key.prefix)
	require.Zero(t, c.stats().Bytes)
}

func TestBlockCachePrefixEvictionPreservesPinsAcrossShards(t *testing.T) {
	t.Parallel()
	c := newBlockCacheWithShards(4096, 8)
	entries := make([]*blockEntry, 128)
	shards := make(map[*blockCacheShard]struct{})
	for i := range entries {
		key := blockKey{prefix: "shared-prefix", col: colTsID, blk: i}
		shards[c.shard(key)] = struct{}{}
		entries[i] = c.insert(&blockEntry{key: key, i64: []int64{int64(i)}, bytes: 8})
	}
	require.Greater(t, len(shards), 1)
	c.evictPrefix("shared-prefix")
	require.Zero(t, c.stats().Bytes)
	require.Zero(t, c.stats().Items)
	for i, entry := range entries {
		require.Equal(t, []int64{int64(i)}, entry.i64, "eviction cannot recycle a pinned block")
		c.release(entry)
	}
	require.LessOrEqual(t, len(c.i64Free.free), blockBufCap, "one pool bound covers all shards")
}

func TestBlockCacheConcurrentDuplicateReturnsCredits(t *testing.T) {
	t.Parallel()
	c := newBlockCacheWithShards(64, 8)
	key := blockKey{prefix: "duplicate", col: colTsID}
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range 64 {
		wg.Go(func() {
			<-start
			entry := c.insert(&blockEntry{key: key, i64: []int64{7}, bytes: 8})
			if len(entry.i64) != 1 || entry.i64[0] != 7 {
				t.Error("concurrent insertion returned recycled data")
			}
			c.release(entry)
		})
	}
	close(start)
	wg.Wait()
	require.Equal(t, int64(8), c.stats().Bytes)
	require.Equal(t, 1, c.stats().Items)
	c.evictPrefix(key.prefix)
	require.Zero(t, c.stats().Bytes)
}

func TestBlockCacheReplacementDoesNotRecycleOldLease(t *testing.T) {
	t.Parallel()
	c := newBlockCacheWithShards(8, 8)
	key := blockKey{prefix: "replacement", col: colTsID}
	old := c.insert(&blockEntry{key: key, i64: []int64{1}, bytes: 8})
	c.evictPrefix(key.prefix)
	current := c.insert(&blockEntry{key: key, i64: []int64{2}, bytes: 8})
	require.Equal(t, []int64{1}, old.i64)
	require.Equal(t, []int64{2}, current.i64)
	c.release(old)
	require.Nil(t, old.i64)
	require.Equal(t, []int64{2}, current.i64)
	require.Equal(t, int64(8), c.stats().Bytes)
	c.release(current)
	c.evictPrefix(key.prefix)
	require.Zero(t, c.stats().Bytes)
}

func TestBlockCachePendingCreditDoesNotDeadlockInsertion(t *testing.T) {
	t.Parallel()
	c := newBlockCacheWithShards(64, 8)
	require.True(t, c.reserve(64))
	entry := c.insert(&blockEntry{key: blockKey{prefix: "pending"}, i64: []int64{9}, bytes: 8})
	require.True(t, entry.evicted)
	require.Equal(t, []int64{9}, entry.i64)
	c.release(entry)
	require.Equal(t, int64(64), c.stats().Bytes)
	c.bytes.Add(-64)
	require.Zero(t, c.stats().Bytes)
}

func TestBlockCacheTrimmerRetainsFreedCredits(t *testing.T) {
	t.Parallel()
	c := newBlockCacheWithShards(16, 8)
	for i := range 2 {
		entry := c.insert(&blockEntry{key: blockKey{prefix: "credit", blk: i}, i64: []int64{1}, bytes: 8})
		c.release(entry)
	}
	c.trimMu.Lock()
	defer c.trimMu.Unlock()
	c.trimming.Store(true)
	defer c.trimming.Store(false)
	require.True(t, c.evictOldestShardEntry())
	require.False(t, c.tryReserveAvailable(8), "a new small insertion cannot steal credits from the large request")
	require.True(t, c.evictOldestShardEntry())
	require.True(t, c.tryReserve(16), "the trimmer can accumulate enough credits for a whole-budget entry")
	c.bytes.Add(-16)
	require.Zero(t, c.stats().Bytes)
}

func TestBlockCacheSharedBudgetUnderChurn(t *testing.T) {
	t.Parallel()
	c := newBlockCacheWithShards(256, 8)
	var wg sync.WaitGroup
	for worker := range 8 {
		wg.Go(func() {
			for i := range 500 {
				key := blockKey{prefix: fmt.Sprintf("p-%d", i%16), col: colTsID, blk: worker}
				entry, ok := c.get(key)
				if !ok {
					entry = c.insert(&blockEntry{key: key, i64: []int64{int64(worker)}, bytes: 8})
				}
				if entry.i64[0] != int64(worker) {
					t.Error("cache buffer recycled while pinned")
				}
				c.release(entry)
				if c.bytes.Load() > c.maxBytes {
					t.Error("shared cache budget exceeded")
				}
			}
		})
	}
	wg.Wait()
	var actual int64
	for i := range c.shards {
		for _, element := range c.shards[i].items {
			actual += element.Value.(*blockEntry).bytes
		}
	}
	require.Equal(t, actual, c.stats().Bytes)
	require.LessOrEqual(t, actual, c.maxBytes)
}
