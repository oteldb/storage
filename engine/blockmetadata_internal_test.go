package engine

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/block"
)

func TestMetadataLoadSharesUncachedResult(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		const callers = 32
		c := newBlockCache(64)
		key := blockKey{prefix: "uncached", col: colTsID, blk: -1}
		ready := make(chan struct{})
		finish := make(chan struct{})
		results := make(chan *blockEntry, callers)
		var loads atomic.Int64
		var wg sync.WaitGroup
		for range callers {
			wg.Go(func() {
				entry, err := c.metadata(t.Context(), key, func() (*blockEntry, error) {
					loads.Add(1)
					<-ready
					return c.insert(&blockEntry{key: key, decoder: &block.Decoder{}, bytes: 128}), nil
				})
				if err != nil {
					t.Error(err)
				}
				results <- entry
				<-finish
				c.release(entry)
			})
		}
		synctest.Wait()
		require.EqualValues(t, 1, loads.Load())
		require.Equal(t, callers, c.shard(key).loads[key].callers)
		close(ready)
		synctest.Wait()
		first := <-results
		require.NotNil(t, first.decoder)
		require.True(t, first.evicted)
		require.Equal(t, callers, first.refs)
		for range callers - 1 {
			require.Same(t, first, <-results)
		}
		require.Zero(t, c.bytes.Load())
		close(finish)
		wg.Wait()
		require.Nil(t, first.decoder)
		require.Zero(t, first.refs)
	})
}

func TestMetadataLoadCanceledWaiter(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		c := newBlockCache(64)
		key := blockKey{prefix: "canceled-waiter", col: colTsID, blk: -1}
		ready := make(chan struct{})
		leader := make(chan *blockEntry, 1)
		go func() {
			entry, err := c.metadata(t.Context(), key, func() (*blockEntry, error) {
				<-ready
				return c.insert(&blockEntry{key: key, decoder: &block.Decoder{}, bytes: 32}), nil
			})
			if err != nil {
				t.Error(err)
			}
			leader <- entry
		}()
		synctest.Wait()
		ctx, cancel := context.WithCancel(t.Context())
		waiter := make(chan error, 1)
		go func() {
			_, err := c.metadata(ctx, key, func() (*blockEntry, error) {
				t.Error("waiter unexpectedly loaded metadata")
				return nil, errors.New("unexpected metadata load")
			})
			waiter <- err
		}()
		synctest.Wait()
		cancel()
		require.ErrorIs(t, <-waiter, context.Canceled)
		require.Equal(t, 1, c.shard(key).loads[key].callers)
		close(ready)
		entry := <-leader
		require.Equal(t, 1, entry.refs)
		c.release(entry)
		c.evictPrefix(key.prefix)
		require.Nil(t, entry.decoder)
	})
}

func TestMetadataLoadCanceledLeaderRetries(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		c := newBlockCache(64)
		key := blockKey{prefix: "canceled-leader", col: colTsID, blk: -1}
		ctx, cancel := context.WithCancel(t.Context())
		leader := make(chan error, 1)
		go func() {
			_, err := c.metadata(ctx, key, func() (*blockEntry, error) {
				<-ctx.Done()
				return nil, errors.Wrap(ctx.Err(), "read metadata")
			})
			leader <- err
		}()
		synctest.Wait()
		waiter := make(chan *blockEntry, 1)
		go func() {
			entry, err := c.metadata(t.Context(), key, func() (*blockEntry, error) {
				return c.insert(&blockEntry{key: key, decoder: &block.Decoder{}, bytes: 32}), nil
			})
			if err != nil {
				t.Error(err)
			}
			waiter <- entry
		}()
		synctest.Wait()
		cancel()
		require.ErrorIs(t, <-leader, context.Canceled)
		entry := <-waiter
		require.NotNil(t, entry.decoder)
		c.release(entry)
		require.Empty(t, c.shard(key).loads)
	})
}

func TestMetadataLoadFailureRetries(t *testing.T) {
	t.Parallel()
	c := newBlockCache(64)
	key := blockKey{prefix: "failed", col: colTsID, blk: -1}
	failure := errors.New("invalid metadata")
	_, err := c.metadata(t.Context(), key, func() (*blockEntry, error) { return nil, failure })
	require.ErrorIs(t, err, failure)
	entry, err := c.metadata(t.Context(), key, func() (*blockEntry, error) {
		return c.insert(&blockEntry{key: key, decoder: &block.Decoder{}, bytes: 32}), nil
	})
	require.NoError(t, err)
	require.NotNil(t, entry.decoder)
	c.release(entry)
}

func TestBlockCacheProtectsMetadataFromSampleScan(t *testing.T) {
	t.Parallel()
	c := newBlockCacheWithShards(128, 8)
	key := blockKey{prefix: "protected", col: colTsID, blk: -1}
	metadata := c.insert(&blockEntry{key: key, decoder: &block.Decoder{}, bytes: 64})
	c.release(metadata)
	for i := range 100 {
		entry := c.insert(&blockEntry{key: blockKey{prefix: "sample", col: colTsID, blk: i}, bytes: 32})
		c.release(entry)
	}
	got, ok := c.get(key)
	require.True(t, ok)
	require.Same(t, metadata, got)
	c.release(got)
	require.LessOrEqual(t, c.bytes.Load(), int64(128))
	c.evictPrefix(key.prefix)
	require.Zero(t, c.metadataBytes.Load())
}

func TestBlockCacheMetadataBorrowsWithoutStarvingSamples(t *testing.T) {
	t.Parallel()
	for _, bytes := range []int64{64, 96, 128} {
		c := newBlockCacheWithShards(128, 8)
		metadata := c.insert(&blockEntry{key: blockKey{prefix: "metadata", col: colTsID, blk: -1}, decoder: &block.Decoder{}, bytes: 96})
		c.release(metadata)
		sample := c.insert(&blockEntry{key: blockKey{prefix: "sample", col: colTsID, blk: 0}, bytes: bytes})
		require.False(t, sample.evicted)
		require.Zero(t, c.metadataBytes.Load())
		c.release(sample)
		require.LessOrEqual(t, c.bytes.Load(), int64(128))
	}
}

func TestBlockCachePinnedMetadataSurvivesEviction(t *testing.T) {
	t.Parallel()
	c := newBlockCache(128)
	metadata := c.insert(&blockEntry{key: blockKey{prefix: "metadata", col: colTsID, blk: -1}, decoder: &block.Decoder{}, bytes: 96})
	sample := c.insert(&blockEntry{key: blockKey{prefix: "sample", col: colTsID}, bytes: 96})
	require.True(t, metadata.evicted)
	require.NotNil(t, metadata.decoder)
	c.release(metadata)
	require.Nil(t, metadata.decoder)
	c.release(sample)
}

func TestBlockCacheReclaimsMetadataExcessAcrossShards(t *testing.T) {
	t.Parallel()
	c := newBlockCacheWithShards(128, 8)
	metadataKey := blockKey{prefix: "metadata", col: colTsID, blk: -1}
	sampleKey := blockKey{col: colTsID}
	for i := 0; c.shard(sampleKey) == c.shard(metadataKey); i++ {
		sampleKey.prefix = strconv.Itoa(i)
	}
	metadata := c.insert(&blockEntry{key: metadataKey, decoder: &block.Decoder{}, bytes: 96})
	c.release(metadata)
	sample := c.insert(&blockEntry{key: sampleKey, bytes: 32})
	for i := range c.shards {
		if &c.shards[i] == sample.owner {
			c.trimNext = i
		}
	}
	newSample := c.insert(&blockEntry{key: blockKey{prefix: "new-sample", col: colTsID}, bytes: 32})
	require.True(t, metadata.evicted)
	require.False(t, sample.evicted, "directory excess is reclaimed before any sample shard")
	c.release(sample)
	c.release(newSample)
}

func TestMetadataLoadHandoffSurvivesConcurrentPrefixEviction(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		c := newBlockCache(64)
		key := blockKey{prefix: "evicted-flight", col: colTsID, blk: -1}
		ready := make(chan struct{})
		publish := make(chan struct{})
		inserted := make(chan *blockEntry, 1)
		leaderDone := make(chan struct{})
		go func() {
			entry, err := c.metadata(t.Context(), key, func() (*blockEntry, error) {
				<-ready
				entry := c.insert(&blockEntry{key: key, decoder: &block.Decoder{}, bytes: 32})
				inserted <- entry
				<-publish
				return entry, nil
			})
			if err != nil {
				t.Error(err)
			}
			c.release(entry)
			close(leaderDone)
		}()
		synctest.Wait()
		waiter := make(chan *blockEntry, 1)
		go func() {
			entry, err := c.metadata(t.Context(), key, func() (*blockEntry, error) {
				return nil, errors.New("unexpected duplicate load")
			})
			if err != nil {
				t.Error(err)
			}
			waiter <- entry
		}()
		synctest.Wait()
		require.Equal(t, 2, c.shard(key).loads[key].callers)
		close(ready)
		synctest.Wait()
		entry := <-inserted
		c.evictPrefix(key.prefix)
		require.True(t, entry.evicted)
		require.NotNil(t, entry.decoder)
		close(publish)
		<-leaderDone
		require.Same(t, entry, <-waiter)
		require.Equal(t, 1, entry.refs)
		require.NotNil(t, entry.decoder, "waiter owns its pin before the leader releases")
		c.release(entry)
		require.Nil(t, entry.decoder)
	})
}
