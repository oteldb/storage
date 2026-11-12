package seriestable

import (
	"bytes"
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
	"github.com/oteldb/storage/signal"
)

func cachedRun(t *testing.T, be backend.Backend, prefix string, cache *IDCache) *Run {
	t.Helper()
	var builder Builder
	data := descriptors(16)
	for i := range data {
		s := &data[i]
		require.NoError(t, builder.Add(s.Hash(), *s))
	}
	r, err := WriteWithIDCache(t.Context(), be, prefix, &builder, cache)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, r.Close()) })
	return r
}

func TestIDCacheLookup(t *testing.T) {
	t.Parallel()
	be := backendtest.NewByteCounter(backend.Memory())
	cache := NewIDCache(1 << 20)
	r := cachedRun(t, be, "retained", cache)
	cache.publish(r)
	require.Nil(t, cache.reserve(r))
	reader, err := r.NewReader(t.Context())
	require.NoError(t, err)
	defer func() { require.NoError(t, reader.Close()) }()
	data := descriptors(16)
	for _, s := range data {
		raw, found, err := reader.LookupRaw(s.Hash())
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, s.AppendHashInput(nil), raw)
	}
	negative, err := r.NewReader(t.Context())
	require.NoError(t, err)
	defer func() { require.NoError(t, negative.Close()) }()
	be.Reset()
	for _, id := range []signal.SeriesID{{}, {Hi: ^uint64(0), Lo: ^uint64(0)}} {
		_, found, err := negative.LookupRaw(id)
		require.NoError(t, err)
		require.False(t, found)
	}
	require.Zero(t, be.Reads(), "retained ID arrays prove absence without disk reads")
	require.Nil(t, negative.raw, "negative probes allocate no descriptor cursor")
	require.LessOrEqual(t, cache.SizeBytes(), int64(1<<20))
}

func TestIDCacheEvictionAndClose(t *testing.T) {
	t.Parallel()
	be := backend.Memory()
	probe := NewIDCache(1 << 20)
	first := cachedRun(t, be, "probe", probe)
	unit := probe.SizeBytes()
	require.NoError(t, first.Close())
	base := probe.SizeBytes()
	cache := NewIDCache(base + 2*(unit-base))
	a := cachedRun(t, be, "a", cache)
	b := cachedRun(t, be, "b", cache)
	retained := retainedForTest(cache, a)
	require.True(t, retained)
	c := cachedRun(t, be, "c", cache)
	retained = retainedForTest(cache, b)
	require.False(t, retained, "least recently used run is evicted")
	reader, err := b.NewReader(t.Context())
	require.NoError(t, err)
	raw, found, err := reader.LookupRaw(descriptors(16)[0].Hash())
	require.NoError(t, err)
	require.True(t, found)
	require.NotEmpty(t, raw)
	require.NoError(t, reader.Close())
	active, err := a.NewReader(t.Context())
	require.NoError(t, err)
	require.NoError(t, a.Close())
	retained = retainedForTest(cache, a)
	require.True(t, retained, "active reader pins the run")
	require.NoError(t, active.Close())
	retained = retainedForTest(cache, a)
	require.False(t, retained)
	require.NoError(t, c.Close())
	require.Equal(t, base, cache.SizeBytes())
}

func TestIDCacheDisabledAndPending(t *testing.T) {
	t.Parallel()
	for _, limit := range []int64{-1, 0, 1} {
		require.Nil(t, NewIDCache(limit))
	}
	be := backend.Memory()
	noCache := cachedRun(t, be, "no-cache", nil)
	tiny := NewIDCache(256)
	oversized, err := OpenWithIDCache(t.Context(), be, "no-cache", tiny, noCache.Digest())
	require.NoError(t, err)
	defer func() { require.NoError(t, oversized.Close()) }()
	require.Empty(t, tiny.runs)
	cache := NewIDCache(1 << 20)
	slot := cache.reserve(noCache)
	require.NotNil(t, slot)
	retained := retainedForTest(cache, noCache)
	require.False(t, retained, "unvalidated arrays are invisible")
	raw, err := be.Read(t.Context(), "no-cache/c/0")
	require.NoError(t, err)
	corrupt := bytes.Clone(raw)
	corrupt[len(corrupt)-1] ^= 1
	require.NoError(t, be.Write(t.Context(), "no-cache/c/0", corrupt))
	before := cache.SizeBytes()
	_, err = OpenWithIDCache(t.Context(), be, "no-cache", cache)
	require.Error(t, err)
	require.Len(t, cache.runs, 1)
	require.Less(t, cache.SizeBytes()-before, slot.charge, "failed validation releases array storage; map capacity remains accounted")
	cache.remove(noCache)
}

func TestIDCacheConcurrentEviction(t *testing.T) {
	t.Parallel()
	cache := NewIDCache(640)
	be := backend.Memory()
	r := cachedRun(t, be, "race", cache)
	var workers sync.WaitGroup
	for range 4 {
		workers.Go(func() {
			reader, err := r.NewReader(context.Background())
			if err != nil {
				t.Error(err)
				return
			}
			defer func() {
				if err := reader.Close(); err != nil {
					t.Error(err)
				}
			}()
			for range 50 {
				_, found, err := reader.LookupRaw(descriptors(16)[0].Hash())
				if err != nil || !found {
					t.Errorf("lookup: %v %v", found, err)
				}
			}
		})
	}
	for range 20 {
		other := &Run{index: r.index}
		slot := cache.reserve(other)
		if slot != nil {
			for i, s := range descriptors(16) {
				slot.ids[i] = s.Hash()
			}
			cache.publish(other)
			cache.remove(other)
		}
	}
	workers.Wait()
	require.LessOrEqual(t, cache.SizeBytes(), int64(640))
}

func retainedForTest(cache *IDCache, run *Run) bool {
	entry := cache.acquire(run)
	cache.release(entry)
	return entry != nil
}

func TestIDCachePinnedBudget(t *testing.T) {
	t.Parallel()
	cache := NewIDCache(640)
	run := cachedRun(t, backend.Memory(), "pinned", cache)
	reader, err := run.NewReader(t.Context())
	require.NoError(t, err)
	other := &Run{index: run.index}
	require.Nil(t, cache.reserve(other), "pending allocation cannot evict or outlive accounting for pinned arrays")
	require.LessOrEqual(t, cache.SizeBytes(), int64(640))
	require.NoError(t, reader.Close())
	require.NotNil(t, cache.reserve(other))
	cache.remove(other)
}
