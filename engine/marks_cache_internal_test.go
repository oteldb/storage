package engine

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/block"
)

func TestCachedMarksAliasSurvivesEviction(t *testing.T) {
	t.Parallel()
	e := New(Config{DecodeCacheBytes: 1 << 20})
	p := &part{prefix: "marks"}
	want := []block.Granule{{FirstRow: 0, MinKey: 10, MaxKey: 20}}
	entry := e.blockCache.insert(&blockEntry{key: marksBlockKey(p), granules: want, bytes: 256 + 24})
	e.blockCache.release(entry)
	got := e.partGranuleTimes(t.Context(), p)
	require.Same(t, &want[0], &got[0])
	e.blockCache.evictPrefix(p.prefix)
	require.Nil(t, entry.granules)
	require.Equal(t, want, got, "eviction drops cache ownership without recycling immutable bounds")
}

func TestCachedMarksSurviveNumericChurn(t *testing.T) {
	t.Parallel()
	c := newBlockCache(1 << 20)
	key := blockKey{prefix: "marks", col: colMarksID, blk: -1}
	marks := c.insert(&blockEntry{key: key, granules: []block.Granule{{MinKey: 1, MaxKey: 2}}, bytes: 280})
	c.release(marks)
	for i := range 2048 {
		entry := c.insert(&blockEntry{key: blockKey{prefix: "data", col: colTsID, blk: i}, i64: make([]int64, 128), bytes: 1024})
		c.release(entry)
	}
	entry, ok := c.get(key)
	require.True(t, ok, "small immutable bounds survive a full numeric working-set replacement")
	c.release(entry)
	require.LessOrEqual(t, c.stats().Bytes, c.maxBytes)
}

func TestLargeDecoderCanDisplaceMarks(t *testing.T) {
	t.Parallel()
	c := newBlockCache(1 << 20)
	marksKey := blockKey{prefix: "marks", col: colMarksID, blk: -1}
	marks := c.insert(&blockEntry{key: marksKey, granules: []block.Granule{{MinKey: 1, MaxKey: 2}}, bytes: 280})
	c.release(marks)
	key := blockKey{prefix: "large", col: colTsID, blk: -1}
	large := c.insert(&blockEntry{key: key, bytes: c.maxBytes})
	c.release(large)
	entry, ok := c.get(key)
	require.True(t, ok, "the shared budget remains available to one large decoder entry")
	c.release(entry)
	_, ok = c.get(marksKey)
	require.False(t, ok)
	require.Equal(t, c.maxBytes, c.stats().Bytes)
	require.Zero(t, c.marksBytes.Load())
}
