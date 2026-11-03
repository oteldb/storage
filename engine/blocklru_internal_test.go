package engine

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBlockCachePrefixEvictionKeepsOtherParts(t *testing.T) {
	t.Parallel()
	for _, shards := range []int{1, 8} {
		t.Run(strconv.Itoa(shards), func(t *testing.T) {
			t.Parallel()
			c := newBlockCacheWithShards(1<<20, shards)
			for _, prefix := range []string{"a", "b"} {
				for i := range 64 {
					e := c.insert(&blockEntry{key: blockKey{prefix: prefix, col: colTsID, blk: i}, i64: []int64{int64(i)}, bytes: 8})
					c.release(e)
				}
			}
			pinned, ok := c.get(blockKey{prefix: "a", col: colTsID, blk: 17})
			require.True(t, ok)
			c.evictPrefix("a")
			require.Equal(t, []int64{17}, pinned.i64)
			require.Equal(t, 64, c.stats().Items)
			require.EqualValues(t, 512, c.stats().Bytes)
			for i := range 64 {
				_, ok := c.get(blockKey{prefix: "a", col: colTsID, blk: i})
				require.False(t, ok)
				e, ok := c.get(blockKey{prefix: "b", col: colTsID, blk: i})
				require.True(t, ok)
				require.Equal(t, int64(i), e.i64[0])
				c.release(e)
			}
			c.release(pinned)
			require.Nil(t, pinned.i64)
			c.evictPrefix("b")
			require.Zero(t, c.stats().Items)
			require.Zero(t, c.stats().Bytes)
			for i := range c.shards {
				s := &c.shards[i]
				require.Empty(t, s.items)
				require.Nil(t, s.ll.head)
				require.Nil(t, s.ll.tail)
			}
		})
	}
}
