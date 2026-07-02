package engine

import (
	"sync"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// i64Backing returns the identity of a slice's backing array, so a test can assert a later draw
// reused a recycled buffer rather than allocating a fresh one.
func i64Backing(s []int64) unsafe.Pointer { return unsafe.Pointer(unsafe.SliceData(s)) }

// TestBlockCacheRecyclesEvictedBuffer verifies the core alloc-rate fix: a byte-budget eviction of an
// unpinned block returns its decoded slice to the freelist, and the next decode draws that same
// backing array instead of minting a fresh one.
func TestBlockCacheRecyclesEvictedBuffer(t *testing.T) {
	t.Parallel()

	c := newBlockCache(8) // one 1-row int64 block (8 bytes) fits; a second evicts the first

	bufA := c.getI64Buf(1)
	ptrA := i64Backing(bufA)
	bufA[0] = 42

	entA := c.insert(&blockEntry{key: blockKey{prefix: "p", col: colTsID, blk: 0}, i64: bufA, bytes: 8})
	require.Equal(t, bufA[0], entA.i64[0])
	c.release(entA) // fetch done reading A; A is unpinned but still resident

	// Inserting B evicts A (coldest). A is unpinned, so its buffer recycles immediately.
	bufB := c.getI64Buf(1)
	entB := c.insert(&blockEntry{key: blockKey{prefix: "p", col: colTsID, blk: 1}, i64: bufB, bytes: 8})
	c.release(entB)
	assert.Nil(t, entA.i64, "evicted, unpinned entry's buffer is handed back and cleared")

	// The next decode buffer is A's recycled backing array — no new allocation.
	got := c.getI64Buf(1)
	assert.Equal(t, ptrA, i64Backing(got), "eviction refilled the freelist; the draw reuses it")
}

// TestBlockCachePinnedBufferSurvivesEviction verifies the safety property that makes recycling sound:
// a block evicted by the byte budget while a fetch still holds a view is NOT recycled until that
// fetch releases it, so the reader never sees its buffer reused underneath.
func TestBlockCachePinnedBufferSurvivesEviction(t *testing.T) {
	t.Parallel()

	c := newBlockCache(8)

	bufA := c.getI64Buf(1)
	bufA[0] = 7
	entA := c.insert(&blockEntry{key: blockKey{prefix: "p", col: colValID, blk: 0}, i64: bufA, bytes: 8})
	// entA stays pinned (refs == 1): the fetch is still reading it.

	bufB := c.getI64Buf(1)
	entB := c.insert(&blockEntry{key: blockKey{prefix: "p", col: colValID, blk: 1}, i64: bufB, bytes: 8})
	c.release(entB)

	require.NotNil(t, entA.i64, "a pinned block keeps its buffer through eviction")
	assert.Equal(t, int64(7), entA.i64[0], "and the view still reads the right data")

	c.release(entA) // last reader done → now safe to recycle
	assert.Nil(t, entA.i64, "the buffer recycles once the last pin drops")
}

// TestBlockCacheRaceInsertGetEvict hammers the cache from many goroutines — concurrent decodes racing
// on the same keys, byte-budget eviction, and pinned reads — under -race, asserting the refcounting
// never recycles a buffer a reader is holding (which -race would flag as a read of reused memory).
func TestBlockCacheRaceInsertGetEvict(t *testing.T) {
	t.Parallel()

	c := newBlockCache(16 * 8) // room for ~16 one-row blocks; a broad key space forces constant eviction

	const workers, iters, keys = 12, 400, 64

	var wg sync.WaitGroup
	for w := range workers {
		seed := w

		wg.Go(func() {
			for i := range iters {
				k := blockKey{prefix: "p", col: colTsID, blk: (seed*7 + i) % keys}

				if ent, ok := c.get(k); ok {
					_ = ent.i64[0] // read the pinned buffer; must not be recycled concurrently
					c.release(ent)

					continue
				}

				buf := c.getI64Buf(1)
				buf[0] = int64(k.blk)
				ent := c.insert(&blockEntry{key: k, i64: buf, bytes: 8})
				require.Equal(t, int64(k.blk), ent.i64[0])
				c.release(ent)
			}
		})
	}

	wg.Wait()
}

// TestBufFreeListBounded checks the freelist honors its count bound (so pooled-but-free buffers add a
// bounded amount to RSS) and that a too-small recycled buffer stays pooled for a later smaller
// request instead of being discarded.
func TestBufFreeListBounded(t *testing.T) {
	t.Parallel()

	var p bufFreeList[int64]
	for range blockBufCap + 10 {
		p.put(make([]int64, 8))
	}

	assert.Len(t, p.free, blockBufCap, "the freelist is bounded")

	// A recycled buffer smaller than the request is skipped (kept pooled), not returned undersized.
	var q bufFreeList[int64]
	q.put(make([]int64, 0, 2))
	got := q.get(8)
	assert.GreaterOrEqual(t, cap(got), 8)
	assert.Len(t, q.free, 1, "the too-small buffer stays pooled for a smaller request")

	small := q.get(2)
	assert.Equal(t, 2, cap(small), "a later fitting request draws the kept buffer")
	assert.Empty(t, q.free)
}

// TestBufFreeListScalesWithInflight checks the block cache's freelist bound grows with the number
// of registered in-flight fetches (and falls back to the baseline when they end), so a concurrency
// burst's returned buffers are kept for the next fetch instead of dropped at the static cap.
func TestBufFreeListScalesWithInflight(t *testing.T) {
	t.Parallel()

	c := newBlockCache(1 << 20)

	fill := func() int {
		n := 0

		for {
			before := len(c.i64Free.free)
			c.i64Free.put(make([]int64, 8))

			if len(c.i64Free.free) == before {
				return n
			}

			n++
		}
	}

	require.Equal(t, blockBufCap, fill(), "at rest the bound is the baseline")

	c.fetchStart()
	c.fetchStart()

	extra := fill()
	assert.Equal(t, 2*blockBufCap, extra, "two in-flight fetches double the headroom over the baseline")

	c.fetchEnd()
	c.fetchEnd()

	assert.Equal(t, 0, fill(), "back at rest the (now over-full) list accepts nothing more")
}

// TestReleaseSeriesPinsKeepsMemo checks the per-series pin release: pins other than the memoized
// entries are released (refs drop, evicted buffers recycle), while exactly one pin per memoized
// block survives so its held view stays valid for the next series.
func TestReleaseSeriesPinsKeepsMemo(t *testing.T) {
	t.Parallel()

	c := newBlockCache(1 << 20)
	r := &seriesBlockReader{engine: &Engine{blockCache: c}}

	entA := c.insert(&blockEntry{key: blockKey{prefix: "p", col: colTsID, blk: 0}, i64: make([]int64, 1), bytes: 8})
	entB := c.insert(&blockEntry{key: blockKey{prefix: "p", col: colTsID, blk: 1}, i64: make([]int64, 1), bytes: 8})
	entV := c.insert(&blockEntry{key: blockKey{prefix: "p", col: colValID, blk: 1}, f64: make([]float64, 1), bytes: 8})

	// The reader visited ts blocks 0 and 1 (block 1 twice: revisited) and value block 1; the memo
	// currently holds ts block 1 and value block 1.
	entB.refs++ // second pin from the revisit

	r.pins = []*blockEntry{entA, entB, entB, entV}
	r.tsEnt, r.valEnt = entB, entV

	r.releaseSeriesPins()

	assert.Equal(t, 0, entA.refs, "a non-memo pin is released")
	assert.Equal(t, 1, entB.refs, "exactly one pin survives for a memoized block, duplicates released")
	assert.Equal(t, 1, entV.refs, "the value memo keeps its pin")
	assert.Equal(t, []*blockEntry{entB, entV}, r.pins, "pins compact to the retained memo entries")

	r.releasePins()
	assert.Zero(t, entB.refs)
	assert.Zero(t, entV.refs)
}
