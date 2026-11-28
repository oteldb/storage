package backend

import (
	"context"
	"math"
	"sync"
	"sync/atomic"

	"github.com/go-faster/errors"
	"github.com/maypok86/otter/v2"
	"github.com/maypok86/otter/v2/stats"
)

type postingKey struct {
	object readCacheKey
	lookup string
}

type postingBitmap struct {
	words  []uint64
	chunks int
}

type postingLoad struct {
	done   chan struct{}
	bitmap postingBitmap
	err    error
}

type postingCache struct {
	values *otter.Cache[postingKey, postingBitmap]
	loads  sync.Map
}

type postingCounters struct {
	hits, builds, joins, skipped, chunks, avoided atomic.Int64
}

// PostingCacheViewer shares validated, read-only bitmaps for immutable objects.
type PostingCacheViewer interface {
	PostingCacheEnabled() bool
	ReadPostingBitmap(ctx context.Context, object string, lookup []byte, load func() ([]uint64, int, error)) ([]uint64, error)
}

// PostingCacheStats describes exact posting reuse and cache residency.
type PostingCacheStats struct {
	Hits, Builds, Joins, Skipped, ChunksVisited, ChunksAvoided int64
	Evictions, Bytes                                           uint64
	Items                                                      int
}

func newPostingCache(maxBytes int64) *postingCache {
	return &postingCache{values: otter.Must(&otter.Options[postingKey, postingBitmap]{
		MaximumWeight: uint64(maxBytes),
		Weigher: func(key postingKey, bitmap postingBitmap) uint32 {
			return uint32(min(int64(len(bitmap.words))*8+int64(len(key.lookup)+len(key.object.name))+256, math.MaxUint32))
		},
		StatsRecorder: stats.NewCounter(),
	})}
}

func (c *cachedBackend) PostingCacheEnabled() bool { return c.postings != nil }

func (c *cachedBackend) ReadPostingBitmap(
	ctx context.Context, object string, lookup []byte, load func() ([]uint64, int, error),
) ([]uint64, error) {
	if c.postings == nil {
		words, _, err := load()
		return words, err
	}

	generation, ok := c.pages.Load(object)
	if !ok {
		generation, _ = c.pages.LoadOrStore(object, &pageGeneration{})
	}
	key := postingKey{object: c.readKey(ctx, object), lookup: string(lookup)}
	key.object.generation = generation.(*pageGeneration)
	if bitmap, ok := c.postings.values.GetIfPresent(key); ok {
		return c.postingHit(bitmap), nil
	}

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		loadState := &postingLoad{done: make(chan struct{})}
		prior, loaded := c.postings.loads.LoadOrStore(key, loadState)
		if !loaded {
			return c.loadPosting(ctx, key, loadState, load)
		}
		call := prior.(*postingLoad)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-call.done:
			if errors.Is(call.err, context.Canceled) || errors.Is(call.err, context.DeadlineExceeded) {
				continue
			}
			if call.err == nil {
				c.postingStats.joins.Add(1)
				c.postingStats.avoided.Add(int64(call.bitmap.chunks))
			}
			return call.bitmap.words, call.err
		}
	}
}

func (c *cachedBackend) postingHit(bitmap postingBitmap) []uint64 {
	c.postingStats.hits.Add(1)
	c.postingStats.avoided.Add(int64(bitmap.chunks))
	return bitmap.words
}

func (c *cachedBackend) buildPosting(load func() ([]uint64, int, error)) (postingBitmap, error) {
	c.postingStats.builds.Add(1)
	words, chunks, err := load()
	c.postingStats.chunks.Add(int64(chunks))
	return postingBitmap{words: words, chunks: chunks}, err
}

func (c *cachedBackend) loadPosting(
	ctx context.Context, key postingKey, state *postingLoad, load func() ([]uint64, int, error),
) ([]uint64, error) {
	defer func() {
		c.postings.loads.Delete(key)
		close(state.done)
	}()
	if bitmap, ok := c.postings.values.GetIfPresent(key); ok {
		state.bitmap = bitmap
		return c.postingHit(bitmap), nil
	}
	state.bitmap, state.err = c.buildPosting(load)
	if state.err != nil {
		return nil, state.err
	}
	// Sparse selectors do not get to retain a bitmap sized to the entire root.
	if state.bitmap.chunks < 4 || len(state.bitmap.words) > (2<<20)/8 {
		c.postingStats.skipped.Add(1)
	} else if ctx.Err() == nil {
		c.postings.values.Set(key, state.bitmap)
	}
	return state.bitmap.words, nil
}

func (c *cachedBackend) PostingStats() PostingCacheStats {
	p := &c.postingStats
	out := PostingCacheStats{
		Hits: p.hits.Load(), Builds: p.builds.Load(), Joins: p.joins.Load(), Skipped: p.skipped.Load(),
		ChunksVisited: p.chunks.Load(), ChunksAvoided: p.avoided.Load(),
	}
	if c.postings != nil {
		values := c.postings.values
		values.CleanUp()
		out.Evictions = values.Stats().Evictions
		out.Bytes, out.Items = values.WeightedSize(), values.EstimatedSize()
	}
	return out
}
