package backend

import "context"

// PageViewer optionally caches immutable byte ranges under the backend's read budget.
type PageViewer interface {
	ReadPageView(ctx context.Context, key string, off, n int64) ([]byte, error)
}

// ReadPageView borrows a read-only range, using the shared page cache when available.
func ReadPageView(ctx context.Context, b Backend, key string, off, n int64) ([]byte, error) {
	if v, ok := b.(PageViewer); ok {
		return v.ReadPageView(ctx, key, off, n)
	}
	return ReadViewAt(ctx, b, key, off, n)
}

type pageGeneration [1]byte

type readCacheKey struct {
	name       string
	generation *pageGeneration
	offset     int64
	length     int64
}

func (c *cachedBackend) invalidate(key string) {
	c.values.Invalidate(readCacheKey{name: key})
	c.pages.Delete(key)
}

// ReadPageView caches ranges alongside whole objects, sharing one total byte budget.
func (c *cachedBackend) ReadPageView(ctx context.Context, key string, off, n int64) ([]byte, error) {
	if off < 0 || n < 0 {
		return ReadAt(ctx, c.inner, key, off, n)
	}
	if v, ok := c.values.GetIfPresent(readCacheKey{name: key}); ok {
		return clampRange(v, off, n), nil
	}
	generation, ok := c.pages.Load(key)
	if !ok {
		generation, _ = c.pages.LoadOrStore(key, &pageGeneration{})
	}
	cacheKey := readCacheKey{name: key, generation: generation.(*pageGeneration), offset: off, length: n}
	v, err := c.values.Get(ctx, cacheKey, c.load)
	if err != nil {
		return nil, err
	}
	if int64(len(v)) > c.maxBytes {
		c.values.Invalidate(cacheKey)
	}
	return v, nil
}
