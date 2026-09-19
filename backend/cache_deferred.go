package backend

import "context"

var _ DeferredSyncer = (*cachedBackend)(nil)

// WriteDeferred is [cachedBackend.Write] through the inner backend's deferred write. Implements
// [DeferredSyncer].
func (c *cachedBackend) WriteDeferred(ctx context.Context, key string, data []byte) error {
	if err := WriteDeferred(ctx, c.inner, key, data); err != nil {
		return err
	}

	c.store(key, data)

	return nil
}

// CreateObjectDeferred forwards the deferred incremental write, invalidating like
// [cachedBackend.CreateObject]. Implements [DeferredSyncer].
func (c *cachedBackend) CreateObjectDeferred(ctx context.Context, key string) (ObjectWriter, error) {
	w, err := CreateObjectDeferred(ctx, c.inner, key)
	if err != nil {
		return nil, err
	}

	return &cacheInvalidatingWriter{ObjectWriter: w, cache: c, key: key}, nil
}

// DeleteDeferred is [cachedBackend.Delete] through the inner backend's deferred delete. Implements
// [DeferredSyncer].
func (c *cachedBackend) DeleteDeferred(ctx context.Context, key string) error {
	err := DeleteDeferred(ctx, c.inner, key)
	c.values.Invalidate(key)

	return err
}

// SyncPrefix forwards to the inner backend; durability does not touch the cache. Implements
// [DeferredSyncer].
func (c *cachedBackend) SyncPrefix(ctx context.Context, prefix string) error {
	return SyncPrefix(ctx, c.inner, prefix)
}

// WriteUncachedDeferred is [WriteUncached] through b's deferred write.
func WriteUncachedDeferred(ctx context.Context, b Backend, key string, data []byte) error {
	c, ok := b.(*cachedBackend)
	if !ok {
		return WriteDeferred(ctx, b, key, data)
	}

	if err := WriteDeferred(ctx, c.inner, key, data); err != nil {
		return err
	}

	c.values.Invalidate(key)

	return nil
}
