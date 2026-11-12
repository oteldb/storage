package backend

import (
	"context"

	"github.com/maypok86/otter/v2"
)

// PageDecoder validates a packed page and returns an owned immutable representation.
// DecodePage must be deterministic for its format and must not retain the input.
type PageDecoder interface {
	DecodePage(packed []byte) ([]byte, error)
}

// DecodedPageViewer shares decoded pages under the backend's existing read-cache budget.
// The nonempty format identifies the decoder and all options affecting its output.
type DecodedPageViewer interface {
	ReadDecodedPage(ctx context.Context, key string, off, n int64, format string, decoder PageDecoder) ([]byte, error)
}

// ReadDecodedPage borrows a read-only decoded page, falling back to an uncached decode.
func ReadDecodedPage(ctx context.Context, b Backend, key string, off, n int64, format string, decoder PageDecoder) ([]byte, error) {
	if v, ok := b.(DecodedPageViewer); ok && format != "" {
		return v.ReadDecodedPage(ctx, key, off, n, format, decoder)
	}
	packed, err := ReadAt(ctx, b, key, off, n)
	if err != nil {
		return nil, err
	}
	return decoder.DecodePage(packed)
}

func (c *cachedBackend) ReadDecodedPage(ctx context.Context, key string, off, n int64, format string, decoder PageDecoder) ([]byte, error) {
	if off < 0 || n < 0 || format == "" {
		return ReadDecodedPage(ctx, c.inner, key, off, n, format, decoder)
	}
	generation, ok := c.pages.Load(key)
	if !ok {
		generation, _ = c.pages.LoadOrStore(key, &pageGeneration{})
	}
	k := c.readKey(ctx, key)
	k.generation, k.offset, k.length, k.format = generation.(*pageGeneration), off, n, format
	if value, ok := c.values.GetIfPresent(k); ok {
		return value, nil
	}
	// Misses collapse before decoding. The loader is synchronous and is not retained.
	value, err := c.values.Get(ctx, k, otter.LoaderFunc[readCacheKey, []byte](func(ctx context.Context, _ readCacheKey) ([]byte, error) {
		packed, err := c.ReadPageView(ctx, key, off, n)
		if err != nil {
			return nil, err
		}
		return decoder.DecodePage(packed)
	}))
	if err != nil {
		return nil, err
	}
	if int64(len(value)) > c.maxBytes {
		c.values.Invalidate(k)
	}
	return value, nil
}
