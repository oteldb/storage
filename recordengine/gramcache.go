package recordengine

import (
	"context"

	"github.com/go-faster/errors"
	"github.com/maypok86/otter/v2"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/index/bloom"
)

// defaultGramCacheBytes bounds the decoded gram filters held in memory when [Config.GramCacheBytes]
// is unset. Gram filters are 4–5× a token bloom and 3.4–5.9% of a part's on-disk bytes, so holding
// one per live part the way [part.blooms] does would make them the process's largest resident term
// (~48 GiB per TiB stored). They are demand-loaded instead: a fetch reads a part's filter only when
// a substring hint targets that part's column, and the cache keeps the working set — the recently
// queried parts — rather than all of it.
const defaultGramCacheBytes = 64 << 20

// gramCache is the demand-load cache for decoded gram filters, keyed by sidecar key.
//
// It is otter's weight-bounded loading cache, which supplies the three properties this needs and
// which a plain LRU does not: the bound is in *bytes* (entries differ by an order of magnitude, so
// an entry count would not bound memory), concurrent misses on the same sidecar collapse into one
// backend read (on an object store, one GET instead of one per in-flight query), and the loader's
// verdict is cached even when it is "no filter here" — so a part without a sidecar is not re-probed
// by every query. Eviction is adaptive W-TinyLFU rather than strict LRU, which matters for the
// access pattern this sees: a scan over many cold parts should not evict the hot ones.
//
// The load dedup is best-effort, not exclusive: otter checks the map and then starts a call, so a
// caller arriving in that window loads a second time. Harmless here — a sidecar is immutable, so
// the duplicate read returns identical bytes and one of the two results is simply dropped.
//
// Within an engine's life part prefixes are append-only (a sequence is burned, never reused — see
// [Engine.reserveSeq]), so a cached entry cannot be served for a *different* part that later took
// the same key; entries for deleted parts are just garbage the policy ages out. [Engine.Reset] is
// the sole exception — it rewinds the sequence — and invalidates the cache for exactly that reason.
type gramCache = otter.Cache[string, *bloom.Filter]

func newGramCache(maxBytes int64) *gramCache {
	if maxBytes <= 0 {
		maxBytes = defaultGramCacheBytes
	}

	return otter.Must(&otter.Options[string, *bloom.Filter]{
		MaximumWeight: uint64(maxBytes),
		Weigher:       gramWeight,
	})
}

// gramWeight prices a cache entry by the filter's resident bytes, which is what [Config.GramCacheBytes]
// bounds. A cached miss (no usable sidecar) is a nil filter: it costs nothing resident and must not
// be weighed as if it did, or a store full of gram-less parts would evict the real filters.
func gramWeight(_ string, f *bloom.Filter) uint32 {
	if f == nil {
		return 0
	}

	return uint32(f.Bytes())
}

// gramFilterFor returns the gram filter for the part column at key, reading it through the cache. A
// nil filter (with a nil error) means the part has no usable gram index there, so it cannot be
// pruned — that verdict is itself cached.
//
// A read error is returned, not cached: the next query retries. Callers treat it as "cannot prune".
func gramFilterFor(ctx context.Context, cache *gramCache, b backend.Backend, key string) (*bloom.Filter, error) {
	return cache.Get(ctx, key, otter.LoaderFunc[string, *bloom.Filter](
		func(ctx context.Context, key string) (*bloom.Filter, error) {
			f, _, err := readGramFilter(ctx, b, key)

			return f, err
		},
	))
}

// readGramFilter reads and decodes one gram sidecar. ok is false for an absent sidecar, or one this
// build must not read: the part simply does not prune by substring, which is not an error.
func readGramFilter(ctx context.Context, b backend.Backend, key string) (f *bloom.Filter, ok bool, err error) {
	data, err := b.Read(ctx, key)
	if err != nil {
		if errors.Is(err, backend.ErrNotExist) {
			return nil, false, nil
		}

		return nil, false, errors.Wrapf(err, "read grams %q", key)
	}

	f, err = decodeGramFilter(data)
	if err != nil {
		if errors.Is(err, errGramFormat) {
			return nil, false, nil
		}

		return nil, false, errors.Wrapf(err, "decode grams %q", key)
	}

	return f, true, nil
}

// grams returns the engine's gram-filter cache, allocating it on first use. Engines whose schema has
// no gram column never build one.
func (e *Engine) grams() *gramCache {
	e.gramOnce.Do(func() { e.gramCache = newGramCache(e.cfg.GramCacheBytes) })

	return e.gramCache
}
