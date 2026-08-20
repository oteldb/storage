package s3

import (
	"context"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
)

// CompareAndSwap stores data under key only if the object still carries the ETag the committer
// read, using S3's own conditional PUT (If-Match, or If-None-Match: * for the create). The store
// evaluates the condition, so this is a genuine CAS across processes and regions — not a
// read-then-write this backend could lose a race inside. Implements [backend.Backend].
func (b *Backend) CompareAndSwap(
	ctx context.Context, key string, expected backend.Version, data []byte,
) (backend.Version, bool, error) {
	etag, ok, err := b.store.PutObjectIfVersion(ctx, b.key(key), data, string(expected))
	if err != nil {
		return backend.VersionAbsent, false, errors.Wrapf(err, "compare-and-swap %q", key)
	}

	if !ok {
		return backend.VersionAbsent, false, nil
	}

	// An empty ETag would be indistinguishable from [backend.VersionAbsent], so the committer's
	// next conditional write would demand the key be absent and could never succeed. Fail loudly
	// instead of handing back a token that quietly wedges the commit loop.
	if etag == "" {
		return backend.VersionAbsent, false, errors.Errorf("store reported no ETag for %q", key)
	}

	return backend.Version(etag), true, nil
}

// ReadVersioned returns the object and its ETag, which is the version a later
// [Backend.CompareAndSwap] conditions on. Implements [backend.Backend].
func (b *Backend) ReadVersioned(ctx context.Context, key string) ([]byte, backend.Version, error) {
	data, etag, err := b.store.GetObjectVersion(ctx, b.key(key))
	if err != nil {
		if errors.Is(err, ErrObjectNotFound) {
			return nil, backend.VersionAbsent, nil
		}

		return nil, backend.VersionAbsent, errors.Wrapf(err, "read %q", key)
	}

	return data, backend.Version(etag), nil
}
