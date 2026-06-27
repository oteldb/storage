// Package s3 implements a [backend.Backend] over an S3-compatible object store. The
// store-specific calls are isolated behind the small [ObjectStore] interface: the AWS
// SDK adapter ([NewAWS]) implements it for real S3, and tests implement it with an
// in-memory fake, so all of the Backend's mapping logic — key prefixing, sorted listing,
// 404 → [backend.ErrNotExist] translation, conditional-put, and existence-checked delete —
// is exercised by the shared backend conformance suite without a live bucket.
//
// The object store is the durable, stateless tier: nodes hold no authoritative state, so
// the read path is reconstructed entirely from objects (DESIGN.md §3, §11). Whole-object
// Get/Put is sufficient because a part maps to one key prefix with one object per
// column/marks/manifest.
package s3

import (
	"context"
	"slices"
	"strings"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
)

// ErrObjectNotFound is returned (wrapped) by [ObjectStore.GetObject] for an absent key. The
// [Backend] translates it to [backend.ErrNotExist].
var ErrObjectNotFound = errors.New("s3: object not found")

// ObjectStore is the minimal object-store API the [Backend] needs. It is intentionally
// thin and S3-shaped (delete is idempotent, list is by prefix); the Backend layers the
// backend.Backend contract on top. Implementations must be safe for concurrent use.
type ObjectStore interface {
	// GetObject returns the object bytes, or an error wrapping [ErrObjectNotFound] if absent.
	GetObject(ctx context.Context, key string) ([]byte, error)

	// PutObject stores data under key, overwriting any existing object.
	PutObject(ctx context.Context, key string, data []byte) error

	// PutObjectIfAbsent stores data under key only if it does not exist, returning whether it
	// was created (the conditional write — S3 If-None-Match: *).
	PutObjectIfAbsent(ctx context.Context, key string, data []byte) (bool, error)

	// HeadObject reports whether key exists.
	HeadObject(ctx context.Context, key string) (bool, error)

	// DeleteObject removes key. It is idempotent (no error if the key is absent), mirroring
	// S3 DeleteObject.
	DeleteObject(ctx context.Context, key string) error

	// ListObjects returns every key under prefix (the implementation paginates internally).
	// Order is unspecified; the [Backend] sorts.
	ListObjects(ctx context.Context, prefix string) ([]string, error)
}

// Backend is a [backend.Backend] over an [ObjectStore]. Keys are stored under an optional
// root prefix so several datasets can share one bucket.
type Backend struct {
	store  ObjectStore
	prefix string // root key prefix (e.g. "oteldb/"); may be empty
}

var _ backend.Backend = (*Backend)(nil)

// New returns a [Backend] over store, rooting all keys under keyPrefix (which may be empty). Pass
// [WithRetry] to make it resilient to a lossy/slow endpoint (per-attempt timeouts, bounded retries,
// hedged GETs).
func New(store ObjectStore, keyPrefix string, opts ...Option) *Backend {
	var cfg config
	for _, opt := range opts {
		opt(&cfg)
	}

	if cfg.retry.Enabled() {
		store = newRetryStore(store, cfg.retry)
	}

	return &Backend{store: store, prefix: keyPrefix}
}

// IsEphemeral reports false: objects persist in the store.
func (*Backend) IsEphemeral() bool { return false }

// Write stores data under key, overwriting any existing object.
func (b *Backend) Write(ctx context.Context, key string, data []byte) error {
	if err := b.store.PutObject(ctx, b.key(key), data); err != nil {
		return errors.Wrapf(err, "put %q", key)
	}

	return nil
}

// PutIfAbsent stores data under key only if it does not already exist.
func (b *Backend) PutIfAbsent(ctx context.Context, key string, data []byte) (bool, error) {
	ok, err := b.store.PutObjectIfAbsent(ctx, b.key(key), data)
	if err != nil {
		return false, errors.Wrapf(err, "put-if-absent %q", key)
	}

	return ok, nil
}

// Read returns the value stored under key, or an [backend.ErrNotExist]-wrapping error.
func (b *Backend) Read(ctx context.Context, key string) ([]byte, error) {
	data, err := b.store.GetObject(ctx, b.key(key))
	if err != nil {
		if errors.Is(err, ErrObjectNotFound) {
			return nil, errors.Wrapf(backend.ErrNotExist, "read %q", key)
		}

		return nil, errors.Wrapf(err, "read %q", key)
	}

	return data, nil
}

// List returns, sorted ascending, every key with the given prefix.
func (b *Backend) List(ctx context.Context, prefix string) ([]string, error) {
	keys, err := b.store.ListObjects(ctx, b.key(prefix))
	if err != nil {
		return nil, errors.Wrapf(err, "list %q", prefix)
	}

	out := make([]string, 0, len(keys))
	for _, k := range keys {
		out = append(out, b.unkey(k))
	}

	slices.Sort(out)

	return out, nil
}

// Delete removes key, returning an [backend.ErrNotExist]-wrapping error if it is absent.
// Because S3 DeleteObject is idempotent, existence is checked first to honor the contract.
func (b *Backend) Delete(ctx context.Context, key string) error {
	exists, err := b.store.HeadObject(ctx, b.key(key))
	if err != nil {
		return errors.Wrapf(err, "head %q", key)
	}

	if !exists {
		return errors.Wrapf(backend.ErrNotExist, "delete %q", key)
	}

	if err := b.store.DeleteObject(ctx, b.key(key)); err != nil {
		return errors.Wrapf(err, "delete %q", key)
	}

	return nil
}

// key maps a backend key to a store key by prepending the root prefix.
func (b *Backend) key(k string) string { return b.prefix + k }

// unkey strips the root prefix from a store key.
func (b *Backend) unkey(k string) string { return strings.TrimPrefix(k, b.prefix) }
