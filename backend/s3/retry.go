package s3

import (
	"context"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/internal/retry"
	"github.com/oteldb/storage/reliability"
)

// Option configures a [Backend] at construction.
type Option func(*config)

type config struct {
	retry reliability.RetryConfig
}

// WithRetry makes the backend survive an unreliable S3 endpoint: each call gets a per-attempt
// timeout (so a hung request is abandoned instead of stalling for the provider's full timeout) and
// bounded retries, and GETs are additionally *hedged* — a slow read is re-issued on a fresh
// connection and the first response wins. Use [reliability.LossyEnvironment] for noisy networks.
// The zero config leaves the bare store (the AWS SDK's own retryer still applies).
func WithRetry(c reliability.RetryConfig) Option { return func(o *config) { o.retry = c } }

// retryStore wraps an [ObjectStore] with retry/hedge policies. Reads (idempotent) hedge and retry on
// any transient error except a genuine "not found"; idempotent writes (overwrite, delete) retry on
// transient errors; the conditional put (CAS) retries only when the request provably never reached
// the server, so the conditional semantics are never corrupted by a re-send.
type retryStore struct {
	inner       ObjectStore
	maxAttempts int
	read        retry.Policy // hedged GET
	list        retry.Policy // sequential retry (paginated; not hedged)
	write       retry.Policy // idempotent overwrite/delete
	cas         retry.Policy // conditional put (conservative)
}

// rangeRetryStore is [retryStore] plus ranged reads. It is a separate type so the capability is
// claimed only when the wrapped store actually has it: a wrapper that advertises [RangeObjectStore]
// over a store without it would satisfy the type assertion and then fall back to reading whole
// objects, which is the cost a ranged read exists to avoid.
type rangeRetryStore struct {
	*retryStore

	rng RangeObjectStore
}

func (s *rangeRetryStore) GetObjectRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	return retry.Hedge(ctx, s.read, retry.Repeat(func(ctx context.Context) ([]byte, error) {
		return s.rng.GetObjectRange(ctx, key, off, n)
	}, s.maxAttempts))
}

// multipartRetry wraps a [MultipartObjectStore]'s calls in the retry policy each one can bear. It
// is a type of its own rather than methods on [retryStore] so that the range and multipart
// capabilities compose without the wrappers' method sets colliding.
type multipartRetry struct {
	mp    MultipartObjectStore
	write retry.Policy
	cas   retry.Policy
}

type multipartRetryStore struct {
	*retryStore
	*multipartRetry
}

type rangeMultipartRetryStore struct {
	*rangeRetryStore
	*multipartRetry
}

// CreateMultipartUpload retries like an idempotent write. It is not idempotent — a retry that
// lands twice starts a second upload — but the extra upload is never completed, so it becomes no
// object; the bucket's AbortIncompleteMultipartUpload rule reaps it.
func (s *multipartRetry) CreateMultipartUpload(ctx context.Context, key string) (string, error) {
	return retry.Do(ctx, s.write, func(ctx context.Context) (string, error) {
		return s.mp.CreateMultipartUpload(ctx, key)
	})
}

// UploadPart retries freely: a part number identifies its slot, so a re-sent part replaces itself.
func (s *multipartRetry) UploadPart(
	ctx context.Context, key, uploadID string, partNum int32, data []byte,
) (string, error) {
	return retry.Do(ctx, s.write, func(ctx context.Context) (string, error) {
		return s.mp.UploadPart(ctx, key, uploadID, partNum, data)
	})
}

// CompleteMultipartUpload retries only where the request provably never reached the server, the
// same rule the conditional put follows. S3 can answer a complete with 200 and an error body, so
// an uncertain result does not say whether the object became visible, and re-sending is not a
// question this layer can answer — it is the commit point of the object.
func (s *multipartRetry) CompleteMultipartUpload(ctx context.Context, key, uploadID string, etags []string) error {
	_, err := retry.Do(ctx, s.cas, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, s.mp.CompleteMultipartUpload(ctx, key, uploadID, etags)
	})

	return err
}

// AbortMultipartUpload retries like an idempotent delete.
func (s *multipartRetry) AbortMultipartUpload(ctx context.Context, key, uploadID string) error {
	_, err := retry.Do(ctx, s.write, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, s.mp.AbortMultipartUpload(ctx, key, uploadID)
	})

	return err
}

func newRetryStore(inner ObjectStore, c reliability.RetryConfig) ObjectStore {
	base := retry.Policy{
		MaxAttempts:   c.MaxAttempts,
		PerTryTimeout: c.PerTryTimeout,
		BaseBackoff:   c.BaseBackoff,
		MaxBackoff:    c.MaxBackoff,
	}

	read := base
	read.HedgeDelay = c.HedgeDelay
	read.Retryable = func(err error) bool { return !errors.Is(err, ErrObjectNotFound) && retry.Transient(err) }

	list := base
	list.Retryable = retry.Transient

	write := base
	write.Retryable = retry.Transient

	cas := base
	cas.Retryable = retry.ConnFailure

	s := &retryStore{inner: inner, maxAttempts: max(c.MaxAttempts, 1), read: read, list: list, write: write, cas: cas}

	rng, hasRange := inner.(RangeObjectStore)
	mp, hasMultipart := inner.(MultipartObjectStore)

	var mpr *multipartRetry
	if hasMultipart {
		mpr = &multipartRetry{mp: mp, write: write, cas: cas}
	}

	switch {
	case hasRange && hasMultipart:
		return &rangeMultipartRetryStore{
			rangeRetryStore: &rangeRetryStore{retryStore: s, rng: rng},
			multipartRetry:  mpr,
		}
	case hasRange:
		return &rangeRetryStore{retryStore: s, rng: rng}
	case hasMultipart:
		return &multipartRetryStore{retryStore: s, multipartRetry: mpr}
	default:
		return s
	}
}

func (s *retryStore) GetObject(ctx context.Context, key string) ([]byte, error) {
	return retry.Hedge(ctx, s.read, retry.Repeat(func(ctx context.Context) ([]byte, error) {
		return s.inner.GetObject(ctx, key)
	}, s.maxAttempts))
}

func (s *retryStore) HeadObject(ctx context.Context, key string) (bool, error) {
	return retry.Do(ctx, s.list, func(ctx context.Context) (bool, error) {
		return s.inner.HeadObject(ctx, key)
	})
}

func (s *retryStore) ListObjects(ctx context.Context, prefix string) ([]string, error) {
	return retry.Do(ctx, s.list, func(ctx context.Context) ([]string, error) {
		return s.inner.ListObjects(ctx, prefix)
	})
}

func (s *retryStore) PutObject(ctx context.Context, key string, data []byte) error {
	_, err := retry.Do(ctx, s.write, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, s.inner.PutObject(ctx, key, data)
	})

	return err
}

func (s *retryStore) PutObjectIfAbsent(ctx context.Context, key string, data []byte) (bool, error) {
	return retry.Do(ctx, s.cas, func(ctx context.Context) (bool, error) {
		return s.inner.PutObjectIfAbsent(ctx, key, data)
	})
}

func (s *retryStore) PutObjectIfVersion(
	ctx context.Context, key string, data []byte, etag string,
) (string, bool, error) {
	type result struct {
		etag string
		ok   bool
	}

	r, err := retry.Do(ctx, s.cas, func(ctx context.Context) (result, error) {
		e, ok, err := s.inner.PutObjectIfVersion(ctx, key, data, etag)

		return result{etag: e, ok: ok}, err
	})

	return r.etag, r.ok, err
}

func (s *retryStore) GetObjectVersion(ctx context.Context, key string) ([]byte, string, error) {
	type result struct {
		data []byte
		etag string
	}

	r, err := retry.Do(ctx, s.read, func(ctx context.Context) (result, error) {
		data, etag, err := s.inner.GetObjectVersion(ctx, key)

		return result{data: data, etag: etag}, err
	})

	return r.data, r.etag, err
}

func (s *retryStore) DeleteObject(ctx context.Context, key string) error {
	_, err := retry.Do(ctx, s.write, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, s.inner.DeleteObject(ctx, key)
	})

	return err
}
