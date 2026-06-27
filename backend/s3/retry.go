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

func newRetryStore(inner ObjectStore, c reliability.RetryConfig) *retryStore {
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

	return &retryStore{inner: inner, maxAttempts: max(c.MaxAttempts, 1), read: read, list: list, write: write, cas: cas}
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

func (s *retryStore) DeleteObject(ctx context.Context, key string) error {
	_, err := retry.Do(ctx, s.write, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, s.inner.DeleteObject(ctx, key)
	})

	return err
}
