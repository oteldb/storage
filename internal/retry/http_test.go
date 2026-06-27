package retry_test

import (
	"context"
	"io"
	"net"
	"syscall"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"

	"github.com/oteldb/storage/internal/retry"
)

var (
	errInner = errors.New("inner")
	errOther = errors.New("some other transport error")
)

func TestTransient(t *testing.T) {
	t.Parallel()

	assert.False(t, retry.Transient(nil))
	assert.False(t, retry.Transient(context.Canceled), "parent gave up")
	assert.True(t, retry.Transient(context.DeadlineExceeded), "per-try deadline is retryable")
	assert.True(t, retry.Transient(io.ErrUnexpectedEOF))
	assert.True(t, retry.Transient(syscall.ECONNRESET))
	assert.True(t, retry.Transient(&net.OpError{Op: "read", Err: errInner}))
	assert.True(t, retry.Transient(errOther))
}

func TestConnFailure(t *testing.T) {
	t.Parallel()

	assert.False(t, retry.ConnFailure(nil))
	assert.True(t, retry.ConnFailure(syscall.ECONNREFUSED))
	assert.True(t, retry.ConnFailure(&net.OpError{Op: "dial", Err: errInner}))
	assert.False(t, retry.ConnFailure(&net.OpError{Op: "read", Err: errInner}), "ambiguous: request may have been applied")
	assert.False(t, retry.ConnFailure(context.DeadlineExceeded), "ambiguous timeout is not safe to retry for writes")
}

func TestRetryableStatus(t *testing.T) {
	t.Parallel()

	assert.True(t, retry.RetryableStatus(500))
	assert.True(t, retry.RetryableStatus(503))
	assert.True(t, retry.RetryableStatus(429))
	assert.False(t, retry.RetryableStatus(404))
	assert.False(t, retry.RetryableStatus(400))
	assert.False(t, retry.RetryableStatus(200))
}
