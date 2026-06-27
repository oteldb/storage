package retry

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"syscall"
)

// Transient reports whether err is worth retrying for an *idempotent* call: transport failures
// (dial/connection errors, resets, timeouts), a per-attempt deadline, and unexpected EOFs. A
// parent-context cancellation is never retried (the caller gave up). Unknown non-nil errors are
// treated as transient, since an idempotent call is safe to repeat — pair this only with calls that
// truly are idempotent.
func Transient(err error) bool {
	if err == nil {
		return false
	}

	// The parent gave up (canceled). A per-attempt deadline (DeadlineExceeded) IS retryable: the
	// next attempt gets a fresh per-try budget; the outer loop still stops once the parent is done.
	if errors.Is(err, context.Canceled) {
		return false
	}

	if errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) {
		return true
	}

	var ne net.Error
	if errors.As(err, &ne) {
		return true
	}

	return true
}

// ConnFailure reports whether err means the request never reached the server — a dial failure or a
// refused connection. It is the *safe* retry predicate for non-idempotent calls (writes): retrying
// is correct only when we know the server did not act on the request. An ambiguous failure (a
// timeout after the body was sent) returns false, giving writes at-most-once semantics.
func ConnFailure(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, syscall.ECONNREFUSED) {
		return true
	}

	var oe *net.OpError
	if errors.As(err, &oe) && oe.Op == "dial" {
		return true
	}

	return false
}

// RetryableStatus reports whether an HTTP status code is a transient server-side failure (5xx, or
// 429 Too Many Requests). 4xx (except 429) is a permanent client error and must not be retried.
func RetryableStatus(code int) bool {
	return code == http.StatusTooManyRequests || code >= 500
}
