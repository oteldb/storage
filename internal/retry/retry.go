// Package retry adds durability and tail-latency control to calls over unreliable transports
// (node-to-node RPCs, S3). It provides three things, all allocation-light and used only off the
// data plane's hot path:
//
//   - bounded retries with exponential backoff + jitter ([Do]), for calls that are safe to repeat;
//   - per-attempt timeouts, so a single slow attempt cannot stall a call for the whole deadline;
//   - hedged (opportunistic concurrent) retries ([Hedge]) for idempotent calls: stagger a second
//     attempt once the first is slow, race them, take the first success, and cancel the losers —
//     the classic defense against a tail of slow/stuck requests in a lossy, noisy environment.
//
// A [Policy] is a value; the zero value runs exactly one attempt with no timeout (i.e. a plain
// call), so wiring it in is always safe.
package retry

import (
	"context"
	"math/rand/v2"
	"time"

	"github.com/go-faster/errors"
)

// Policy configures retries, per-attempt timeouts, and hedging. It is a plain value; copy freely.
type Policy struct {
	// MaxAttempts is the total number of attempts for one logical call (≥1). 0 means 1.
	MaxAttempts int

	// PerTryTimeout bounds a single attempt. 0 disables it (the attempt runs until the parent
	// context's deadline). With hedging, a slow attempt is bypassed by [HedgeDelay] regardless;
	// the per-try timeout additionally bounds the resources a stuck attempt holds.
	PerTryTimeout time.Duration

	// BaseBackoff is the wait before the second sequential attempt in [Do]; it doubles each attempt
	// up to MaxBackoff, with equal jitter. 0 disables backoff (attempts run back-to-back).
	BaseBackoff time.Duration
	MaxBackoff  time.Duration

	// HedgeDelay is how long [Hedge] waits for the in-flight attempt before launching the next one
	// concurrently. 0 makes [Hedge] a pure sequential failover (the next launches only when the
	// current one fails). Tune it above the normal-case latency so the fast path never hedges.
	HedgeDelay time.Duration

	// Retryable classifies an error: true ⇒ try the next attempt, false ⇒ fail immediately (e.g.
	// a "not found" or a 4xx is permanent). nil ⇒ retry every error except a parent-context
	// cancellation.
	Retryable func(error) bool

	// Rand returns a float in [0,1) for backoff jitter. nil ⇒ the default global source.
	Rand func() float64

	// Observability hooks (all optional). attempt is 0-based; attempt 0 is the first try.
	OnAttempt func(attempt int)                                // every launch (incl. the first)
	OnRetry   func(attempt int, err error, wait time.Duration) // before a sequential retry waits
	OnHedge   func(attempt int)                                // when a hedged attempt is launched (attempt ≥ 1)
}

func (p Policy) attempts() int {
	if p.MaxAttempts < 1 {
		return 1
	}

	return p.MaxAttempts
}

func (p Policy) retryable(err error) bool {
	if err == nil {
		return false
	}

	if p.Retryable != nil {
		return p.Retryable(err)
	}

	return !errors.Is(err, context.Canceled)
}

func (p Policy) randFloat() float64 {
	if p.Rand != nil {
		return p.Rand()
	}

	return rand.Float64() //nolint:gosec // jitter, not security-sensitive
}

// backoff returns the wait before the given (1-based) retry: base·2^(n-1), capped at MaxBackoff,
// with equal jitter (half fixed, half random) so a fleet does not retry in lockstep.
func (p Policy) backoff(retry int) time.Duration {
	if p.BaseBackoff <= 0 {
		return 0
	}

	d := p.BaseBackoff
	for i := 1; i < retry; i++ {
		d *= 2
		if p.MaxBackoff > 0 && d >= p.MaxBackoff {
			d = p.MaxBackoff

			break
		}
	}

	if p.MaxBackoff > 0 && d > p.MaxBackoff {
		d = p.MaxBackoff
	}

	half := d / 2

	return half + time.Duration(p.randFloat()*float64(half))
}

// Do runs fn with sequential retries under the policy: at most [Policy.MaxAttempts] attempts, each
// bounded by [Policy.PerTryTimeout], spaced by exponential backoff. It stops early on success, on a
// non-retryable error, or when ctx is done. Use it for calls that are safe to repeat but should not
// run concurrently (e.g. a write with a bounded retry budget).
func Do[T any](ctx context.Context, p Policy, fn func(context.Context) (T, error)) (T, error) {
	var (
		zero    T
		lastErr error
	)

	n := p.attempts()
	for i := range n {
		if i > 0 {
			wait := p.backoff(i)
			if p.OnRetry != nil {
				p.OnRetry(i, lastErr, wait)
			}

			if !sleep(ctx, wait) {
				return zero, ctx.Err()
			}
		}

		if p.OnAttempt != nil {
			p.OnAttempt(i)
		}

		v, err := runOne(ctx, p.PerTryTimeout, fn)
		if err == nil {
			return v, nil
		}

		lastErr = err

		if !p.retryable(err) || ctx.Err() != nil {
			if ctx.Err() != nil {
				return zero, ctx.Err()
			}

			return zero, err
		}
	}

	return zero, lastErr
}

// Hedge issues the thunks as opportunistic concurrent attempts: it launches the first immediately,
// and launches each next one either after [Policy.HedgeDelay] elapses with no result (the in-flight
// attempt is slow) or as soon as an attempt fails retryably (failover). It returns the first
// success and cancels the rest; a non-retryable error short-circuits. All thunks must be idempotent
// — several may run at once. With one thunk it degrades to a single [Policy.PerTryTimeout]-bounded
// call.
func Hedge[T any](ctx context.Context, p Policy, thunks []func(context.Context) (T, error)) (T, error) {
	var zero T

	if len(thunks) == 0 {
		return zero, errors.New("retry: no attempts")
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel() // first success / return aborts every still-running attempt

	type res struct {
		v   T
		err error
	}

	done := make(chan res, len(thunks)) // buffered: losers never block on send after we return

	var (
		next     int
		inFlight int
	)

	launch := func() {
		i := next
		next++
		inFlight++

		if i > 0 && p.OnHedge != nil {
			p.OnHedge(i)
		}

		if p.OnAttempt != nil {
			p.OnAttempt(i)
		}

		go func() {
			v, err := runOne(ctx, p.PerTryTimeout, thunks[i])
			done <- res{v, err}
		}()
	}

	var timer *time.Timer

	stop := func() {
		if timer != nil {
			timer.Stop()
			timer = nil
		}
	}
	arm := func() {
		stop()

		if p.HedgeDelay > 0 && next < len(thunks) {
			timer = time.NewTimer(p.HedgeDelay)
		}
	}

	defer stop()

	launch()
	arm()

	var lastErr error

	for inFlight > 0 {
		var tick <-chan time.Time
		if timer != nil {
			tick = timer.C
		}

		select {
		case <-ctx.Done():
			return zero, ctx.Err()
		case <-tick:
			if next < len(thunks) { // slow in-flight attempt: stage one more concurrently
				launch()
			}

			arm() // re-arm for the attempt after this one (no-op once the list is exhausted)
		case r := <-done:
			inFlight--

			if r.err == nil {
				return r.v, nil
			}

			lastErr = r.err

			if !p.retryable(r.err) {
				return zero, r.err
			}

			if next < len(thunks) { // failover: promote the next target now, don't wait for the tick
				launch()
			}

			arm()
		}
	}

	return zero, lastErr
}

// Repeat builds n identical thunks from one call, for hedging against a single endpoint (e.g. a
// slow S3 GET re-issued on a fresh connection): the second attempt is a retry, not a different host.
func Repeat[T any](thunk func(context.Context) (T, error), n int) []func(context.Context) (T, error) {
	if n < 1 {
		n = 1
	}

	out := make([]func(context.Context) (T, error), n)
	for i := range out {
		out[i] = thunk
	}

	return out
}

// runOne applies the per-attempt timeout (if any) around a single call.
func runOne[T any](ctx context.Context, perTry time.Duration, fn func(context.Context) (T, error)) (T, error) {
	if perTry <= 0 {
		return fn(ctx)
	}

	cctx, cancel := context.WithTimeout(ctx, perTry)
	defer cancel()

	return fn(cctx)
}

// sleep waits d, returning false if ctx is done first. A non-positive d only checks ctx.
func sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}

	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
