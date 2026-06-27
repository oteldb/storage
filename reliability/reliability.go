// Package reliability holds the embedder-facing knobs for surviving unreliable transports — the
// node-to-node cluster RPCs and the S3 backend — in lossy, noisy environments where a request can
// fail immediately, hang and fail after a long timeout, or simply run slow.
//
// The library applies a [RetryConfig] in two complementary ways, both off the data plane's hot path:
//
//   - idempotent reads (cluster read fan-out, S3 GET/LIST/HEAD) are *hedged*: once an in-flight
//     attempt passes [RetryConfig.HedgeDelay] with no answer, a second attempt races it (to another
//     replica for cluster reads, or a fresh connection for S3), first success wins, losers cancel —
//     this cuts the long tail caused by a single slow/stuck request;
//   - every attempt is bounded by [RetryConfig.PerTryTimeout] so one stuck call cannot consume the
//     whole deadline, and failed attempts retry with exponential backoff up to [RetryConfig.MaxAttempts].
//
// Writes are retried conservatively (only when the request provably never reached the server), so
// they keep at-most-once semantics.
package reliability

import "time"

// RetryConfig tunes retries, per-attempt timeouts, and hedging. The zero value disables all of it
// (one attempt, no timeout) — i.e. plain calls; use [Default] or [LossyEnvironment] as a base.
type RetryConfig struct {
	// MaxAttempts is the total attempts for one logical call (≥1). For hedged reads it caps how
	// many replicas/re-issues race; for writes it caps sequential retries.
	MaxAttempts int

	// PerTryTimeout bounds a single attempt. 0 means an attempt may run until the caller's deadline.
	PerTryTimeout time.Duration

	// BaseBackoff and MaxBackoff bound the exponential, jittered wait between sequential retries
	// (writes and S3). 0 BaseBackoff retries with no wait.
	BaseBackoff time.Duration
	MaxBackoff  time.Duration

	// HedgeDelay is how long an idempotent read waits for the in-flight attempt before racing the
	// next one. 0 disables hedging (reads fail over only on error). Keep it comfortably above the
	// normal-case latency so the fast path never pays for a redundant request.
	HedgeDelay time.Duration
}

// Enabled reports whether the config asks for anything beyond a single plain attempt.
func (c RetryConfig) Enabled() bool {
	return c.MaxAttempts > 1 || c.PerTryTimeout > 0 || c.HedgeDelay > 0
}

// Default is a mild, broadly-safe profile: a couple of retries, a generous per-attempt timeout, and
// a hedge delay high enough that a healthy LAN never hedges. It improves durability without changing
// behavior on a fast, reliable network.
func Default() RetryConfig {
	return RetryConfig{
		MaxAttempts:   3,
		PerTryTimeout: 10 * time.Second,
		BaseBackoff:   50 * time.Millisecond,
		MaxBackoff:    2 * time.Second,
		HedgeDelay:    150 * time.Millisecond,
	}
}

// LossyEnvironment is an aggressive profile for networks that drop, stall, or 30-second-timeout
// requests: short per-attempt timeouts so a hang is abandoned quickly, more attempts, and an early
// hedge so a slow replica is raced almost immediately. It trades extra redundant requests for a much
// tighter tail latency.
func LossyEnvironment() RetryConfig {
	return RetryConfig{
		MaxAttempts:   4,
		PerTryTimeout: 3 * time.Second,
		BaseBackoff:   25 * time.Millisecond,
		MaxBackoff:    time.Second,
		HedgeDelay:    40 * time.Millisecond,
	}
}
