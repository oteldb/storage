package tenant

import (
	"time"

	"github.com/oteldb/storage/signal"
)

// Limits are the per-tenant operational limits enforced as lossless admission control: when a
// tenant exceeds one, the offending samples are shed and reported via OTLP partial-success
// (RESOURCE_EXHAUSTED), so an overload degrades rather than OOMs. They are resolved per write,
// so a consumer's hot-reloaded changes take effect immediately. Zero values mean "no limit".
type Limits struct {
	// IngestBytesPerSecond caps the per-tenant ingest rate (a token bucket whose burst is one
	// second of budget). Bytes over budget are shed. Zero ⇒ unlimited.
	IngestBytesPerSecond int64
	// MaxInFlightBytes caps the unflushed in-flight bytes buffered for a tenant (memory
	// backpressure): samples arriving while at the cap are shed until a flush drains the head.
	// Zero ⇒ unlimited.
	MaxInFlightBytes int64
	// MaxSeries caps the active series cardinality per tenant: a sample that would mint a new
	// series past the cap is shed; existing series are unaffected. Zero ⇒ unlimited.
	MaxSeries int64
	// MaxPartSize caps an immutable part's size. Zero ⇒ unlimited. (Not yet enforced.)
	MaxPartSize int64
}

// Retention is the per-tenant retention policy (DESIGN.md §10): whole-partition drops,
// never per-row deletes. Zero values mean "retain forever".
type Retention struct {
	// MaxAge is the maximum age of retained data. Zero ⇒ retain forever.
	MaxAge time.Duration
	// MaxBytes is the maximum total retained bytes. Zero ⇒ unlimited.
	MaxBytes int64
}

// DownsampleTier rolls up samples once they reach a given age: every sample older than
// After is replaced, at merge time, by one representative per Interval-wide time bucket,
// the bucket's samples combined by Agg. A tier with Interval ≤ 0 is ignored.
//
// Tiers coarsen old data: configure increasing After with increasing Interval (e.g. 5m
// buckets after 24h, 1h buckets after 7d). A sample is rolled up by the coarsest tier
// whose After it has exceeded; samples younger than every tier's After stay raw. Buckets
// are aligned to absolute multiples of Interval (not to ingest time), so the rollup of a
// time range is independent of when the merge runs — repeated merges are stable.
type DownsampleTier struct {
	// After is the age past which this tier applies (relative to now at merge time).
	After time.Duration
	// Interval is the rollup bucket width. Zero ⇒ the tier is disabled.
	Interval time.Duration
	// Agg combines the samples in a bucket. The zero value is [signal.AggLast].
	Agg signal.Aggregation
}

// Downsample is the per-tenant downsampling policy: a merge-time rollup reusing the same
// background-merge engine (no separate subsystem). Optional and per-tenant; an empty
// Tiers list disables downsampling (data stays raw until retention drops it).
type Downsample struct {
	// Tiers are the age-banded rollup resolutions, applied at merge time. Order does not
	// matter; the engine assigns each sample to the coarsest applicable tier.
	Tiers []DownsampleTier
}

// Policy is the resolved policy for a tenant: limits, retention, downsampling, and
// routing. It is the return value of [Resolver.Resolve].
type Policy struct {
	Limits     Limits
	Retention  Retention
	Downsample Downsample
	// RoutingHints TODO(M6): per-tenant shard placement / zone preferences.
}

// Resolver maps a [signal.TenantID] to a [Policy] (DESIGN.md §2, §8). It is the
// callback-based multi-tenancy mechanism: the consumer supplies the resolver, which
// may be backed by hot-reloaded overrides (Mimir-style). Implementations must be safe
// for concurrent use.
//
// The zero value [Resolver] is not usable; consumers provide a function via
// [ResolverFunc].
type Resolver interface {
	// Resolve returns the policy for a tenant. It must not be expensive (called on
	// the hot ingest/query path); back hot values with a cache.
	Resolve(t signal.TenantID) Policy
}

// ResolverFunc adapts a function into a [Resolver].
type ResolverFunc func(signal.TenantID) Policy

// Resolve calls f(t).
func (f ResolverFunc) Resolve(t signal.TenantID) Policy { return f(t) }

// Default returns a permissive [Resolver] that applies no limits and infinite
// retention. It is the fallback when [Options.Tenancy] is nil (DESIGN.md §5).
func Default() Resolver { return ResolverFunc(func(signal.TenantID) Policy { return Policy{} }) }
