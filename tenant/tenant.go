package tenant

import (
	"time"

	"github.com/oteldb/storage/signal"
)

// Limits are the per-tenant operational limits (DESIGN.md §8, §9): a memory tracker
// caps in-flight bytes; ingestion rate and series cardinality are bounded. Hot-reloaded
// by the consumer via [Resolver]. Zero values mean "no limit".
//
// This is a scaffold stub; fields are filled in as the engine grows (M3+).
type Limits struct {
	// IngestBytesPerSecond caps the ingest rate. Zero ⇒ unlimited.
	IngestBytesPerSecond int64
	// MaxInFlightBytes caps in-flight bytes per tenant (backpressure).
	MaxInFlightBytes int64
	// MaxSeries caps the active series cardinality.
	MaxSeries int64
	// MaxPartSize caps an immutable part's size.
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

// Downsample is the per-tenant downsampling policy (DESIGN.md §10): a merge-time
// rollup reusing the same background-merge engine. Optional and per-tenant.
type Downsample struct {
	// TODO(M3): rollup definitions and merge-time hooks.
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
