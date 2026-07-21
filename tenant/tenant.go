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
	// series past the cap is shed (or routed to overflow — see MaxSeriesSoft); existing series
	// are unaffected. It is the hard ceiling. Zero ⇒ unlimited.
	MaxSeries int64
	// MaxSeriesSoft, when 0 < MaxSeriesSoft <= MaxSeries, is a soft cardinality budget: past it a
	// *new* series' samples are routed to a synthetic per-metric overflow series (keeping a tenant's
	// aggregates approximately correct under a spike) rather than shed, until the hard MaxSeries is
	// reached. Zero (or with no MaxSeries) ⇒ no soft budget: the cap is a hard reject at MaxSeries.
	// Metrics only (collapsing a log/trace stream would lose its rows). Note: cardinality here is
	// monotonic within an engine's lifetime, so once crossed the budget stays in overflow (no
	// hysteresis); see docs/design/cardinality-overflow.md.
	MaxSeriesSoft int64
	// MaxPartSize caps an immutable part's (approximate, uncompressed) size: flush and merge split
	// their output so no single part exceeds it. It is a structural cap fixed when a tenant's engine
	// is first created. Zero ⇒ unlimited (one part). Metrics only.
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

// Sampling is the per-tenant lossy admission policy (DESIGN §8a): when ingest exceeds the
// budget, keep a representative subset rather than reject, tagging each kept sample with a scale
// factor (the inverse keep-probability) so an embedder's count/sum/rate stays unbiased. It is
// off by default (lossless) — sampling only ever activates under an explicit budget, and only
// for metrics (dropping a log or span would break a stream or trace). Sampling is deterministic
// by (series, timestamp), so the same point is consistently kept or dropped.
type Sampling struct {
	// MaxRowsPerSecond is the per-tenant ingest budget. When the observed rate exceeds it, the
	// sampler keeps ~budget rows/second and scales the kept samples up to compensate. 0 ⇒ no
	// sampling (lossless: every sample is stored with a scale factor of 1).
	MaxRowsPerSecond int64
}

// Recompress is the per-tenant cold-data recompression policy: once a part's data is older than
// After, the merge engine rewrites it with a higher-ratio (Zstandard) profile, trading merge CPU
// for storage. It is decode-transparent (the reader keys off the recorded algorithm), so it costs
// nothing to read. Optional; a zero After disables it.
type Recompress struct {
	// After is the age past which a fully-cold part is recompressed at merge. Zero ⇒ disabled.
	After time.Duration
	// Level is the Zstandard level (1 fastest … 19 best ratio). Zero ⇒ the best-ratio default.
	Level int
}

// PrecisionTier makes a part's value column lossy once its data reaches a given age: a part fully
// older than After is re-encoded, at merge, retaining only Bits significant mantissa bits
// (scaled-decimal). Fewer bits ⇒ better compression, less accuracy. A tier with Bits == 0 or
// Bits ≥ 64 is lossless and ignored.
//
// Tiers coarsen old data (configure increasing After with decreasing Bits, e.g. 32 bits after
// 7d, 16 bits after 30d). A part takes the most aggressive tier whose After it has exceeded; data
// younger than every tier's After stays lossless. It rides the same background-merge engine — no
// separate subsystem — and is never worse than lossless Gorilla (the encoder keeps whichever is
// smaller), so a tier can only help size.
//
// Bits is significant mantissa bits of the value, not decimal places: ~16 bits keeps roughly 4–5
// significant decimal digits, ~24 bits ~7 digits. Counters and clean low-precision gauges are
// already near-lossless dense, so precision tiers mainly help noisy/high-entropy gauges where some
// accuracy on old data can be traded for size.
type PrecisionTier struct {
	// After is the age past which this tier applies (relative to now at merge time).
	After time.Duration
	// Bits is the significant mantissa bits to retain (1..63). 0 or ≥64 ⇒ lossless (ignored).
	Bits uint8
}

// Precision is the per-tenant lossy float-compression policy: age-tiered value-column precision
// applied at merge, so recent data stays lossless and only old data trades accuracy for size.
// Optional and per-tenant; an empty Tiers list keeps every part lossless.
type Precision struct {
	// Tiers are the age-banded precision budgets. Order does not matter; the engine assigns each
	// part the most aggressive applicable tier.
	Tiers []PrecisionTier
}

// ECScheme is a Reed-Solomon erasure-coding scheme for a tenant's flushed parts: each part
// object is split into Data shards plus Parity parity shards, spread across Data+Parity
// distinct owners; any Data of them reconstruct the object. It trades reconstruct-on-miss
// read cost for storage: (Data+Parity)/Data of the logical size instead of RF full copies
// (e.g. {4,2} survives 2 node losses at 1.5x). Applies only to flushed, immutable parts —
// the unflushed head is always full-copy replicated. A nil *ECScheme means full-copy.
type ECScheme struct {
	// Data is the number of data shards (k). Must be ≥ 1.
	Data int
	// Parity is the number of parity shards (m); the scheme tolerates Parity node losses.
	// Must be ≥ 1.
	Parity int
	// After is the age past which a fully-cold part is erasure-coded, at merge, in place of
	// full-copy replication — recent parts stay full-copy for fast local reads, and only cold
	// parts trade a reconstruct-on-read cost for the storage saving. It mirrors [Recompress.After]
	// and rides the same background-merge engine. Zero ⇒ EC every part (accept the read cost
	// everywhere for the cheapest storage).
	After time.Duration
}

// Durability is the per-tenant replication policy for the cluster layer: how many ring-owners
// hold a tenant's data and how flushed parts are stored across them. Zero value ⇒ the
// cluster-wide defaults (cluster.Config.RF, full-copy). Ignored in single-node mode.
type Durability struct {
	// RF is the replication factor: the number of ring-owners for the tenant's shards
	// (primary + RF-1 replicas). Zero ⇒ the cluster default; values are clamped to the
	// membership size at lookup time.
	RF int
	// EC, when non-nil, erasure-codes the tenant's flushed parts instead of storing RF
	// full copies. Nil ⇒ full-copy replication.
	EC *ECScheme
}

// Policy is the resolved policy for a tenant: limits, retention, downsampling, sampling,
// recompression, durability, and routing. It is the return value of [Resolver.Resolve].
type Policy struct {
	Limits     Limits
	Retention  Retention
	Downsample Downsample
	Sampling   Sampling
	Recompress Recompress
	Precision  Precision
	Durability Durability
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
