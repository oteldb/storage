package storage

import (
	"time"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/encoding"
	"github.com/oteldb/storage/reliability"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/tenant"
)

// Options configures a [Storage]. Use [WithX] functional options to override
// defaults; [Open] applies them. The zero value is not valid — call [Open] (or
// [InMemory]) to construct a [Storage].
type Options struct {
	// Backend is the storage backend (memory/file/s3). If nil, [Open] defaults to
	// [backend.Memory] — the in-memory, ephemeral engine (DESIGN.md §5).
	Backend backend.Backend

	// Cluster is the cluster configuration. nil ⇒ single-node (the cluster layer
	// is absent, DESIGN.md §3, §11). Single-node users ignore L0 entirely.
	Cluster *cluster.Config

	// Tenant derives a record's tenant id from its Resource and Scope (so one OTLP
	// batch may fan out to many tenants). If nil, every record routes to "default".
	Tenant func(signal.Resource, signal.Scope) signal.TenantID

	// Tenancy resolves a tenant id to limits, retention, downsampling, and routing.
	// If nil, a permissive default resolver is used.
	Tenancy tenant.Resolver

	// Encoding is the default codec-chain profile per column kind (DESIGN.md §6).
	// If nil, a sensible default is used.
	Encoding encoding.Profile

	// Durability is the WAL + flush policy. [DurabilityEphemeral] disables both —
	// the in-memory engine: the head answers all queries and "flushed" parts are
	// kept in RAM and dropped on [Storage.Close] (DESIGN.md §5).
	Durability Durability

	// WALDir is the WAL directory for the file backend with durability enabled.
	// Ignored when [Durability] is [DurabilityEphemeral].
	WALDir string

	// WALSync is the WAL fsync policy (default [WALSyncNone]). Ignored without a WAL.
	WALSync WALSyncMode

	// WALSyncInterval is the background fsync period when [WALSync] is [WALSyncInterval]. Zero ⇒
	// default. (Resolved into a duration internally.)
	WALSyncInterval int64 // nanoseconds

	// FlushThresholdBytes is the head size at which a flush to an immutable part is
	// triggered. Zero ⇒ default.
	FlushThresholdBytes int64

	// ReadCacheBytes enables an in-memory LRU cache over backend read objects (immutable part
	// columns/manifests/marks/index), sized to this many bytes. It targets the cold tier
	// (file/S3), where a part is otherwise re-read on every query; recommended for those
	// deployments. Zero disables it, and it is always skipped for an ephemeral (in-memory)
	// backend, which is already RAM. See [backend.Cached].
	ReadCacheBytes int64

	// DecodeCacheBytes enables a per-tenant LRU cache of *decoded* part columns, sized to this many
	// bytes. It eliminates the column re-decode that the read cache cannot — a hit returns the
	// already-decoded arrays — so it helps every backend (a decode is CPU even when the read is
	// RAM-fast). With it, a fetch also prefetches its parts' decodes concurrently. Zero disables it.
	DecodeCacheBytes int64

	// AggregateStats writes a per-series aggregate sidecar (count/sum/min/max) alongside each metric
	// part, so [Storage.AggregateMetrics] answers a range-covering aggregate without decoding the
	// value column — returning one number per series instead of every sample. It costs a little
	// storage per series; off by default. AggregateMetrics works without it (by decoding).
	AggregateStats bool

	// FlushInterval is the max age of unflushed head data. Zero ⇒ default.
	// (Resolved into a duration internally to keep the API import-light.)
	FlushInterval int64 // nanoseconds

	// OOOWindow is the out-of-order ingestion window in nanoseconds (DESIGN.md §8).
	// Samples older than (newest - OOOWindow) are rejected (counted). Zero ⇒ default.
	OOOWindow int64 // nanoseconds

	// MaintenanceConcurrency caps how many engines the background maintenance loop flushes/merges
	// (and fsyncs) concurrently. Engines are independent per-tenant/per-signal shards, so this
	// bounds the parallel compaction fan-out against the backend. Zero ⇒ a CPU-derived default.
	MaintenanceConcurrency int

	// QuerySplitInterval, when > 0, makes [Storage.Fetcher] split a query's time window
	// into aligned sub-windows of this width (nanoseconds), fetched concurrently and merged
	// (the query-frontend "split by interval" technique). Zero ⇒ no splitting.
	QuerySplitInterval int64 // nanoseconds

	// QueryCacheEntries, when > 0, gives [Storage.Fetcher] a shared bounded-LRU results
	// cache of this many entries. Only fully-pushable (serializable-equality) requests over
	// an explicit tenant set are cached. Zero ⇒ no cache.
	QueryCacheEntries int

	// QueryCacheFreshness is the recent-window guard for the results cache, in nanoseconds: a
	// query whose window ends within this of now is not cached (new samples may still land in
	// it), so only settled/historical windows are memoized. 0 ⇒ no guard (cache every eligible
	// request). Ignored when QueryCacheEntries is 0.
	QueryCacheFreshness int64 // nanoseconds

	// Observability is injected, never owned (DESIGN.md §16): the library logs, traces, and meters
	// through the embedder's handles and is a zero-overhead no-op when they are unset. The library
	// imports only the OTel API; the embedder owns the SDK, sampling, and exporters.
	//
	// Logger is the zap logger (nil ⇒ a no-op logger). TracerProvider supplies OTel spans (nil ⇒
	// the OTel noop tracer). MeterProvider supplies OTel metrics, including the mandatory admission
	// meta-metrics (nil ⇒ the OTel noop meter).
	Logger         *zap.Logger
	TracerProvider trace.TracerProvider
	MeterProvider  metric.MeterProvider

	// Retry tunes how the library survives unreliable transports: per-attempt timeouts, bounded
	// retries, and hedged (opportunistic concurrent) reads across cluster replicas. The zero value
	// selects [reliability.Default] (a mild, LAN-safe profile); set [reliability.LossyEnvironment]
	// (or a custom config) for networks that drop/stall/30s-timeout requests. It governs the cluster
	// RPCs; the S3 backend is configured independently where it is constructed.
	Retry reliability.RetryConfig
}

// retryConfig returns the configured retry profile, defaulting to [reliability.Default] when unset.
func (o *Options) retryConfig() reliability.RetryConfig {
	if !o.Retry.Enabled() {
		return reliability.Default()
	}

	return o.Retry
}

// Durability is the WAL + flush policy (DESIGN.md §5, §8).
type Durability uint8

const (
	// DurabilityDefault is the default durability for a durable backend (WAL on,
	// flush to backend). For the in-memory engine use [DurabilityEphemeral].
	DurabilityDefault Durability = iota
	// DurabilityEphemeral disables the WAL and durable flush: the head answers all
	// queries and "flushed" parts live in RAM, dropped on [Storage.Close]. This is
	// the in-memory engine and the default in tests (DESIGN.md §5).
	DurabilityEphemeral
)

// WALSyncMode is the WAL fsync policy (the durability/throughput trade-off). It applies only when a
// WAL is configured ([Options.WALDir] on a durable backend).
type WALSyncMode uint8

const (
	// WALSyncNone (default) lets WAL writes settle in the OS page cache: they survive a process
	// crash (recovery replays them) but not necessarily a power loss. Fastest.
	WALSyncNone WALSyncMode = iota
	// WALSyncAlways fsyncs after every WAL record — power-loss durable, slowest.
	WALSyncAlways
	// WALSyncInterval fsyncs every engine's WAL in the background every [Options.WALSyncInterval]:
	// a bounded power-loss window at a fraction of the cost of WALSyncAlways.
	WALSyncInterval
)

// Option is a functional option applied to [Options] by [Open]/[InMemory].
type Option func(*Options)

// WithBackend sets the storage backend.
func WithBackend(b backend.Backend) Option { return func(o *Options) { o.Backend = b } }

// WithCluster sets the cluster configuration. nil ⇒ single-node.
func WithCluster(c *cluster.Config) Option { return func(o *Options) { o.Cluster = c } }

// WithTenancy sets the tenant policy resolver.
func WithTenancy(r tenant.Resolver) Option { return func(o *Options) { o.Tenancy = r } }

// WithTenant sets the record→tenant routing callback (resource/scope → tenant id).
func WithTenant(fn func(signal.Resource, signal.Scope) signal.TenantID) Option {
	return func(o *Options) { o.Tenant = fn }
}

// WithEncoding sets the default encoding profile.
func WithEncoding(p encoding.Profile) Option { return func(o *Options) { o.Encoding = p } }

// WithDurability sets the WAL + flush policy.
func WithDurability(d Durability) Option { return func(o *Options) { o.Durability = d } }

// WithWALDir sets the WAL directory (file backend + durability).
func WithWALDir(dir string) Option { return func(o *Options) { o.WALDir = dir } }

// WithWALSync sets the WAL fsync policy ([WALSyncNone]/[WALSyncAlways]/[WALSyncInterval]).
func WithWALSync(m WALSyncMode) Option { return func(o *Options) { o.WALSync = m } }

// WithWALSyncInterval selects background fsync ([WALSyncInterval]) every d.
func WithWALSyncInterval(d time.Duration) Option {
	return func(o *Options) { o.WALSync = WALSyncInterval; o.WALSyncInterval = int64(d) }
}

// defaultWALSyncInterval is the background fsync period when [WALSyncInterval] is selected without an
// explicit interval.
const defaultWALSyncInterval = 200 * time.Millisecond

// walSyncInterval returns the background WAL fsync period, or 0 when background syncing is off (no
// WAL, or a non-interval sync mode).
func (o *Options) walSyncInterval() time.Duration {
	if o.WALSync != WALSyncInterval || o.WALDir == "" {
		return 0
	}

	if o.WALSyncInterval > 0 {
		return time.Duration(o.WALSyncInterval)
	}

	return defaultWALSyncInterval
}

// WithFlushThresholdBytes sets the head flush size threshold.
func WithFlushThresholdBytes(n int64) Option { return func(o *Options) { o.FlushThresholdBytes = n } }

// WithReadCache enables an in-memory LRU object cache over the backend, sized to maxBytes (the
// object-store read cache for the cold tier). Skipped for an ephemeral backend. See [Options.ReadCacheBytes].
func WithReadCache(maxBytes int64) Option { return func(o *Options) { o.ReadCacheBytes = maxBytes } }

// WithDecodeCache enables a per-tenant LRU cache of decoded part columns, sized to maxBytes, plus
// concurrent prefetch of a fetch's parts. See [Options.DecodeCacheBytes].
func WithDecodeCache(maxBytes int64) Option {
	return func(o *Options) { o.DecodeCacheBytes = maxBytes }
}

// WithAggregateStats writes the per-series aggregate sidecar that lets [Storage.AggregateMetrics]
// answer range-covering aggregates without decoding. See [Options.AggregateStats].
func WithAggregateStats() Option { return func(o *Options) { o.AggregateStats = true } }

// WithFlushInterval sets the head flush time interval in nanoseconds.
func WithFlushInterval(ns int64) Option { return func(o *Options) { o.FlushInterval = ns } }

// WithOOOWindow sets the out-of-order ingestion window in nanoseconds.
func WithOOOWindow(ns int64) Option { return func(o *Options) { o.OOOWindow = ns } }

// WithMaintenanceConcurrency caps the background maintenance loop's parallel flush/merge fan-out
// across engines. Zero ⇒ a CPU-derived default.
func WithMaintenanceConcurrency(n int) Option {
	return func(o *Options) { o.MaintenanceConcurrency = n }
}

// WithLogger sets the zap logger the library logs through (DESIGN.md §16). nil ⇒ a no-op logger.
func WithLogger(l *zap.Logger) Option { return func(o *Options) { o.Logger = l } }

// WithTracerProvider sets the OTel tracer provider for the library's spans. nil ⇒ the noop tracer.
func WithTracerProvider(tp trace.TracerProvider) Option {
	return func(o *Options) { o.TracerProvider = tp }
}

// WithMeterProvider sets the OTel meter provider for the library's metrics (including the admission
// meta-metrics). nil ⇒ the noop meter.
func WithMeterProvider(mp metric.MeterProvider) Option {
	return func(o *Options) { o.MeterProvider = mp }
}

// WithRetry sets the transport reliability profile (per-attempt timeouts, bounded retries, hedged
// reads). Use [reliability.LossyEnvironment] in noisy networks; the zero config keeps the mild
// [reliability.Default].
func WithRetry(c reliability.RetryConfig) Option { return func(o *Options) { o.Retry = c } }

// WithQuerySplitInterval enables split-by-interval on [Storage.Fetcher]: a query window is
// divided into aligned sub-windows of ns nanoseconds, fetched concurrently and merged. ns ≤ 0
// disables splitting.
func WithQuerySplitInterval(ns int64) Option {
	return func(o *Options) { o.QuerySplitInterval = ns }
}

// WithQueryCache enables a shared bounded-LRU results cache on [Storage.Fetcher] holding at most
// maxEntries results. maxEntries ≤ 0 disables the cache. Only fully-pushable equality requests
// over an explicit tenant set are cached, and entries do not auto-invalidate.
func WithQueryCache(maxEntries int) Option {
	return func(o *Options) { o.QueryCacheEntries = maxEntries }
}

// WithQueryCacheFreshness sets the results-cache recent-window guard in nanoseconds: a query whose
// window ends within ns of now is not cached, so only settled windows are memoized. ns ≤ 0
// disables the guard. Has effect only with the cache enabled (see [WithQueryCache]).
func WithQueryCacheFreshness(ns int64) Option {
	return func(o *Options) { o.QueryCacheFreshness = ns }
}

// ErrClosed is returned by [Storage] methods after [Storage.Close].
var ErrClosed = errors.New("storage: closed")

// ErrNotEphemeral is returned by [Storage.Reset] when the backend is durable: Reset wipes
// all ingested data and is only permitted on an ephemeral (in-memory) store.
var ErrNotEphemeral = errors.New("storage: reset requires an ephemeral backend")

// errOptionInvalid is returned by [Open] for an invalid option combination.
func errOptionInvalid(reason string) error {
	return errors.Errorf("storage: invalid options: %s", reason)
}

// validate checks the option combination before construction.
func (o *Options) validate() error {
	if o.Durability == DurabilityEphemeral && o.WALDir != "" {
		return errOptionInvalid("WALDir must be empty when Durability is Ephemeral")
	}
	return nil
}

// applyDefaults fills nil/zero fields with the documented defaults.
func (o *Options) applyDefaults() {
	if o.Backend == nil {
		o.Backend = backend.Memory()
	}
	if o.Durability == DurabilityDefault && o.WALDir == "" && o.Backend.IsEphemeral() {
		// An ephemeral backend (memory) with no explicit durability choice and no
		// WAL dir ⇒ the in-memory engine.
		o.Durability = DurabilityEphemeral
	}
}

// ensure errors import is used even if validation grows later.
var _ = errors.Is
