package storage

import (
	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/encoding"
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

	// FlushThresholdBytes is the head size at which a flush to an immutable part is
	// triggered. Zero ⇒ default.
	FlushThresholdBytes int64

	// FlushInterval is the max age of unflushed head data. Zero ⇒ default.
	// (Resolved into a duration internally to keep the API import-light.)
	FlushInterval int64 // nanoseconds

	// OOOWindow is the out-of-order ingestion window in nanoseconds (DESIGN.md §8).
	// Samples older than (newest - OOOWindow) are rejected (counted). Zero ⇒ default.
	OOOWindow int64 // nanoseconds
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

// WithFlushThresholdBytes sets the head flush size threshold.
func WithFlushThresholdBytes(n int64) Option { return func(o *Options) { o.FlushThresholdBytes = n } }

// WithFlushInterval sets the head flush time interval in nanoseconds.
func WithFlushInterval(ns int64) Option { return func(o *Options) { o.FlushInterval = ns } }

// WithOOOWindow sets the out-of-order ingestion window in nanoseconds.
func WithOOOWindow(ns int64) Option { return func(o *Options) { o.OOOWindow = ns } }

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
