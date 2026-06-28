package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/encoding"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/tenant"
)

func TestOptionsApply(t *testing.T) {
	t.Parallel()

	var o Options
	tenantFn := func(signal.Resource, signal.Scope) signal.TenantID { return "x" }
	for _, opt := range []Option{
		WithBackend(backend.Memory()),
		WithCluster(&cluster.Config{}),
		WithTenancy(tenant.Default()),
		WithTenant(tenantFn),
		WithEncoding(encoding.Profile{}),
		WithDurability(DurabilityEphemeral),
		WithWALDir("/wal"),
		WithFlushThresholdBytes(11),
		WithFlushInterval(22),
		WithOOOWindow(33),
	} {
		opt(&o)
	}

	assert.NotNil(t, o.Backend)
	assert.NotNil(t, o.Cluster)
	assert.NotNil(t, o.Tenancy)
	assert.NotNil(t, o.Tenant)
	assert.Equal(t, DurabilityEphemeral, o.Durability)
	assert.Equal(t, "/wal", o.WALDir)
	assert.Equal(t, int64(11), o.FlushThresholdBytes)
	assert.Equal(t, int64(22), o.FlushInterval)
	assert.Equal(t, int64(33), o.OOOWindow)
}

func TestValidateRejectsEphemeralWithWALDir(t *testing.T) {
	t.Parallel()

	_, err := Open(context.Background(), Options{Durability: DurabilityEphemeral, WALDir: "/wal"})
	require.Error(t, err)
}

func TestApplyDefaults(t *testing.T) {
	t.Parallel()

	// No backend, no durability ⇒ memory backend + ephemeral.
	o := Options{}
	o.applyDefaults()
	assert.NotNil(t, o.Backend)
	assert.True(t, o.Backend.IsEphemeral())
	assert.Equal(t, DurabilityEphemeral, o.Durability)
	assert.Zero(t, o.FlushInterval, "the ephemeral engine never flushes, so no default interval")
}

// TestApplyDefaultsFlushInterval covers issue 23: a durable store left at the zero FlushInterval must
// default to a real interval (not silently disable all flushing), while a negative value opts out.
func TestApplyDefaultsFlushInterval(t *testing.T) {
	t.Parallel()

	// Durable backend, no explicit interval ⇒ the maintenance loop runs at the default cadence.
	d := Options{Backend: durableBackend{backend.Memory()}}
	d.applyDefaults()
	require.NotEqual(t, DurabilityEphemeral, d.Durability)
	assert.Equal(t, int64(defaultFlushInterval), d.FlushInterval, "a durable store flushes by default")

	// A negative interval is the explicit opt-out (manual/Admin-driven maintenance) and is preserved.
	off := Options{Backend: durableBackend{backend.Memory()}, FlushInterval: -1}
	off.applyDefaults()
	assert.Equal(t, int64(-1), off.FlushInterval, "a negative interval disables the background loop")

	// An explicit positive interval is honored verbatim.
	set := Options{Backend: durableBackend{backend.Memory()}, FlushInterval: 22}
	set.applyDefaults()
	assert.Equal(t, int64(22), set.FlushInterval)
}
