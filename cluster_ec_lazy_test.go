package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/signal"
)

func countingEngineOps(calls *int) engineOps {
	noop := func() error { return nil }

	return engineOps{
		flush: noop, merge: noop, refresh: noop,
		adopt: func([]bucketindex.Want) {},
		ecParts: func() []ecPartRef {
			*calls++

			return nil
		},
	}
}

// TestMaintainEcPartsLazy pins the snapshot as lazy: a tenant without EC never pays for the
// [engine.Engine.Parts] copy the EC callees would discard on their first statement.
func TestMaintainEcPartsLazy(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	owner := 0
	s.maintainOneEngine(ctx, "default", "/metrics/", nil, countingEngineOps(&owner))
	assert.Zero(t, owner, "owner branch must not snapshot parts without EC")

	replica := 0
	s.maintainOneEngine(ctx, "default", "/metrics/", map[signal.TenantID]struct{}{}, countingEngineOps(&replica))
	assert.Zero(t, replica, "replica branch must not snapshot parts without EC")
}

// TestMaintainEcPartsSharedSnapshot checks the other half: with EC configured, the owner branch's
// two callees share one snapshot instead of taking one each.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestMaintainEcPartsSharedSnapshot(t *testing.T) {
	endpoint := startEtcd(t)
	ctx := context.Background()

	s := openClusterNodeEC(t, endpoint, "node-a", 2, 1, 0)
	_, ok := s.ecSchemeFor("default")
	require.True(t, ok, "EC must apply for this node")

	calls := 0
	s.maintainOneEngine(ctx, "default", "/metrics/", map[signal.TenantID]struct{}{"default": {}}, countingEngineOps(&calls))
	assert.Equal(t, 1, calls, "one snapshot shared by convert and repair")
}
