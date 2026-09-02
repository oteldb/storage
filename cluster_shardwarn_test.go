package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/oteldb/storage/backend"
)

const singleShardMsg = "cluster is running one shard per tenant: writes cannot spread"

// A multi-node cluster left at the default ingests every tenant through one primary however many
// replicas RF asks for. Nothing else reports that: the ring is healthy, RF is satisfied, and the
// only symptom is that one node does all the accepting. The advisory is the sole signal, so it is
// worth a test.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestSingleShardWarning(t *testing.T) {
	endpoint := startEtcd(t)
	ctx := context.Background()

	core, logs := observer.New(zap.WarnLevel)

	nodes := map[string]*Storage{
		"node-a": openClusterNodeWith(t, endpoint, "node-a", backend.Memory(), WithLogger(zap.New(core))),
		"node-b": openClusterNodeWith(t, endpoint, "node-b", backend.Memory()),
	}
	awaitMembership(t, nodes)

	a := nodes["node-a"]

	// Driven through a maintenance cycle rather than by calling the advisory directly, so the test
	// also fails if it stops being wired in.
	a.maintain(ctx)
	require.Equal(t, 1, logs.FilterMessage(singleShardMsg).Len(), "a two-node single-shard cluster is warned")

	// The condition cannot resolve without re-keying every shard, so repeating it each maintenance
	// cycle would be nagging about something that cannot be fixed in place.
	for range 5 {
		a.maintain(ctx)
	}

	assert.Equal(t, 1, logs.FilterMessage(singleShardMsg).Len(), "and warned exactly once")

	entry := logs.FilterMessage(singleShardMsg).All()[0].ContextMap()
	assert.Equal(t, int64(2), entry["nodes"])
	assert.Equal(t, int64(0), entry["shards_per_tenant"], "the configured value, not the clamped one")
}

// A single-node deployment is a legitimate configuration, not a misconfiguration, and a node that
// has not yet seen its peers is indistinguishable from one — so neither is warned.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestSingleShardWarningSilentOnOneNode(t *testing.T) {
	endpoint := startEtcd(t)

	core, logs := observer.New(zap.WarnLevel)
	s := openClusterNodeWith(t, endpoint, "solo", backend.Memory(), WithLogger(zap.New(core)))

	require.Eventually(t, func() bool { return ringSize(s) == 1 }, 10*time.Second, 50*time.Millisecond,
		"the node registers itself")

	s.warnSingleShard(context.Background())
	assert.Zero(t, logs.FilterMessage(singleShardMsg).Len(), "one node cannot spread writes anyway")
}

// Sharding is the fix the advisory names, so a cluster that took it must go quiet.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestSingleShardWarningSilentWhenSharded(t *testing.T) {
	endpoint := startEtcd(t)

	core, logs := observer.New(zap.WarnLevel)

	nodes := map[string]*Storage{
		"node-a": openClusterNodeSharded(t, endpoint, "node-a", 8, WithLogger(zap.New(core))),
		"node-b": openClusterNodeSharded(t, endpoint, "node-b", 8),
	}
	awaitMembership(t, nodes)

	nodes["node-a"].warnSingleShard(context.Background())
	assert.Zero(t, logs.FilterMessage(singleShardMsg).Len(), "a sharded tenant spreads across the ring")
}
