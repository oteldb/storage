package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/cluster/etcd"
	"github.com/oteldb/storage/signal"
)

// A pressure flush is memory relief, not an ownership decision: writing a shard's parts is the
// compaction owner's job alone. These pin the cluster half of that — a node that cannot prove the
// shard is its own keeps the head instead of forking the shard's part sequence.

// pressureFlushCluster is a three-node log shard with no maintenance tick and a threshold nothing
// reaches on its own, so the only flush is the one a test asks for. It returns the shard's
// compaction owner and its replica, both holding an unflushed head over the threshold.
func pressureFlushCluster(t *testing.T) (c *readPolicyCluster, owner, replica string) {
	t.Helper()
	ctx := context.Background()

	c = newReadPolicyCluster(t, startEtcd(t), WithFlushInterval(int64(time.Hour)))
	c.write(t)

	owner, replica = c.compactionOwnerOf(t)

	// A second write refills both heads: c.write's maintenance passes drained them.
	_, err := c.nodes["node-a"].WriteLogs(ctx, logBatch("api", [3]any{300, 9, "third"}))
	require.NoError(t, err)

	for _, id := range []string{owner, replica} {
		eng, ok := c.nodes[id].lookupRecordEngine(signal.Log, c.shard)
		require.True(t, ok, "%s holds the shard", id)
		require.Eventually(t, func() bool { return eng.HeadBytes() > 0 },
			10*time.Second, 20*time.Millisecond, "%s buffers the second write", id)

		// Writes are done, so lowering the threshold now puts every head over it without arming
		// the write path's poke.
		c.nodes[id].opts.FlushThresholdBytes = 1
	}

	return c, owner, replica
}

// logShardState is a node's part count and buffered head bytes for the log shard.
func (c *readPolicyCluster) logShardState(t *testing.T, id string) (parts int, head int64) {
	t.Helper()

	eng, ok := c.nodes[id].lookupRecordEngine(signal.Log, c.shard)
	require.True(t, ok)

	return len(eng.Parts()), eng.HeadBytes()
}

// TestPressureFlushSkipsReplica: the size-triggered sweep sees every engine on the node, including
// the shards it only replicates. Flushing one of those forks the shard's part sequence — two nodes
// independently numbering parts for records the reads do not dedup.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestPressureFlushSkipsReplica(t *testing.T) {
	ctx := context.Background()

	c, owner, replica := pressureFlushCluster(t)
	t.Logf("compaction owner=%s replica=%s", owner, replica)

	partsBefore, headBefore := c.logShardState(t, replica)
	require.Positive(t, headBefore, "the replica has something a pressure flush could write")

	c.nodes[replica].flushPressured(ctx)

	partsAfter, headAfter := c.logShardState(t, replica)
	assert.Equal(t, partsBefore, partsAfter, "a replica writes no part for a shard it does not own")
	assert.Positive(t, headAfter, "and keeps the head, which the owner's part sync supersedes")
}

// TestPressureFlushSkipsFencedOwner: past its lease deadline a node can prove nothing about what it
// owns, so the head it is holding stays held — flushing it writes parts under a tenure that has
// ended, against a shard another node may already have taken.
//
//nolint:paralleltest // owns an embedded etcd; runs serially
func TestPressureFlushSkipsFencedOwner(t *testing.T) {
	ctx := context.Background()

	c, owner, _ := pressureFlushCluster(t)

	partsBefore, headBefore := c.logShardState(t, owner)
	require.Positive(t, headBefore)

	// Nothing sleeps: the fence deadline is last keep-alive + TTL − margin, compared against a
	// clock the node lets the test move.
	node := c.nodes[owner]
	node.cluster.membership.SetClock(func() time.Time { return time.Now().Add(2 * etcd.DefaultTTL) })

	t.Cleanup(func() { node.cluster.membership.SetClock(nil) })

	require.True(t, node.fenced(), "the owner is past its lease deadline")

	node.flushPressured(ctx)

	partsAfter, headAfter := c.logShardState(t, owner)
	assert.Equal(t, partsBefore, partsAfter, "a fenced node writes no part under a tenure that ended")
	assert.Positive(t, headAfter, "the unflushed head is kept, neither dropped nor flushed")
}
