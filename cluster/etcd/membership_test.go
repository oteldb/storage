package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/cluster/etcd/etcdtest"
)

func TestMemberEncodeDecodeRoundTrip(t *testing.T) {
	t.Parallel()

	in := Member{ID: "node-7", Zone: "eu-1", Addr: "10.0.0.7:9000"}
	out, err := decodeMember(in.encode())
	require.NoError(t, err)
	assert.Equal(t, in, out)

	// The hierarchical failure-domain path round-trips too.
	hier := Member{ID: "node-7", Zone: "rack1", Addr: "10.0.0.7:9000", Domains: []string{"rack1", "srv3"}}
	out, err = decodeMember(hier.encode())
	require.NoError(t, err)
	assert.Equal(t, hier, out)

	// A member without domains decodes with a nil Domains (the old wire form stays valid).
	noDomains, err := decodeMember(in.encode())
	require.NoError(t, err)
	assert.Nil(t, noDomains.Domains)
}

func TestDecodeMemberRejectsGarbage(t *testing.T) {
	t.Parallel()

	_, err := decodeMember([]byte("not json"))
	require.Error(t, err)
}

func memberIDs(ms []Member) []string {
	out := make([]string, len(ms))
	for i, m := range ms {
		out[i] = m.ID
	}

	return out
}

//nolint:paralleltest // owns an embedded etcd; runs serially
func TestMembershipJoinWatchLeave(t *testing.T) {
	client := etcdtest.NewServer(t).Dial()
	ctx := context.Background()

	// Node A joins.
	a, err := Join(ctx, client, "/oteldb", Member{ID: "node-a", Zone: "z1", Addr: "a:1"}, 5*time.Second)
	require.NoError(t, err)
	assert.Equal(t, []string{"node-a"}, memberIDs(a.Members()))
	assert.Equal(t, 1, a.Ring().Len())

	// Node B joins; A's watch must see it and rebuild the ring.
	b, err := Join(ctx, client, "/oteldb", Member{ID: "node-b", Zone: "z2", Addr: "b:1"}, 5*time.Second)
	require.NoError(t, err)

	require.Eventually(t, func() bool { return a.Ring().Len() == 2 }, 5*time.Second, 25*time.Millisecond,
		"node A's ring picks up node B")
	assert.Equal(t, []string{"node-a", "node-b"}, memberIDs(a.Members()))

	// Both nodes place a key on the same owners (deterministic, shared membership).
	assert.Equal(t,
		a.Ring().Lookup([]byte("series-42"), 2),
		b.Ring().Lookup([]byte("series-42"), 2),
		"placement agrees across nodes")

	// Node B leaves (lease revoked on Close); A's ring shrinks back.
	require.NoError(t, b.Close(ctx))
	require.Eventually(t, func() bool { return a.Ring().Len() == 1 }, 5*time.Second, 25*time.Millisecond,
		"node A drops node B after it leaves")
	assert.Equal(t, []string{"node-a"}, memberIDs(a.Members()))

	require.NoError(t, a.Close(ctx))
}

func TestJoinRequiresID(t *testing.T) {
	t.Parallel()

	_, err := Join(context.Background(), nil, "/oteldb", Member{}, time.Second)
	require.Error(t, err)
}
