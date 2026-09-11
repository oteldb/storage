package recordengine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/signal"
)

// TestRefreshReplicaReplacesAChangedHandle: a reload keeps a handle only while its entry is unchanged;
// otherwise it opens a fresh one and leaves the published handle exactly as its readers saw it.
func TestRefreshReplicaReplacesAChangedHandle(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cfg := Config{
		Schema:  NewSchema(Column{Name: "severity", Kind: KindInt64, Codec: chunk.CodecT64}),
		Backend: backend.Memory(),
		Prefix:  "t/recs",
	}

	owner := New(cfg)
	series := signal.Series{Resource: signal.Resource{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte("svc"))},
	)}}

	_, err := owner.AppendBatch(&Batch{
		Stream: series.Hash(), Identity: func() signal.Series { return series },
		Ts: []int64{1000, 2000, 3000}, Ints: [][]int64{{1, 2, 3}},
	}, AppendLimits{})
	require.NoError(t, err)
	require.NoError(t, owner.Flush(ctx))

	replica := New(cfg)
	require.NoError(t, replica.RefreshReplica(ctx))
	require.Len(t, replica.parts, 1)

	published := replica.parts[0]

	require.NoError(t, replica.RefreshReplica(ctx))
	require.Same(t, published, replica.parts[0], "an unchanged entry reuses the handle")

	ix, version, err := bucketindex.LoadVersioned(ctx, cfg.Backend, replica.indexKey())
	require.NoError(t, err)
	require.Len(t, ix.Entries, 1)

	minTime, maxTime := ix.Entries[0].MinTime, ix.Entries[0].MaxTime
	ix.Entries[0].MaxTime++

	_, err = ix.Save(ctx, cfg.Backend, replica.indexKey(), version)
	require.NoError(t, err)

	require.NoError(t, replica.RefreshReplica(ctx))
	require.Len(t, replica.parts, 1)

	fresh := replica.parts[0]
	assert.NotSame(t, published, fresh)
	assert.Equal(t, maxTime+1, fresh.maxTime)
	assert.Equal(t, [2]int64{minTime, maxTime}, [2]int64{published.minTime, published.maxTime},
		"the published handle is never written")
	assert.False(t, replica.identityDirty, "a replaced handle is not a dropped part")
}
