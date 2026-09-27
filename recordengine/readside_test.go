package recordengine_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/recordengine"
)

func TestReadSideWindow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	e := sideEngine(backend.Memory(), newFakeSide())

	for _, flush := range []struct {
		ts  int64
		ids []uint64
	}{{10, []uint64{1, 2}}, {20, []uint64{2, 3}}} {
		side := map[uint64][]byte{}
		for _, id := range flush.ids {
			side[id] = []byte{byte(id)}
		}

		b := mkBatch("api", rrec{ts: flush.ts, body: "x"})
		b.Side = encodeSide(side)
		ingest(t, e, b)
		require.NoError(t, e.Flush(ctx))
	}

	head := mkBatch("api", rrec{ts: 30, body: "z"})
	head.Side = encodeSide(map[uint64][]byte{9: []byte("h")})
	ingest(t, e, head)

	assert.Equal(t, []uint64{1, 2, 3, 9}, readSideIDs(t, e, 0, 0))
	assert.Equal(t, []uint64{1, 2, 9}, readSideIDs(t, e, 5, 15))
	assert.Equal(t, []uint64{2, 3, 9}, readSideIDs(t, e, 20, 25))
	assert.Equal(t, []uint64{9}, readSideIDs(t, e, 21, 29), "no part overlaps; the head always answers")

	rd := e.ReadSide(0, 0, func(recordengine.SideStore) {})
	defer rd.Release()

	require.Len(t, rd.Parts, 2)

	newest, err := rd.Parts[0].Load(ctx)
	require.NoError(t, err)

	got := map[uint64][]byte{}
	require.NoError(t, decodeSide(newest["table"], got))
	assert.Contains(t, got, uint64(3), "newest part first")
}

// TestReadSidePinsParts checks that a merge cannot reclaim a part a ReadSide still holds.
func TestReadSidePinsParts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	e := sideEngine(backend.Memory(), newFakeSide())

	for ts := range int64(2) {
		b := mkBatch("api", rrec{ts: ts + 1, body: "x"})
		b.Side = encodeSide(map[uint64][]byte{uint64(ts): []byte("a")})
		ingest(t, e, b)
		require.NoError(t, e.Flush(ctx))
	}

	rd := e.ReadSide(0, 0, func(recordengine.SideStore) {})
	require.Len(t, rd.Parts, 2)

	require.NoError(t, e.Merge(ctx, 0))
	require.Equal(t, 1, e.PartCount())

	for _, p := range rd.Parts {
		tables, err := p.Load(ctx)
		require.NoError(t, err)
		assert.Contains(t, tables, "table", "a pinned part keeps its sidecars")
	}

	rd.Release()
	rd.Release()
	require.NoError(t, e.Flush(ctx))

	for _, p := range rd.Parts {
		tables, err := p.Load(ctx)
		require.NoError(t, err)
		assert.Empty(t, tables, "released parts are reclaimed")
	}
}

func TestReadSideWithoutSideStore(t *testing.T) {
	t.Parallel()

	e := recordengine.New(recordengine.Config{Schema: testSchema, Backend: backend.Memory(), Prefix: "t/recs"})
	rd := e.ReadSide(0, 0, func(recordengine.SideStore) { t.Fatal("no side store to read") })
	defer rd.Release()

	assert.Empty(t, rd.Parts)
	assert.Nil(t, rd.Flushing)
}
