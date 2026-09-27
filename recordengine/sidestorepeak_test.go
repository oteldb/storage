//go:build !race

package recordengine_test

import (
	"context"
	"crypto/rand"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/internal/heaptest"
	"github.com/oteldb/storage/recordengine"
)

// TestMergeSidecarUnionHoldsItsGrant: a side store's parts carry large sidecars, and every part a
// merge writes loads all of them, unions them and encodes the union back. The merge reserves that for
// each finish, and its heap, sampled at every object it writes, stays inside the grant.
//
//nolint:paralleltest // samples the process-wide heap
func TestMergeSidecarUnionHoldsItsGrant(t *testing.T) {
	ctx := context.Background()

	fb, err := file.New(t.TempDir())
	require.NoError(t, err)

	b := &heaptest.FileSampler{File: fb, Writes: true}
	e := recordengine.New(recordengine.Config{
		Schema: testSchema, Backend: b, Prefix: "t/recs", SideStore: newFakeSide(), MergeMemoryBytes: 8 << 20,
	})

	const (
		parts   = 4
		entries = 32 << 10
	)

	for p := range parts {
		side := make(map[uint64][]byte, entries)

		for i := range entries {
			v := make([]byte, 96)
			_, _ = rand.Read(v)
			side[uint64(p*entries+i)] = v
		}

		batch := mkBatch("api", rrec{ts: int64(p + 1), body: "x"})
		batch.Side = encodeSide(side)
		ingest(t, e, batch)
		require.NoError(t, e.Flush(ctx))
	}

	var grant int64

	defer recordengine.SetMergeResidentObserver(func(_, _, _, g int64) { grant = g })()

	heap := int64(heaptest.Resident(t, b, func() {
		require.NoError(t, e.MergeWith(ctx, recordengine.MergeOptions{Force: true}))
	}))

	runtime.KeepAlive(e)

	t.Logf("%d parts of %d sidecar entries: merge heap %.1f MiB, grant %.1f MiB",
		parts, entries, float64(heap)/(1<<20), float64(grant)/(1<<20))
	require.Len(t, e.PartPrefixes(), 1, "the merge must have run")
	assert.LessOrEqual(t, heap, grant, "the sidecar union outgrew the merge's grant")
}
