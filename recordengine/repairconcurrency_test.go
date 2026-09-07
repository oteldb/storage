package recordengine_test

import (
	"context"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/recordengine"
)

// TestRepairConcurrentMergesFetchOnce pins the single-flight: two maintenance passes overlapping on
// one engine — an operator's MaintainNow racing the background loop — must not each copy the same
// part from a peer, nor each count the copy. The gated fetcher holds the first pass inside the peer
// I/O until the second has run as far as it can, which is where the duplicate used to be issued.
func TestRepairConcurrentMergesFetchOnce(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		be, peer := backend.Memory(), backend.Memory()

		f := &fakeFetcher{}
		e := newRepairEngine(t, be, f)

		ingest(t, e, mkBatch("api", rrec{ts: 100, body: "p1"}))
		require.NoError(t, e.Flush(ctx))
		ingest(t, e, mkBatch("api", rrec{ts: 200, body: "p2"}))
		require.NoError(t, e.Flush(ctx))

		parts := e.PartPrefixes()
		require.Len(t, parts, 2)
		lost := parts[0]

		copyObjects(t, be, peer, lost)
		dropObjects(t, be, lost)
		e.LosePart(lost, bucketindex.Interval{Min: 1, Max: 1})

		entered := make(chan struct{}, 2)
		release := make(chan struct{})

		f.answer = func(w bucketindex.Want) (bucketindex.Entry, bucketindex.WantOutcome, error) {
			entered <- struct{}{}
			<-release
			copyObjects(t, peer, be, w.Prefix)

			return bucketindex.Entry{
				Prefix: w.Prefix, MinTime: 100, MaxTime: 100, Blocks: w.Blocks,
			}, bucketindex.WantSatisfied, nil
		}

		errs := make(chan error, 2)
		go func() { errs <- e.Merge(ctx, 0) }()

		<-entered // the first pass is inside the peer fetch

		go func() { errs <- e.Merge(ctx, 0) }()

		synctest.Wait() // the second pass has run as far as it can

		close(release)

		require.NoError(t, <-errs)
		require.NoError(t, <-errs)

		assert.Equal(t, []string{lost}, f.asks(), "the wanted part is copied from a peer once")
		assert.Equal(t, 1, f.fetchCalls())
		assert.Equal(t, recordengine.RepairStats{Fetched: 1}, e.RepairStats(),
			"one published part, one counted fetch and nothing else")
		assert.Empty(t, e.WantPrefixes())
		assert.Empty(t, committedIndex(t, be).Wanted)
	})
}
