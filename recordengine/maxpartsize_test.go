package recordengine_test

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/signal"
)

// splitEngine returns an engine whose parts hold ~4 rows each (recordRowBytes ≈ 1 KiB per row).
func splitEngine(t *testing.T, maxPartBytes int64) *recordengine.Engine {
	t.Helper()

	return recordengine.New(recordengine.Config{
		Schema: testSchema, Backend: backend.Memory(), Prefix: "t/split", MaxPartBytes: maxPartBytes,
	})
}

// splitRecs is n records across two streams' worth of distinct bodies/ids, so a split part set
// exercises per-part blooms and record keys rather than one constant column.
func splitRecs(n int) []rrec {
	out := make([]rrec, 0, n)
	for i := range n {
		out = append(out, rrec{
			ts:   int64(i + 1),
			sev:  int64(i % 24),
			body: fmt.Sprintf("body-%02d needle-%02d", i, i),
			id:   fmt.Sprintf("%032x", i),
			attr: [2]string{"idx", strconv.Itoa(i)},
		})
	}

	return out
}

func TestMaxPartSizeSplitsFlush(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := splitEngine(t, 4<<10) // ~4 rows per part

	const n = 20
	recs := splitRecs(n)
	ingest(t, e, mkBatch("api", recs...))
	require.NoError(t, e.Flush(ctx))

	assert.Greater(t, e.PartCount(), 1, "flush split the head into multiple parts under MaxPartBytes")

	// Every record is still readable across the split, in ts order, with its columns intact.
	got := fetchAll(t, e, req("api"))
	require.NotEmpty(t, got)

	var (
		ts     []int64
		bodies []string
	)

	for _, b := range got {
		ts = append(ts, b.Timestamps...)

		col, ok := b.Column("body")
		require.True(t, ok)

		for _, v := range col.Bytes {
			bodies = append(bodies, string(v))
		}
	}

	require.Len(t, ts, n)
	assert.Equal(t, recs[0].ts, ts[0])
	assert.Equal(t, recs[n-1].ts, ts[n-1])
	assert.Equal(t, recs[7].body, bodies[7])
}

func TestMaxPartSizeUnlimitedSinglePart(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := splitEngine(t, 0) // unlimited ⇒ one part per flush, whatever its size

	ingest(t, e, mkBatch("api", splitRecs(50)...))
	require.NoError(t, e.Flush(ctx))

	assert.Equal(t, 1, e.PartCount())
}

// TestMaxPartSizeSplitPartsPrune checks that each part of a split flush carries its own sidecars:
// an equality lookup must find the record whichever part it landed in, and must not be pruned away
// by another part's bloom.
func TestMaxPartSizeSplitPartsPrune(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := splitEngine(t, 4<<10)

	const n = 20
	recs := splitRecs(n)
	ingest(t, e, mkBatch("api", recs...))
	require.NoError(t, e.Flush(ctx))
	require.Greater(t, e.PartCount(), 1)

	for _, want := range []int{0, 9, n - 1} { // first, middle and last part
		id := []byte(recs[want].id)
		cond := fetch.Condition{
			Column: "id",
			Match:  func(v signal.Value) bool { return bytes.Equal(v.Str(), id) },
			Equal:  &fetch.EqualMatcher{Name: "id", Value: string(id)},
		}

		got := fetchAll(t, e, req("api", cond))

		rows := 0
		for _, b := range got {
			rows += len(b.Timestamps)
		}

		assert.Equal(t, 1, rows, "record %d found exactly once across the split parts", want)
	}
}
