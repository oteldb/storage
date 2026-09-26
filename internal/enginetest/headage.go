package enginetest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
)

// headAgeTracksFlushLag pins the one wall clock the head keeps: it starts when the head takes its
// first bytes and ends when a flush drains them, the flush lag no data-time field can report.
func headAgeTracksFlushLag(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	e := k.open(t, backend.Memory())

	require.Zero(t, e.Stats().HeadAge, "an empty head has no age")

	e.Append(t, api(1, 1))
	assert.Positive(t, e.Stats().HeadAge, "the head is accumulating")

	require.NoError(t, e.Flush(ctx))
	assert.Zero(t, e.Stats().HeadAge, "the flush drained it")

	e.Append(t, api(2, 2))
	assert.Positive(t, e.Stats().HeadAge, "the next head starts its own clock")
}

// mergeShapeReportsBytes covers the part-size gauge's input: a part count says nothing about whether
// the parts are small enough to be worth merging.
func mergeShapeReportsBytes(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	e := k.open(t, backend.Memory())

	require.Zero(t, e.MergeShape(0).Bytes, "no parts, no bytes")

	e.Append(t, api(1, 1))
	require.NoError(t, e.Flush(ctx))

	sh := e.MergeShape(0)
	require.Equal(t, 1, sh.Parts)
	assert.Positive(t, sh.Bytes, "the flushed part reports what it occupies")
}

// mergeShapeCountsRetentionWork pins the candidate counts to the merge the policy will actually run:
// a lone part fits its bucket and has no ladder work, yet the retention the next merge applies
// rewrites it when partly expired and drops it when wholly expired.
func mergeShapeCountsRetentionWork(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	e := k.open(t, backend.Memory())

	e.Append(t, api(10, 1), api(30, 2))
	require.NoError(t, e.Flush(ctx))

	sh := e.MergeShape(0)
	assert.Zero(t, sh.Candidates, "without retention a lone part is at rest")
	assert.Zero(t, sh.ForceCandidates)

	sh = e.MergeShape(20)
	assert.Equal(t, 1, sh.Candidates, "retention rewrites the partly expired part")
	assert.Equal(t, 1, sh.ForceCandidates)

	sh = e.MergeShape(40)
	assert.Equal(t, 1, sh.Candidates, "retention drops the wholly expired part")
	assert.Equal(t, 1, sh.ForceCandidates)

	require.NoError(t, e.Merge(ctx, 20))
	assert.Equal(t, []Row{api(30, 2)}, rows(t, e, apiStream))
	assert.Zero(t, e.MergeShape(20).Candidates, "the rewritten part holds nothing retention drops")
}
