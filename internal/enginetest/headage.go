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

	require.Zero(t, e.MergeShape().Bytes, "no parts, no bytes")

	e.Append(t, api(1, 1))
	require.NoError(t, e.Flush(ctx))

	sh := e.MergeShape()
	require.Equal(t, 1, sh.Parts)
	assert.Positive(t, sh.Bytes, "the flushed part reports what it occupies")
}
