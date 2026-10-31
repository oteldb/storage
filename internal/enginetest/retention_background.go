package enginetest

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/backend/faultbackend"
	"github.com/oteldb/storage/backend/file"
)

func retentionCoalescesBackground(t *testing.T, k Kind) {
	t.Helper()
	for _, bc := range []backendtest.Case{backendtest.Memory(), backendtest.Dir("file", file.New)} {
		t.Run(bc.Name, func(t *testing.T) {
			ctx := t.Context()
			base := time.Unix(0, 0).UTC().Add(48 * time.Hour)
			now := base.Add(7 * 24 * time.Hour)
			be := bc.Open(t)
			e := k.Open(t, Config{Backend: be, Now: func() time.Time { return now }})
			ts := func(d time.Duration) int64 { return base.Add(d).UnixNano() }
			e.Append(t, api(ts(30*time.Second), 1), api(ts(40*time.Second), 2), api(ts(50*time.Second), 3),
				api(ts(time.Hour+2*time.Second), 4), api(ts(2*time.Hour), 5), api(ts(23*time.Hour), 6))
			require.NoError(t, e.Flush(ctx))
			changed, err := e.BackgroundMerge(ctx, ts(40*time.Second), false)
			require.NoError(t, err)
			require.True(t, changed)
			before := partIDs(e)
			now = now.Add(5 * time.Second)
			changed, err = e.BackgroundMerge(ctx, ts(45*time.Second), false)
			require.NoError(t, err)
			assert.False(t, changed, "a moving cutoff must not rewrite the same part every tick")
			assert.Equal(t, before, partIDs(e))
			require.NoError(t, e.Merge(ctx, ts(45*time.Second)))
			assert.NotEqual(t, before, partIDs(e), "explicit compaction bypasses the gate")
			before = partIDs(e)
			now = now.Add(time.Hour - time.Nanosecond)
			changed, err = e.BackgroundMerge(ctx, ts(time.Hour+5*time.Second), false)
			require.NoError(t, err)
			assert.False(t, changed)
			now = now.Add(time.Nanosecond)
			changed, err = e.BackgroundMerge(ctx, ts(time.Hour+5*time.Second), false)
			require.NoError(t, err)
			assert.True(t, changed, "the retention interval must expire")
			assert.NotEqual(t, before, partIDs(e))
			assert.Equal(t, []Row{api(ts(2*time.Hour), 5), api(ts(23*time.Hour), 6)}, rows(t, e, apiStream))
			changed, err = e.BackgroundMerge(ctx, ts(3*time.Hour), true)
			require.NoError(t, err)
			assert.True(t, changed, "Force bypasses the background interval")
			assert.Equal(t, []Row{api(ts(23*time.Hour), 6)}, rows(t, e, apiStream))
			changed, err = e.BackgroundMerge(ctx, ts(24*time.Hour), false)
			require.NoError(t, err)
			assert.True(t, changed, "whole-part expiry bypasses the gate")
			assert.Empty(t, e.Parts())
		})
	}
}

func retentionYieldsToOrdinaryCompaction(t *testing.T, k Kind) {
	t.Helper()
	ctx := context.Background()
	base := time.Unix(0, 0).UTC().Add(48 * time.Hour)
	now := base.Add(7 * 24 * time.Hour)
	be := backend.Memory()
	e := k.Open(t, Config{Backend: be, Now: func() time.Time { return now }})
	e.Append(t, api(base.UnixNano(), 1), api(base.Add(2*time.Second).UnixNano(), 2), api(base.Add(23*time.Hour).UnixNano(), 3))
	require.NoError(t, e.Flush(ctx))
	_, err := e.BackgroundMerge(ctx, base.Add(time.Second).UnixNano(), false)
	require.NoError(t, err)
	protected := partIDs(e)[0]
	for i := range 4 {
		e.Append(t, api(base.Add(48*time.Hour+time.Duration(i)*time.Second).UnixNano(), int64(i)))
		require.NoError(t, e.Flush(ctx))
	}
	now = now.Add(time.Second)
	changed, err := e.BackgroundMerge(ctx, base.Add(3*time.Second).UnixNano(), false)
	require.NoError(t, err)
	assert.True(t, changed)
	assert.Equal(t, 2, e.PartCount(), "ordinary backlog must compact while the boundary part is gated")
	assert.Contains(t, partIDs(e), protected)
	reloaded := k.Open(t, Config{Backend: be, Now: func() time.Time { return now }})
	require.NoError(t, reloaded.LoadParts(ctx))
	_, err = reloaded.BackgroundMerge(ctx, base.Add(2*time.Minute).UnixNano(), false)
	require.NoError(t, err)
	assert.NotContains(t, partIDs(reloaded), protected, "a restart permits one extra retention rewrite")
}

func retentionFailureDoesNotArmGate(t *testing.T, k Kind) {
	t.Helper()
	ctx := t.Context()
	now := time.Unix(1, 0)
	be := faultbackend.Wrap(backend.Memory())
	e := k.Open(t, Config{Backend: be, Now: func() time.Time { return now }})
	e.Append(t, api(100, 1), api(200, 2), api(300, 3))
	require.NoError(t, e.Flush(ctx))
	before := partIDs(e)
	rejectWrites(be, "/"+bucketindex.Object, errWriteRejected)
	_, err := e.BackgroundMerge(ctx, 150, false)
	require.Error(t, err)
	assert.Equal(t, before, partIDs(e))
	be.Reset()
	changed, err := e.BackgroundMerge(ctx, 175, false)
	require.NoError(t, err)
	assert.True(t, changed, "failed publication must leave retention retryable")
	assert.Equal(t, []Row{api(200, 2), api(300, 3)}, rows(t, e, apiStream))
}

func retentionDoesNotPullProtectedNeighbors(t *testing.T, k Kind) {
	t.Helper()
	ctx := t.Context()
	base := int64(48 * time.Hour)
	now := time.Unix(0, base).Add(7 * 24 * time.Hour)
	e := k.Open(t, Config{Backend: backend.Memory(), Now: func() time.Time { return now }})
	e.Append(t, api(base, 1), api(base+int64(2*time.Second), 2), api(base+int64(23*time.Hour), 3))
	require.NoError(t, e.Flush(ctx))
	_, err := e.BackgroundMerge(ctx, base+int64(time.Second), false)
	require.NoError(t, err)
	protected := partIDs(e)[0]
	e.Append(t, api(base+int64(time.Second), 4), api(base+int64(23*time.Hour), 5))
	require.NoError(t, e.Flush(ctx))
	now = now.Add(time.Second)
	changed, err := e.BackgroundMerge(ctx, base+int64(3*time.Second), false)
	require.NoError(t, err)
	assert.True(t, changed)
	assert.Equal(t, 2, e.PartCount())
	assert.Contains(t, partIDs(e), protected, "a new retention candidate must not rewrite its protected neighbor")
}

func retentionStampSurvivesOrdinaryMerge(t *testing.T, k Kind) {
	t.Helper()
	ctx := t.Context()
	now := time.Unix(1, 0)
	e := k.Open(t, Config{Backend: backend.Memory(), Now: func() time.Time { return now }})
	e.Append(t, api(100, 1), api(200, 2), api(300, 3))
	require.NoError(t, e.Flush(ctx))
	_, err := e.BackgroundMerge(ctx, 150, false)
	require.NoError(t, err)
	e.Append(t, api(400, 4), api(500, 5))
	require.NoError(t, e.Flush(ctx))
	changed, err := e.BackgroundMerge(ctx, 150, false)
	require.NoError(t, err)
	assert.True(t, changed)
	require.Equal(t, 1, e.PartCount())
	before := partIDs(e)
	now = now.Add(time.Second)
	changed, err = e.BackgroundMerge(ctx, 250, false)
	require.NoError(t, err)
	assert.False(t, changed, "ordinary compaction must preserve the retention stamp")
	assert.Equal(t, before, partIDs(e))
}
