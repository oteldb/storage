package engine_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/wal"
)

// apiSamples returns every timestamp the engine holds for the "api" job, over all time.
func apiSamples(t *testing.T, e *engine.Engine) []int64 {
	t.Helper()

	got := fetchAll(t, e, fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("job", "api")}})
	if len(got) == 0 {
		return nil
	}

	require.Len(t, got, 1)

	return got[0].Timestamps
}

// TestFlushFailureKeepsSamples verifies a flush that fails before publishing a part folds the
// detached head buffers back, so the samples are retried by the next flush instead of being
// stranded in the in-flight buffer (which the next flush overwrites — silent, permanent loss).
func TestFlushFailureKeepsSamples(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := &rejectWrites{Backend: backend.Memory()}
	e := engine.New(engine.Config{Backend: be, Prefix: "t/metrics"})
	api := mkSeries("job", "api")

	mustAppend(t, e, api, 100, 1.0)

	be.armed.Store(true)
	require.Error(t, e.Flush(ctx), "flush must fail while the backend rejects writes")
	be.armed.Store(false)

	assert.Equal(t, []int64{100}, apiSamples(t, e), "readable after the failed flush")
	require.Positive(t, e.HeadBytes(), "the folded-back samples are accounted as head bytes again")

	mustAppend(t, e, api, 200, 2.0)
	require.NoError(t, e.Flush(ctx))

	assert.Equal(t, []int64{100, 200}, apiSamples(t, e),
		"the retried flush persists both the folded-back samples and the ones appended after it")
	assert.Equal(t, 1, e.PartCount())
}

// TestFlushFailureKeepsSamplesAcrossRestart is [TestFlushFailureKeepsSamples] with durability:
// because the failed flush's samples stay in the head, the next flush persists them and its WAL
// checkpoint only discards segments the committed part supersedes.
func TestFlushFailureKeepsSamplesAcrossRestart(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := &rejectWrites{Backend: backend.Memory()}
	dir := t.TempDir()
	cfg := engine.Config{Backend: be, Prefix: "t/metrics"}
	api := mkSeries("job", "api")

	w, err := wal.Create(dir, 0)
	require.NoError(t, err)

	cfg.WAL = w
	e := engine.New(cfg)

	mustAppend(t, e, api, 100, 1.0)
	require.NoError(t, w.Sync())

	be.armed.Store(true)
	require.Error(t, e.Flush(ctx))
	be.armed.Store(false)

	mustAppend(t, e, api, 200, 2.0)
	require.NoError(t, w.Sync())
	require.NoError(t, e.Flush(ctx))
	require.NoError(t, w.Close())

	// Restart: recover the watermark from the bucket index, then replay whatever WAL is left.
	restored := engine.New(engine.Config{Backend: be, Prefix: "t/metrics"})
	require.NoError(t, restored.LoadParts(ctx))
	require.NoError(t, restored.Replay(dir))

	assert.Equal(t, []int64{100, 200}, apiSamples(t, restored),
		"samples logged to the WAL must survive a restart after a failed flush")
}
