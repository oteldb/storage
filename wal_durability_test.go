package storage

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/profile"
)

// reopenDurable opens a durable (file) store over the given backend + WAL directories.
func reopenDurable(t *testing.T, dataDir, walDir string) *Storage {
	t.Helper()

	be, err := file.New(dataDir)
	require.NoError(t, err)
	s, err := Open(context.Background(), Options{}, WithBackend(be), WithWALDir(walDir))
	require.NoError(t, err)

	return s
}

// TestWALRecoversUnflushedMetrics writes metrics to a WAL-backed durable store, abandons it (no
// flush, no Close — a crash), and verifies a reopened store replays the WAL to restore the
// unflushed head.
func TestWALRecoversUnflushedMetrics(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dataDir, walDir := t.TempDir(), t.TempDir()

	s1 := reopenDurable(t, dataDir, walDir)
	_, err := s1.WriteMetrics(ctx, gaugeBatch("api", "http.requests", []int64{100, 200}, []float64{1, 2}))
	require.NoError(t, err)
	// Simulate a crash: do not Close (which would flush + checkpoint the WAL).

	s2 := reopenDurable(t, dataDir, walDir)
	t.Cleanup(func() { _ = s2.Close(ctx) })

	batches, err := fetch.Drain(ctx, must(s2.Fetcher("default").Fetch(ctx, fetch.Request{
		Start: 0, End: 1 << 60, Matchers: []fetch.Matcher{nameMatcher("http.requests")},
	})))
	require.NoError(t, err)
	require.Len(t, batches, 1, "the crashed process's unflushed samples are recovered from the WAL")
	assert.Equal(t, []int64{100, 200}, batches[0].Timestamps)
	assert.Equal(t, []float64{1, 2}, batches[0].Values)
}

// TestWALRecoversUnflushedProfiles verifies the record + side-store WAL path end to end: an
// abandoned profiles write is recovered with both its samples AND its symbol store, so resolution
// works after restart.
func TestWALRecoversUnflushedProfiles(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dataDir, walDir := t.TempDir(), t.TempDir()

	s1 := reopenDurable(t, dataDir, walDir)
	_, err := s1.WriteProfiles(ctx, profileBatch("api", 1000, sampleSpec{"cpu", "nanoseconds", 42}))
	require.NoError(t, err)
	// Crash: no Close.

	s2 := reopenDurable(t, dataDir, walDir)
	t.Cleanup(func() { _ = s2.Close(ctx) })

	// Samples recovered.
	batches, err := fetch.Drain(ctx, must(s2.ProfileFetcher("default").Fetch(ctx, fetch.Request{
		Signal: signal.Profile, Start: 0, End: 1 << 60, Matchers: []fetch.Matcher{nameMatcherSvc("api")},
	})))
	require.NoError(t, err)
	require.Len(t, batches, 1)
	assert.Equal(t, []int64{42}, profValues(batches[0]))

	// Symbol store recovered (the side frames replayed), so the stack resolves to its frames.
	resolver, err := s2.ProfileResolver(ctx, "default")
	require.NoError(t, err)
	stacks, _ := batches[0].Column(profile.ColStackID)
	frames := resolver.Resolve(stacks.Bytes[0])
	require.NotEmpty(t, frames, "the WAL-recovered symbol store resolves the stack")
	assert.Equal(t, "main", frames[len(frames)-1].Function)
}

// TestWALSyncAlwaysRecovers confirms the WALSyncAlways fsync policy is wired and still recovers an
// abandoned (crashed) write.
func TestWALSyncAlwaysRecovers(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dataDir, walDir := t.TempDir(), t.TempDir()

	be, err := file.New(dataDir)
	require.NoError(t, err)
	s1, err := Open(ctx, Options{}, WithBackend(be), WithWALDir(walDir), WithWALSync(WALSyncAlways))
	require.NoError(t, err)
	_, err = s1.WriteMetrics(ctx, gaugeBatch("api", "http.requests", []int64{100}, []float64{1}))
	require.NoError(t, err)
	// Crash: no Close (WALSyncAlways starts no background goroutine, so nothing leaks).

	s2 := reopenDurable(t, dataDir, walDir)
	t.Cleanup(func() { _ = s2.Close(ctx) })
	batches, err := fetch.Drain(ctx, must(s2.Fetcher("default").Fetch(ctx, fetch.Request{
		Start: 0, End: 1 << 60, Matchers: []fetch.Matcher{nameMatcher("http.requests")},
	})))
	require.NoError(t, err)
	require.Len(t, batches, 1)
}

// TestWALSyncIntervalLifecycle confirms the background-fsync goroutine starts and stops cleanly with
// the store, and the data survives a clean close.
func TestWALSyncIntervalLifecycle(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dataDir, walDir := t.TempDir(), t.TempDir()

	be, err := file.New(dataDir)
	require.NoError(t, err)
	s1, err := Open(ctx, Options{}, WithBackend(be), WithWALDir(walDir), WithWALSyncInterval(20*time.Millisecond))
	require.NoError(t, err)
	_, err = s1.WriteMetrics(ctx, gaugeBatch("api", "http.requests", []int64{100}, []float64{1}))
	require.NoError(t, err)

	time.Sleep(60 * time.Millisecond) // let the background sync tick at least twice
	require.NoError(t, s1.Close(ctx)) // stops the sync goroutine and flushes

	s2 := reopenDurable(t, dataDir, walDir)
	t.Cleanup(func() { _ = s2.Close(ctx) })
	batches, err := fetch.Drain(ctx, must(s2.Fetcher("default").Fetch(ctx, fetch.Request{
		Start: 0, End: 1 << 60, Matchers: []fetch.Matcher{nameMatcher("http.requests")},
	})))
	require.NoError(t, err)
	require.Len(t, batches, 1)
}

// TestWALEmptyAfterCleanClose verifies a graceful Close flushes the head and checkpoints the WAL,
// leaving nothing for a reopen to replay (the data comes from the flushed part instead).
func TestWALEmptyAfterCleanClose(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dataDir, walDir := t.TempDir(), t.TempDir()

	s1 := reopenDurable(t, dataDir, walDir)
	_, err := s1.WriteMetrics(ctx, gaugeBatch("api", "http.requests", []int64{100}, []float64{1}))
	require.NoError(t, err)
	require.NoError(t, s1.Close(ctx)) // flush + checkpoint

	// No WAL segment holds records after a clean close (only a fresh empty segment per engine).
	var segBytes int64
	require.NoError(t, filepath.WalkDir(walDir, func(_ string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}

		info, err := d.Info()
		if err == nil {
			segBytes += info.Size()
		}

		return nil
	}))
	assert.Zero(t, segBytes, "clean close leaves no buffered WAL records")

	// The data is still served after reopen — from the flushed part.
	s2 := reopenDurable(t, dataDir, walDir)
	t.Cleanup(func() { _ = s2.Close(ctx) })
	batches, err := fetch.Drain(ctx, must(s2.Fetcher("default").Fetch(ctx, fetch.Request{
		Start: 0, End: 1 << 60, Matchers: []fetch.Matcher{nameMatcher("http.requests")},
	})))
	require.NoError(t, err)
	require.Len(t, batches, 1)
}
