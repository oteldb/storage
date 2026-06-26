package recordengine_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/wal"
)

// snapshotWAL reads every *.wal segment in dir (name → bytes).
func snapshotWAL(t *testing.T, dir string) map[string][]byte {
	t.Helper()

	out := map[string][]byte{}
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)

	for _, e := range entries {
		if filepath.Ext(e.Name()) != ".wal" {
			continue
		}

		data, rerr := os.ReadFile(filepath.Join(dir, e.Name()))
		require.NoError(t, rerr)
		out[e.Name()] = data
	}

	return out
}

// TestWALExactlyOnceRecovery reproduces the crash window — a flush commits its part and watermark but
// the WAL segments it superseded are NOT deleted — and verifies recovery replays neither too much
// (the flushed records are skipped, no duplicate) nor too little (the unflushed records return).
func TestWALExactlyOnceRecovery(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	be := backend.Memory()
	walDir := t.TempDir()

	w, err := wal.Create(walDir, 0)
	require.NoError(t, err)
	e := recordengine.New(recordengine.Config{Schema: testSchema, Backend: be, Prefix: "t/recs", WAL: w})

	// B1 (generation 1) → WAL, then flush it to a part (watermark 1, segment checkpointed/deleted).
	ingest(t, e, mkBatch("api", rrec{ts: 1, body: "b1"}))
	require.NoError(t, w.Sync())
	flushedSegs := snapshotWAL(t, walDir) // the generation-1 segment(s), about to be deleted
	require.NoError(t, e.Flush(ctx))

	// B2 (generation 2) → WAL, unflushed.
	ingest(t, e, mkBatch("api", rrec{ts: 2, body: "b2"}))
	require.NoError(t, w.Sync())

	// Simulate the crash: restore the generation-1 segments the checkpoint deleted (as if the process
	// died between the bucket-index commit and the WAL deletion).
	for name, data := range flushedSegs {
		require.NoError(t, os.WriteFile(filepath.Join(walDir, name), data, 0o600))
	}

	// Recover into a fresh engine over the same backend + WAL dir.
	w2, err := wal.Create(walDir, 0)
	require.NoError(t, err)
	e2 := recordengine.New(recordengine.Config{Schema: testSchema, Backend: be, Prefix: "t/recs", WAL: w2})
	require.NoError(t, e2.LoadParts(ctx)) // recovers the watermark (1)
	require.NoError(t, e2.Replay(walDir)) // skips generation ≤ 1, replays generation 2

	got := bodies(fetchAll(t, e2, req("api"))[0])
	require.Equal(t, []string{"b1", "b2"}, got,
		"b1 (flushed) appears once despite its WAL segment surviving; b2 (unflushed) is recovered")
}
