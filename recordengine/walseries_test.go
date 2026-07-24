package recordengine_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/wal"
)

// TestWALSeriesLoggedOnRegistration covers the stream-registration/WAL-series split in AppendBatch: a
// stream is registered in the head on first sight, but reports as new only once — so the series
// record must be logged then, not when the batch first has an accepted record. A first batch that is
// fully rejected (here by the OOO window) would otherwise leave a head-registered stream with no
// durable identity, and replay would drop every later record of it silently.
func TestWALSeriesLoggedOnRegistration(t *testing.T) {
	t.Parallel()

	walDir := t.TempDir()

	w, err := wal.Create(walDir, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	e := recordengine.New(recordengine.Config{Schema: testSchema, Prefix: "t/recs", WAL: w, OOOWindow: 50})

	// "api" advances the head's newest to 10000.
	ingest(t, e, mkBatch("api", rrec{ts: 10000, body: "api-1"}))

	// "web" is first seen with a record outside the OOO window: the batch is fully rejected, but the
	// stream is registered.
	res, err := e.AppendBatch(mkBatch("web", rrec{ts: 100, body: "web-old"}), recordengine.AppendLimits{})
	require.NoError(t, err)
	require.Equal(t, 0, res.Accepted)

	// A later in-window batch for "web" is accepted and logged as records — the stream is no longer
	// new, so this is the last chance for its identity to have reached the WAL.
	ingest(t, e, mkBatch("web", rrec{ts: 10001, body: "web-live"}))
	require.NoError(t, w.Sync())

	e2 := recordengine.New(recordengine.Config{Schema: testSchema, Prefix: "t/recs", OOOWindow: 50})
	require.NoError(t, e2.Replay(walDir))

	batches := fetchAll(t, e2, req("web"))

	got := make([]string, 0, len(batches))
	for _, b := range batches {
		got = append(got, bodies(b)...)
	}

	require.Equal(t, []string{"web-live"}, got, "accepted records of an already-registered stream must replay")
}
