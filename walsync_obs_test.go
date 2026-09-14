package storage

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/internal/obs/obstest"
	"github.com/oteldb/storage/query/fetch"
)

// TestWALSyncFailuresAreReported: a background fsync that fails is counted every time, but logged
// once per failing run, so a disk that fails every tick neither goes unnoticed nor floods the log.
func TestWALSyncFailuresAreReported(t *testing.T) {
	t.Parallel()

	mp, m := obstest.Provider(t)
	core, logs := observer.New(zap.InfoLevel)

	s, err := InMemory(WithMeterProvider(mp), WithLogger(zap.New(core)))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(t.Context()) })

	logs.TakeAll()

	failing := func() int64 { v, _ := m.Gauge("storage.wal.sync_failing"); return v }
	eio := errors.New("input/output error")

	s.recordWALSync(t.Context(), []error{nil, nil})
	assert.Zero(t, failing())
	assert.Zero(t, logs.Len())

	for range 3 {
		s.recordWALSync(t.Context(), []error{eio, nil, eio})
	}

	assert.Equal(t, int64(6), m.Counter("storage.wal.sync_failures"))
	assert.Equal(t, int64(1), failing())
	assert.Equal(t, WALSyncStats{Failures: 6, Failing: true}, s.Inspect().WALSync)
	require.Equal(t, 1, logs.FilterLevelExact(zap.ErrorLevel).Len(), "a failing run logs once")

	s.recordWALSync(t.Context(), []error{nil, nil, nil})

	assert.Equal(t, int64(6), m.Counter("storage.wal.sync_failures"))
	assert.Zero(t, failing())
	assert.Equal(t, WALSyncStats{Failures: 6}, s.Inspect().WALSync)
	assert.Equal(t, 1, logs.FilterMessage("background WAL fsync recovered").Len())

	s.recordWALSync(t.Context(), []error{eio})
	assert.Equal(t, 2, logs.FilterLevelExact(zap.ErrorLevel).Len(), "a new failing run logs again")
}

// TestDamagedWALOpensAndIsCounted: a damaged segment does not keep the store from opening. Replay skips
// the damaged record, keeps every other one, counts the damage as tolerated corruption and keeps a copy
// of the segment for inspection.
func TestDamagedWALOpensAndIsCounted(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	dataDir, walDir := t.TempDir(), t.TempDir()

	s1 := reopenDurable(t, dataDir, walDir)
	for i := range int64(8) {
		_, err := s1.WriteMetrics(ctx, gaugeBatch("api", "http.requests", []int64{100 + i}, []float64{float64(i)}))
		require.NoError(t, err)
	}

	crash(t, s1)

	var segments []string

	require.NoError(t, filepath.WalkDir(walDir, func(path string, _ fs.DirEntry, err error) error {
		if err == nil && filepath.Ext(path) == ".wal" {
			segments = append(segments, path)
		}

		return err
	}))
	require.Len(t, segments, 1)

	data, err := os.ReadFile(segments[0])
	require.NoError(t, err)
	data[len(data)-5] ^= 0xff // inside the last sample's frame
	require.NoError(t, os.WriteFile(segments[0], data, 0o600))

	be, err := file.New(dataDir)
	require.NoError(t, err)

	mp, m := obstest.Provider(t)

	s2, err := Open(ctx, Options{}, WithBackend(be), WithWALDir(walDir), WithFlushInterval(-1), WithMeterProvider(mp))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s2.Close(ctx) })

	assert.Equal(t, int64(1), m.Counter("storage.corruption.detected", "component", "wal", "disposition", "tolerated"))
	assert.Zero(t, m.Counter("storage.corruption.detected", "component", "wal", "disposition", "fatal"))

	batches, err := fetch.Drain(ctx, must(s2.Fetcher("default").Fetch(ctx, fetch.Request{
		Start: 0, End: 1 << 60, Matchers: []fetch.Matcher{nameMatcher("http.requests")},
	})))
	require.NoError(t, err)
	require.Len(t, batches, 1)
	assert.Equal(t, []int64{100, 101, 102, 103, 104, 105, 106}, batches[0].Timestamps)

	kept, err := os.ReadFile(segments[0] + ".damaged")
	require.NoError(t, err)
	assert.Equal(t, data, kept)
}
