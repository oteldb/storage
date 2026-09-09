package storage

import (
	"context"
	"crypto/sha256"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/internal/partid"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// backendDigest returns every object in the backend rooted at dir, mapped to a digest of its
// content: the full listing a read-only open must leave byte-identical.
func backendDigest(t *testing.T, dir string) map[string][32]byte {
	t.Helper()
	ctx := context.Background()

	be, err := file.New(dir)
	require.NoError(t, err)

	keys, err := be.List(ctx, "")
	require.NoError(t, err)

	out := make(map[string][32]byte, len(keys))

	for _, k := range keys {
		data, err := be.Read(ctx, k)
		require.NoError(t, err)
		out[k] = sha256.Sum256(data)
	}

	return out
}

// seedDurable writes and flushes one metric and one log part into a fresh data directory, then
// plants an orphan part object — a part directory the bucket index does not name, exactly what the
// open-time sweep reclaims — and returns the directory and the orphan's key.
func seedDurable(t *testing.T) (dir, orphan string) {
	t.Helper()
	ctx := context.Background()
	dir = t.TempDir()

	be, err := file.New(dir)
	require.NoError(t, err)

	s, err := Open(ctx, Options{}, WithBackend(be), WithFlushInterval(-1))
	require.NoError(t, err)

	_, err = s.WriteMetrics(ctx, gaugeBatch("api", "http.requests", []int64{100, 200}, []float64{1, 2}))
	require.NoError(t, err)
	_, err = s.WriteLogs(ctx, logBatch("api", [3]any{100, 9, "hello"}))
	require.NoError(t, err)
	require.NoError(t, s.Close(ctx))

	orphan = "default/metrics/" + partid.New().String() + "/manifest"
	require.NoError(t, be.Write(ctx, orphan, []byte("orphaned part object")))

	return dir, orphan
}

// TestReadOnlyOpenLeavesBackendUntouched is the issue's reproducer in miniature: a read-only open
// must leave the object listing byte-identical, where a normal open sweeps the orphan.
func TestReadOnlyOpenLeavesBackendUntouched(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir, orphan := seedDurable(t)

	before := backendDigest(t, dir)
	require.Contains(t, before, orphan)

	be, err := file.New(dir)
	require.NoError(t, err)

	ro, err := Open(ctx, Options{}, WithBackend(be), WithReadOnly())
	require.NoError(t, err)

	// The flushed data still reads back: a read-only open declines to sweep, it does not decline
	// to recover.
	batches, err := fetch.Drain(ctx, must(ro.Fetcher("default").Fetch(ctx, fetch.Request{
		Start: 0, End: 1 << 60, Matchers: []fetch.Matcher{nameMatcher("http.requests")},
	})))
	require.NoError(t, err)
	require.NotEmpty(t, batches)

	require.NoError(t, ro.Close(ctx))
	assert.Equal(t, before, backendDigest(t, dir), "read-only open must not mutate the backend")

	// The contrast that gives the assertion above its meaning: the default open does sweep.
	be2, err := file.New(dir)
	require.NoError(t, err)

	rw, err := Open(ctx, Options{}, WithBackend(be2), WithFlushInterval(-1))
	require.NoError(t, err)
	require.NoError(t, rw.Close(ctx))

	after := backendDigest(t, dir)
	assert.NotContains(t, after, orphan, "the default open sweeps the orphan")
	assert.NotEqual(t, before, after)
}

// openReadOnly opens a read-only store over a freshly seeded durable directory.
func openReadOnly(t *testing.T) *Storage {
	t.Helper()
	ctx := context.Background()
	dir, _ := seedDurable(t)

	be, err := file.New(dir)
	require.NoError(t, err)

	s, err := Open(ctx, Options{}, WithBackend(be), WithReadOnly())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close(ctx)) })

	return s
}

func TestReadOnlyRefusesWrites(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openReadOnly(t)

	t.Run("Signals", func(t *testing.T) {
		t.Parallel()

		for name, write := range map[string]func() (Accepted, error){
			"metrics": func() (Accepted, error) {
				return s.WriteMetrics(ctx, gaugeBatch("api", "http.requests", []int64{300}, []float64{3}))
			},
			"logs": func() (Accepted, error) {
				return s.WriteLogs(ctx, logBatch("api", [3]any{300, 9, "nope"}))
			},
			"traces": func() (Accepted, error) {
				return s.WriteTraces(ctx, traceBatch("api", spanSpec{traceID: "t", spanID: "s", name: "GET", start: 300, end: 400}))
			},
			"profiles": func() (Accepted, error) {
				return s.WriteProfiles(ctx, profileBatch("api", 300, sampleSpec{typ: "cpu", unit: "ns", value: 1}))
			},
		} {
			acc, err := write()
			require.ErrorIs(t, err, ErrReadOnly, name)
			assert.Zero(t, acc.Accepted, name)
		}
	})

	t.Run("Maintenance", func(t *testing.T) {
		t.Parallel()

		a := s.Admin()

		require.ErrorIs(t, s.Reset(ctx), ErrReadOnly)
		require.ErrorIs(t, a.Flush(ctx, "default", signal.Metric), ErrReadOnly)
		require.ErrorIs(t, a.Compact(ctx, "default", signal.Metric), ErrReadOnly)
		require.ErrorIs(t, a.CompactNow(ctx, "default", signal.Metric), ErrReadOnly)
		require.ErrorIs(t, a.Retention(ctx, "default"), ErrReadOnly)
		require.ErrorIs(t, a.Rebalance(ctx), ErrReadOnly)
		require.ErrorIs(t, a.MaintainNow(ctx), ErrReadOnly)

		_, err := a.PruneIdentities(ctx, "default")
		require.ErrorIs(t, err, ErrReadOnly)
	})

	t.Run("NoMaintenanceLoop", func(t *testing.T) {
		t.Parallel()

		// The loop is what would flush, merge and apply retention on a timer behind the caller's
		// back; a read-only store must never start it, whatever the interval says.
		assert.Nil(t, s.stopCh)
		assert.Negative(t, s.opts.FlushInterval)
	})
}

func TestReadOnlyOptionValidation(t *testing.T) {
	t.Parallel()

	durable := durableBackend{backend.Memory()}

	for name, opts := range map[string][]Option{
		"Cluster":   {WithBackend(durable), WithReadOnly(), WithCluster(&cluster.Config{})},
		"WALDir":    {WithBackend(durable), WithReadOnly(), WithWALDir(t.TempDir())},
		"Ephemeral": {WithBackend(backend.Memory()), WithReadOnly()},
		"NoBackend": {WithReadOnly()},
	} {
		_, err := Open(context.Background(), Options{}, opts...)
		require.ErrorContains(t, err, "invalid options", name)
	}
}
