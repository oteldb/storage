package engine_test

import (
	"context"
	"maps"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/internal/watermark"
)

// readCounter counts reads per key. It embeds the interface rather than the concrete backend, so it
// implements neither Viewer nor Sizer and every read — including a size probe — funnels through
// Read and is counted.
type readCounter struct {
	backend.Backend

	mu     sync.Mutex
	counts map[string]int
}

func newReadCounter() *readCounter {
	return &readCounter{Backend: backend.Memory(), counts: map[string]int{}}
}

func (c *readCounter) Read(ctx context.Context, key string) ([]byte, error) {
	c.mu.Lock()
	c.counts[key]++
	c.mu.Unlock()

	return c.Backend.Read(ctx, key)
}

func (c *readCounter) snapshot() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return maps.Clone(c.counts)
}

// seedBulkyParts flushes two parts holding enough samples per series that the timestamp column is a
// real object: a one-sample part collapses it into the manifest, which would hide the whole-column
// decode this test is about.
func seedBulkyParts(t *testing.T, be backend.Backend) {
	t.Helper()

	ctx := context.Background()
	owner := engine.New(engine.Config{Backend: be, Prefix: replicaPrefix})

	for part := range 2 {
		for s := range 3 {
			series := mkSeries("job", "j"+strconv.Itoa(s))
			for i := range 200 {
				mustAppend(t, owner, series, int64(part)*1_000_000+int64(i)*1000, float64(i))
			}
		}

		require.NoError(t, owner.Flush(ctx))
	}
}

// TestRefreshReplicaDoesNotRereadParts: a refresh reuses the part handles the previous one opened,
// so nothing a part carries is read again — not its timestamp column, its series index, its
// identities or its watermark sidecar. Only the bucket index and the per-part liveness probe are
// re-read, and both are O(1) objects.
func TestRefreshReplicaDoesNotRereadParts(t *testing.T) {
	t.Parallel()

	const refreshes = 5

	ctx := context.Background()
	be := newReadCounter()
	seedBulkyParts(t, be)

	replica := engine.New(engine.Config{Backend: be, Prefix: replicaPrefix})
	require.NoError(t, replica.RefreshReplica(ctx))
	require.Equal(t, 2, replica.PartCount())

	before := be.snapshot()

	// The sidecar's other half: a cold replica resolves the watermarks without touching the
	// timestamp column at all, so a part freshly mirrored here costs no whole-column decode.
	for key, n := range before {
		if strings.HasSuffix(key, "/c/1") {
			assert.Zero(t, n, "timestamp column read on a cold refresh: %s", key)
		}
	}

	for range refreshes {
		require.NoError(t, replica.RefreshReplica(ctx))
	}

	for key, n := range be.snapshot() {
		grew := n - before[key]

		switch {
		case strings.HasSuffix(key, "/manifest"):
			// The liveness probe: a part whose objects went away must still become a repair want
			// (TestRefreshReplicaGonePartBecomesPendingWant), and this is what notices.
			assert.LessOrEqual(t, grew, refreshes, key)
		case !strings.Contains(key, "/c/") && !strings.HasSuffix(key, "/sidx") &&
			!strings.HasSuffix(key, "/identity") && !strings.HasSuffix(key, watermark.Key("")):
			// The bucket index and the legacy whole-set identity object are engine-scoped, not
			// per-part, so their cost does not scale with the part set.
		default:
			assert.Zero(t, grew, "re-read per refresh: %s", key)
		}
	}
}

// TestSeriesWatermarkSidecarMatchesDecode: the sidecar and the timestamp-column decode are two ways
// to the same answer, so a replica that has the sidecar and one that must fall back trim identically.
func TestSeriesWatermarkSidecarMatchesDecode(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	seedBulkyParts(t, be)

	// One sample already durable for its series and one past every part, so the two engines differ
	// unless both resolve the same per-series watermark.
	seedHead := func(e *engine.Engine) {
		for s := range 3 {
			series := mkSeries("job", "j"+strconv.Itoa(s))
			mustAppend(t, e, series, 100_000, 1)
			mustAppend(t, e, series, 1_500_000, 2)
		}
	}

	withSidecar := engine.New(engine.Config{Backend: be, Prefix: replicaPrefix})
	seedHead(withSidecar)
	require.NoError(t, withSidecar.RefreshReplica(ctx))
	require.Equal(t, 3, withSidecar.HeadSampleCount(), "the durable sample is trimmed, the late one kept")

	keys, err := be.List(ctx, replicaPrefix)
	require.NoError(t, err)

	var sidecars int

	for _, k := range keys {
		if strings.HasSuffix(k, watermark.Key("")) {
			sidecars++

			require.NoError(t, be.Delete(ctx, k))
		}
	}

	require.Equal(t, 2, sidecars, "every part carries a watermark sidecar")

	viaDecode := engine.New(engine.Config{Backend: be, Prefix: replicaPrefix})
	seedHead(viaDecode)
	require.NoError(t, viaDecode.RefreshReplica(ctx))

	assert.Equal(t, withSidecar.HeadSampleCount(), viaDecode.HeadSampleCount())

	for s := range 3 {
		job := "j" + strconv.Itoa(s)
		want, got := fetchJob(t, withSidecar, job), fetchJob(t, viaDecode, job)
		require.Len(t, got, len(want))

		for i := range want {
			assert.Equal(t, want[i].Timestamps, got[i].Timestamps, job)
		}
	}
}

// BenchmarkRefreshReplica is the replica maintenance tick itself: reload the index and trim the
// head against every part's per-series watermark.
func BenchmarkRefreshReplica(b *testing.B) {
	const (
		parts   = 4
		series  = 50
		samples = 500
	)

	ctx := context.Background()
	be := backend.Memory()
	owner := engine.New(engine.Config{Backend: be, Prefix: replicaPrefix})

	for part := range parts {
		for s := range series {
			id := mkSeries("job", "j"+strconv.Itoa(s))
			for i := range samples {
				ok, err := owner.Append(id, int64(part)*1_000_000+int64(i)*1000, float64(i))
				require.NoError(b, err)
				require.True(b, ok)
			}
		}

		require.NoError(b, owner.Flush(ctx))
	}

	replica := engine.New(engine.Config{Backend: be, Prefix: replicaPrefix})
	require.NoError(b, replica.RefreshReplica(ctx))
	require.Equal(b, parts, replica.PartCount())

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		if err := replica.RefreshReplica(ctx); err != nil {
			b.Fatal(err)
		}
	}
}
