package recordengine_test

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
	"github.com/oteldb/storage/internal/watermark"
	"github.com/oteldb/storage/recordengine"
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

// seedBulkyParts flushes two parts holding enough records per stream that the timestamp column is a
// real object: a one-record part collapses it into the manifest, which would hide the whole-column
// decode this test is about.
func seedBulkyParts(t *testing.T, be backend.Backend) {
	t.Helper()

	ctx := context.Background()
	owner := newEngine(t, be)

	for part := range 2 {
		for s := range 3 {
			recs := make([]rrec, 0, 200)
			for i := range 200 {
				recs = append(recs, rrec{
					ts:   int64(part)*1_000_000 + int64(i)*1000,
					body: "body " + strconv.Itoa(i),
					id:   strconv.Itoa(i),
				})
			}

			ingest(t, owner, mkBatch("svc"+strconv.Itoa(s), recs...))
		}

		require.NoError(t, owner.Flush(ctx))
	}
}

// TestRefreshReplicaDoesNotRereadParts: a refresh reuses the part handles the previous one opened,
// so nothing a part carries is read again — not its timestamp column, its blooms, its record keys,
// its identities or its watermark sidecar. Only the bucket index and the per-part liveness probe
// are re-read, and both are O(1) objects.
func TestRefreshReplicaDoesNotRereadParts(t *testing.T) {
	t.Parallel()

	const refreshes = 5

	ctx := context.Background()
	be := newReadCounter()
	seedBulkyParts(t, be)

	replica := newEngine(t, be)
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
		case !strings.Contains(key, "/c/") && !strings.Contains(key, "/bloom-") &&
			!strings.HasSuffix(key, "/keys.bin") && !strings.HasSuffix(key, "/identity") &&
			!strings.HasSuffix(key, watermark.Key("")):
			// The bucket index and the legacy whole-set identity object are engine-scoped, not
			// per-part, so their cost does not scale with the part set.
		default:
			assert.Zero(t, grew, "re-read per refresh: %s", key)
		}
	}
}

// TestStreamWatermarkSidecarMatchesDecode: the sidecar and the timestamp-column decode are two ways
// to the same answer, so a replica that has the sidecar and one that must fall back trim identically.
func TestStreamWatermarkSidecarMatchesDecode(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	seedBulkyParts(t, be)

	// One record already durable for its stream and one past every part, so the two engines differ
	// unless both resolve the same per-stream watermark.
	seedHead := func(e *recordengine.Engine) {
		for s := range 3 {
			ingest(t, e, mkBatch("svc"+strconv.Itoa(s),
				rrec{ts: 100_000, body: "durable"},
				rrec{ts: 1_500_000, body: "late"},
			))
		}
	}

	withSidecar := newEngine(t, be)
	seedHead(withSidecar)
	require.NoError(t, withSidecar.RefreshReplica(ctx))
	require.Equal(t, 3, withSidecar.HeadRecordCount(), "the durable record is trimmed, the late one kept")

	keys, err := be.List(ctx, "")
	require.NoError(t, err)

	var sidecars int

	for _, k := range keys {
		if strings.HasSuffix(k, watermark.Key("")) {
			sidecars++

			require.NoError(t, be.Delete(ctx, k))
		}
	}

	require.Equal(t, 2, sidecars, "every part carries a watermark sidecar")

	viaDecode := newEngine(t, be)
	seedHead(viaDecode)
	require.NoError(t, viaDecode.RefreshReplica(ctx))

	assert.Equal(t, withSidecar.HeadRecordCount(), viaDecode.HeadRecordCount())

	for s := range 3 {
		svc := "svc" + strconv.Itoa(s)
		want, got := fetchAll(t, withSidecar, req(svc)), fetchAll(t, viaDecode, req(svc))
		require.Len(t, got, len(want))

		for i := range want {
			assert.Equal(t, want[i].Timestamps, got[i].Timestamps, svc)
		}
	}
}
