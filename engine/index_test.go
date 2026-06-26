package engine_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
)

// TestStatelessReadFromObjectStore is the M5 exit check: a fresh engine reconstructs both the
// part set (bucket index) and the identity index (series object) from the backend alone, and
// serves the same matcher-based query as the writer — with no in-memory state shared.
func TestStatelessReadFromObjectStore(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	cfg := engine.Config{Backend: be, Prefix: "default/metrics"}

	// Writer: ingest two series across two flushed parts.
	w := engine.New(cfg)
	api := mkSeries("job", "api")
	web := mkSeries("job", "web")
	mustAppend(t, w, api, 100, 1.0)
	mustAppend(t, w, web, 100, 9.0)
	require.NoError(t, w.Flush(ctx))
	mustAppend(t, w, api, 200, 2.0)
	require.NoError(t, w.Flush(ctx))
	require.Equal(t, 2, w.PartCount())

	// Reader: a brand-new engine over the same backend, nothing carried over.
	r := engine.New(cfg)
	require.Equal(t, 0, r.PartCount())
	require.Equal(t, 0, r.SeriesCount())

	require.NoError(t, r.LoadParts(ctx))
	assert.Equal(t, 2, r.PartCount(), "parts reconstructed from the bucket index")
	assert.Equal(t, 2, r.SeriesCount(), "identities reconstructed from the series object")

	// A matcher-based query works (postings + labels rebuilt) and returns the writer's data.
	got := fetchAll(t, r, fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("job", "api")}})
	require.Len(t, got, 1)
	assert.Equal(t, []int64{100, 200}, got[0].Timestamps)
	assert.Equal(t, []float64{1, 2}, got[0].Values)
	assert.True(t, api.Equal(got[0].Series), "labels reconstructed, not just ids")

	// The other series is independently reconstructed.
	web2 := fetchAll(t, r, fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("job", "web")}})
	require.Len(t, web2, 1)
	assert.Equal(t, []float64{9}, web2[0].Values)

	// A reloaded reader can keep writing without clobbering existing part keys.
	mustAppend(t, r, api, 300, 3.0)
	require.NoError(t, r.Flush(ctx))
	assert.Equal(t, 3, r.PartCount())
}

// TestReloadAfterMergeSeesConsolidatedParts checks that the bucket index tracks a merge: the
// reader reconstructs a single compacted part, not the merged-away sources.
func TestReloadAfterMergeSeesConsolidatedParts(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	cfg := engine.Config{Backend: be, Prefix: "t/metrics"}

	w := engine.New(cfg)
	s := mkSeries("job", "api")
	mustAppend(t, w, s, 100, 1.0)
	require.NoError(t, w.Flush(ctx))
	mustAppend(t, w, s, 200, 2.0)
	require.NoError(t, w.Flush(ctx))
	require.Equal(t, 2, w.PartCount())

	require.NoError(t, w.Merge(ctx, 0)) // compact two parts into one
	require.Equal(t, 1, w.PartCount())

	r := engine.New(cfg)
	require.NoError(t, r.LoadParts(ctx))
	assert.Equal(t, 1, r.PartCount(), "index reflects the post-merge part set")

	got := fetchAll(t, r, fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("job", "api")}})
	require.Len(t, got, 1)
	assert.Equal(t, []int64{100, 200}, got[0].Timestamps)
}

func TestLoadPartsCorruptIndexesError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	for _, object := range []string{"default/metrics/" + "bucket-index.bin", "default/metrics/series.bin"} {
		t.Run(object, func(t *testing.T) {
			t.Parallel()

			be := backend.Memory()
			cfg := engine.Config{Backend: be, Prefix: "default/metrics"}

			w := engine.New(cfg)
			mustAppend(t, w, mkSeries("job", "api"), 100, 1.0)
			require.NoError(t, w.Flush(ctx))

			// Corrupt one of the durable index objects.
			require.NoError(t, be.Write(ctx, object, []byte("garbage")))

			require.Error(t, engine.New(cfg).LoadParts(ctx), "a corrupt index must fail recovery, not silently lose data")
		})
	}
}

// failKeyWrite fails Write for keys ending in suffix, passing everything else through.
type failKeyWrite struct {
	backend.Backend

	suffix string
}

func (f failKeyWrite) Write(ctx context.Context, key string, data []byte) error {
	if strings.HasSuffix(key, f.suffix) {
		return assert.AnError
	}

	return f.Backend.Write(ctx, key, data)
}

func TestFlushIndexWriteErrorsPropagate(t *testing.T) {
	t.Parallel()

	for _, suffix := range []string{"bucket-index.bin", "series.bin"} {
		t.Run(suffix, func(t *testing.T) {
			t.Parallel()

			be := failKeyWrite{Backend: backend.Memory(), suffix: suffix}
			e := engine.New(engine.Config{Backend: be, Prefix: "default/metrics"})
			mustAppend(t, e, mkSeries("job", "api"), 100, 1.0)

			require.Error(t, e.Flush(context.Background()), "a failed durable-index write must fail the flush")
		})
	}
}

func TestLoadPartsHeadOnlyNoop(t *testing.T) {
	t.Parallel()

	e := engine.New(engine.Config{}) // no backend
	require.NoError(t, e.LoadParts(context.Background()))
	assert.Equal(t, 0, e.PartCount())
}
