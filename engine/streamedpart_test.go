package engine_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/block"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
)

// TestMergeOverStreamingBackend runs a merge end to end over the file backend, the one that builds
// objects incrementally. It is the only path that produces the footer column layout, so it is the
// only one that proves a streamed part is readable by the same reader as a buffered one.
func TestMergeOverStreamingBackend(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	b, err := file.New(t.TempDir())
	require.NoError(t, err)
	require.True(t, backend.StreamsWrites(b), "the file backend is the streaming reference")

	e := engine.New(engine.Config{Backend: b, Prefix: "default/metrics"})

	const (
		series  = 12
		flushes = 5
		perPart = 40
	)

	want := make(map[string][]float64, series)

	for f := range flushes {
		for s := range series {
			lbl := seriesLabel(s)
			ser := mkSeries("job", lbl)

			for i := range perPart {
				ts := int64(f*perPart+i) * 15_000
				v := float64(s*1000+f*100+i) / 4

				mustAppend(t, e, ser, ts, v)
				want[lbl] = append(want[lbl], v)
			}
		}

		require.NoError(t, e.Flush(ctx))
	}

	require.Equal(t, flushes, e.PartCount())
	require.NoError(t, e.Merge(ctx, 0))
	assert.Equal(t, 1, e.PartCount(), "the flushes compact into one streamed part")

	assertColumnsStreamed(t, ctx, b)

	for s := range series {
		lbl := seriesLabel(s)

		got := fetchAll(t, e, fetch.Request{
			Start: 0, End: 1 << 40, Matchers: []fetch.Matcher{eqMatcher("job", lbl)},
		})
		require.Len(t, got, 1, "series %s", lbl)
		assert.Equal(t, want[lbl], got[0].Values, "series %s", lbl)
		assert.Len(t, got[0].Timestamps, flushes*perPart, "series %s", lbl)
	}
}

// TestMergeOverStreamingBackendLeavesNoTempFiles covers the other half of an incremental write: a
// merge retires its sources, and the temp objects its output passed through must not outlive it.
func TestMergeOverStreamingBackendLeavesNoTempFiles(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	b, err := file.New(t.TempDir())
	require.NoError(t, err)

	e := engine.New(engine.Config{Backend: b, Prefix: "default/metrics"})

	for f := range 4 {
		for s := range 6 {
			for i := range 20 {
				mustAppend(t, e, mkSeries("job", seriesLabel(s)), int64(f*20+i)*15_000, float64(i))
			}
		}

		require.NoError(t, e.Flush(ctx))
	}

	require.NoError(t, e.Merge(ctx, 0))

	keys, err := b.List(ctx, "")
	require.NoError(t, err)
	require.NotEmpty(t, keys)

	for _, k := range keys {
		assert.NotContains(t, k, ".tmp-", "a committed merge leaves no temp object behind")
	}
}

// assertColumnsStreamed checks that the surviving part's block-framed columns really took the
// incremental path — without it the test would pass just as well over a writer that buffered.
func assertColumnsStreamed(t *testing.T, ctx context.Context, b backend.Backend) {
	t.Helper()

	keys, err := b.List(ctx, "")
	require.NoError(t, err)

	streamed := 0

	for _, k := range keys {
		prefix, ok := strings.CutSuffix(k, "/manifest")
		if !ok {
			continue
		}

		r, err := block.OpenPart(ctx, b, prefix)
		require.NoError(t, err)

		for _, c := range r.Manifest().Columns {
			if !c.Footer {
				continue
			}

			streamed++

			assert.True(t, c.Blocked && c.Framed,
				"column %q: a footer directory only makes sense on a framed column", c.Name)
		}
	}

	assert.Positive(t, streamed, "the merge output must carry footer-directory columns")
}

func seriesLabel(i int) string {
	return string(rune('a' + i%26))
}
