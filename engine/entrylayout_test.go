package engine_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/signal"
)

// forgetLayouts rewrites the shared index as a writer before the index carried layouts left it.
func forgetLayouts(t *testing.T, be backend.Backend) {
	t.Helper()

	ctx := context.Background()
	key := sharedPrefix + "/" + bucketindex.Object

	ix, version, err := bucketindex.LoadVersioned(ctx, be, key)
	require.NoError(t, err)

	for i := range ix.Entries {
		ix.Entries[i].Rollup = nil
	}

	_, err = ix.Save(ctx, be, key, version)
	require.NoError(t, err)
}

func rolled(agg signal.Aggregation) engine.MergeOptions {
	return engine.MergeOptions{Downsample: []engine.DownsampleTier{{Before: 1 << 62, Interval: 1000, Agg: agg}}}
}

// TestEntryLayoutFilledFromTheManifest: an index entry recording no layout is filled in from the
// part's manifest once the part opens, own or adopted, and the next commit publishes it.
func TestEntryLayoutFilledFromTheManifest(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	a, b, _, _ := rivals(t, be)

	for i := range int64(5) {
		mustAppend(t, a, mkSeries("job", "api"), 100+i, 1)
	}

	require.NoError(t, a.Flush(ctx))
	require.NoError(t, a.MergeWith(ctx, rolled(signal.AggCount)))

	rolledPart := committedIndex(t, be).Entries[0].Prefix
	require.NotNil(t, committedIndex(t, be).Entries[0].Rollup, "a commit records its own parts' layouts")

	forgetLayouts(t, be)

	// b adopts a's part by losing the CAS to it, opens it, and publishes its layout.
	mustAppend(t, b, mkSeries("job", "web"), 100, 1)
	require.NoError(t, b.Flush(ctx))

	layouts := map[string]*bucketindex.Rollup{}
	for _, ent := range committedIndex(t, be).Entries {
		layouts[ent.Prefix] = ent.Rollup
	}

	require.Contains(t, layouts, rolledPart)
	require.NotNil(t, layouts[rolledPart], "the adopted part's layout is filled from its manifest")
	assert.Equal(t, uint8(signal.AggCount), layouts[rolledPart].Tiers[0].Agg)

	forgetLayouts(t, be)

	// a loads it as its own part and fills it the same way.
	require.NoError(t, a.LoadParts(ctx))
	mustAppend(t, a, mkSeries("job", "api"), 200, 1)
	require.NoError(t, a.Flush(ctx))

	for _, ent := range committedIndex(t, be).Entries {
		assert.NotNilf(t, ent.Rollup, "%s records its layout again", ent.Prefix)
	}
}

// TestUnknownLayoutOfAnUnreadablePartIsRaw: an adopted entry that records no layout and whose part
// cannot be opened is legacy, and counts as raw — it never aborts a commit. Aborting would stall the
// merge for good, since only reading the manifest could ever fill the layout in. What it costs is a
// layout check against that one part, which only an index written before layouts were carried can
// lack.
func TestUnknownLayoutOfAnUnreadablePartIsRaw(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	shared := backend.Memory()
	blind := &blindBackend{Backend: shared}

	a := engine.New(engine.Config{Backend: shared, Prefix: sharedPrefix, WriterID: "a"})
	require.NoError(t, a.LoadParts(ctx))
	b := engine.New(engine.Config{Backend: blind, Prefix: sharedPrefix, WriterID: "b"})
	require.NoError(t, b.LoadParts(ctx))

	for i := range int64(5) {
		mustAppend(t, a, mkSeries("job", "api"), 100+i, 1)
		mustAppend(t, b, mkSeries("job", "web"), 100+i, 1)
	}

	require.NoError(t, a.Flush(ctx))
	require.NoError(t, b.Flush(ctx))
	require.NoError(t, a.MergeWith(ctx, rolled(signal.AggCount)))
	forgetLayouts(t, shared)

	parts := a.Parts()

	hidden := make([]string, 0, len(parts))
	for _, p := range parts {
		hidden = append(hidden, p.ID)
	}

	blind.setHidden(hidden...)

	require.NoError(t, b.MergeWith(ctx, rolled(signal.AggSum)))

	var sum bool

	for _, ent := range committedIndex(t, shared).Entries {
		sum = sum || ent.Rollup != nil && len(ent.Rollup.Tiers) > 0 && ent.Rollup.Tiers[0].Agg == uint8(signal.AggSum)
	}

	assert.True(t, sum, "b's rollup committed: the unknown layout did not abort it")
}
