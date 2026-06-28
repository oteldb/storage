package recordengine_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/wal"
)

func labelCard(t *testing.T, cs recordengine.CardinalityStat, name string) recordengine.LabelCard {
	t.Helper()

	for _, l := range cs.Top {
		if l.Name == name {
			return l
		}
	}

	t.Fatalf("label %q not found in cardinality %+v", name, cs.Top)

	return recordengine.LabelCard{}
}

func TestParts(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := newEngine(t, backend.Memory())

	ingest(t, e, mkBatch("api", rrec{ts: 100, body: "a"}, rrec{ts: 200, body: "b"}))
	ingest(t, e, mkBatch("web", rrec{ts: 150, body: "c"}))

	require.Empty(t, e.Parts(), "no parts before flush")
	require.NoError(t, e.Flush(ctx))

	parts := e.Parts()
	require.Len(t, parts, 1)
	assert.Equal(t, 2, parts[0].Series, "two streams")
	assert.Equal(t, int64(3), parts[0].Rows, "three records")
	assert.Equal(t, int64(100), parts[0].MinTime)
	assert.Equal(t, int64(200), parts[0].MaxTime)
	assert.NotEmpty(t, parts[0].ID)
}

func TestPartsDetailed(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := newEngine(t, backend.Memory())
	ingest(t, e, mkBatch("api", rrec{ts: 100, body: "a"}, rrec{ts: 200, body: "b"}))
	require.NoError(t, e.Flush(ctx))

	ds, err := e.PartsDetailed(ctx)
	require.NoError(t, err)
	require.Len(t, ds, 1)

	d := ds[0]
	assert.Equal(t, int64(2), d.Rows)
	assert.Positive(t, d.Bytes)
	assert.GreaterOrEqual(t, d.Chunks, 1)
	require.NotEmpty(t, d.Columns)

	names := make(map[string]struct{}, len(d.Columns))
	for _, c := range d.Columns {
		names[c.Name] = struct{}{}
	}

	assert.Contains(t, names, "body", "schema column present")
}

func TestCardinality(t *testing.T) {
	t.Parallel()

	e := newEngine(t, backend.Memory())
	ingest(t, e, mkBatch("api", rrec{ts: 100, body: "a"}))
	ingest(t, e, mkBatch("web", rrec{ts: 100, body: "b"}))

	cs := e.Cardinality(0)
	assert.Equal(t, int64(2), cs.TotalSeries, "two streams")
	assert.Positive(t, cs.SymbolCount)

	svc := labelCard(t, cs, "service.name")
	assert.Equal(t, int64(2), svc.Series)
	assert.Equal(t, 2, svc.DistinctValues, "api and web")
}

func TestMergeRunningAndBacklog(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := newEngine(t, backend.Memory())
	assert.False(t, e.MergeRunning())
	assert.Equal(t, 0, e.MergeBacklog())

	ingest(t, e, mkBatch("api", rrec{ts: 100, body: "a"}))
	require.NoError(t, e.Flush(ctx))
	ingest(t, e, mkBatch("api", rrec{ts: 200, body: "b"}))
	require.NoError(t, e.Flush(ctx))
	assert.Equal(t, 2, e.MergeBacklog())

	require.NoError(t, e.Merge(ctx, 0))
	assert.False(t, e.MergeRunning())
	assert.Equal(t, 1, e.MergeBacklog(), "two parts compacted into one")
}

func TestWALState(t *testing.T) {
	t.Parallel()

	segs, bytes, epoch, ok := newEngine(t, backend.Memory()).WALState()
	assert.False(t, ok, "no WAL configured")
	assert.Zero(t, segs)
	assert.Zero(t, bytes)
	assert.Zero(t, epoch)

	sw, err := wal.Create(t.TempDir(), 0)
	require.NoError(t, err)

	e := recordengine.New(recordengine.Config{Schema: testSchema, WAL: sw, Prefix: "t/recs"})
	ingest(t, e, mkBatch("api", rrec{ts: 100, body: "a"}))

	segs, bytes, epoch, ok = e.WALState()
	require.True(t, ok)
	assert.GreaterOrEqual(t, segs, 1, "a write opened a segment")
	assert.Positive(t, bytes)
	assert.Positive(t, epoch)
}
