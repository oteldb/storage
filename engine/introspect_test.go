package engine_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/wal"
)

// labelCard finds a label name in a cardinality result, failing the test if it is absent.
func labelCard(t *testing.T, cs engine.CardinalityStat, name string) engine.LabelCard {
	t.Helper()

	for _, l := range cs.Top {
		if l.Name == name {
			return l
		}
	}

	t.Fatalf("label %q not found in cardinality %+v", name, cs.Top)

	return engine.LabelCard{}
}

func TestParts(t *testing.T) {
	t.Parallel()

	e := flushEngine()
	a := mkSeries("job", "api", "inst", "1")
	b := mkSeries("job", "api", "inst", "2")
	c := mkSeries("job", "web")

	mustAppend(t, e, a, 100, 1.0)
	mustAppend(t, e, a, 200, 2.0)
	mustAppend(t, e, b, 150, 1.0)
	mustAppend(t, e, c, 120, 9.0)

	require.Empty(t, e.Parts(), "no parts before flush")
	require.NoError(t, e.Flush(context.Background()))

	parts := e.Parts()
	require.Len(t, parts, 1)
	assert.Equal(t, 3, parts[0].Series, "three distinct series")
	assert.Equal(t, int64(4), parts[0].Rows, "four samples total")
	assert.Equal(t, int64(100), parts[0].MinTime)
	assert.Equal(t, int64(200), parts[0].MaxTime)
	assert.NotEmpty(t, parts[0].ID)

	// A second flush adds a second part.
	mustAppend(t, e, a, 300, 3.0)
	require.NoError(t, e.Flush(context.Background()))
	assert.Len(t, e.Parts(), 2)
}

func TestPartsDetailed(t *testing.T) {
	t.Parallel()

	e := flushEngine()
	mustAppend(t, e, mkSeries("job", "api"), 100, 1.0)
	mustAppend(t, e, mkSeries("job", "api"), 200, 2.0)
	require.NoError(t, e.Flush(context.Background()))

	ds, err := e.PartsDetailed(context.Background())
	require.NoError(t, err)
	require.Len(t, ds, 1)

	d := ds[0]
	assert.Equal(t, int64(2), d.Rows)
	assert.Positive(t, d.Bytes, "part has on-backend bytes")
	assert.GreaterOrEqual(t, d.Chunks, 1, "at least one granule")
	require.NotEmpty(t, d.Columns)

	names := make(map[string]engine.ColumnStat, len(d.Columns))
	for _, c := range d.Columns {
		names[c.Name] = c
	}

	for _, want := range []string{"series", "ts", "value"} {
		assert.Contains(t, names, want, "part carries the %q column", want)
	}
}

func TestCardinality(t *testing.T) {
	t.Parallel()

	e := flushEngine()
	mustAppend(t, e, mkSeries("job", "api", "inst", "1"), 100, 1.0)
	mustAppend(t, e, mkSeries("job", "api", "inst", "2"), 100, 1.0)
	mustAppend(t, e, mkSeries("job", "web"), 100, 1.0)

	cs := e.Cardinality(0)
	assert.Equal(t, int64(3), cs.TotalSeries)
	assert.Positive(t, cs.SymbolCount)
	assert.Equal(t, 2, cs.DistinctLabelNames, "job and inst")

	job := labelCard(t, cs, "job")
	assert.Equal(t, int64(3), job.Series, "all three series carry job")
	assert.Equal(t, 2, job.DistinctValues, "api and web")

	inst := labelCard(t, cs, "inst")
	assert.Equal(t, int64(2), inst.Series, "only two series carry inst")
	assert.Equal(t, 2, inst.DistinctValues)

	// Cardinality spans the head ∪ flushed parts.
	require.NoError(t, e.Flush(context.Background()))
	assert.Equal(t, int64(3), e.Cardinality(0).TotalSeries, "identity survives flush")

	// topN bounds and ranks the result: job (3) outranks inst (2).
	top := e.Cardinality(1)
	require.Len(t, top.Top, 1)
	assert.Equal(t, "job", top.Top[0].Name)
}

func TestMergeRunningAndBacklog(t *testing.T) {
	t.Parallel()

	e := flushEngine()
	assert.False(t, e.MergeRunning())
	assert.Equal(t, 0, e.MergeBacklog())

	mustAppend(t, e, mkSeries("job", "api"), 100, 1.0)
	require.NoError(t, e.Flush(context.Background()))
	mustAppend(t, e, mkSeries("job", "api"), 200, 2.0)
	require.NoError(t, e.Flush(context.Background()))
	assert.Equal(t, 2, e.MergeBacklog())

	require.NoError(t, e.MergeWith(context.Background(), engine.MergeOptions{}))
	assert.False(t, e.MergeRunning(), "flag is cleared after the merge returns")
	assert.Equal(t, 1, e.MergeBacklog(), "two parts compacted into one")
}

func TestWALState(t *testing.T) {
	t.Parallel()

	// The ephemeral engine has no WAL.
	segs, bytes, epoch, ok := flushEngine().WALState()
	assert.False(t, ok)
	assert.Zero(t, segs)
	assert.Zero(t, bytes)
	assert.Zero(t, epoch)

	sw, err := wal.Create(t.TempDir(), 0)
	require.NoError(t, err)

	e := engine.New(engine.Config{WAL: sw, Backend: nil})
	// Release the open WAL segment handle before the test ends so t.TempDir cleanup can remove it on
	// Windows (which refuses to delete a file held open by a live process).
	t.Cleanup(func() { _ = e.CloseWAL() })
	segs, _, _, ok = e.WALState()
	require.True(t, ok)
	assert.Equal(t, 0, segs, "no segment opened before the first write")

	mustAppend(t, e, mkSeries("job", "api"), 100, 1.0)
	segs, bytes, epoch, ok = e.WALState()
	require.True(t, ok)
	assert.Equal(t, 1, segs, "first append opens the first segment")
	assert.Positive(t, bytes, "segment has bytes after a write")
	assert.Positive(t, epoch, "active flush generation is non-zero")
}
