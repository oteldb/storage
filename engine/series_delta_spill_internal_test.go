package engine

import (
	"testing"

	"github.com/stretchr/testify/require"

	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

func TestSharedSeriesPostingSpillsPublishOneRoot(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		count  int
		target int64
	}{
		{name: "no-spill", count: 17, target: 64 << 20},
		{name: "single-level", count: seriesSpillFanIn, target: 1},
		{name: "multi-level", count: seriesSpillFanIn*2 + 1, target: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testSharedSeriesPostingSpill(t, tc.count, tc.target)
		})
	}
}

func testSharedSeriesPostingSpill(t *testing.T, count int, target int64) {
	t.Helper()
	files, err := backendfile.New(t.TempDir())
	require.NoError(t, err)
	e := separateCatalogEngine(files)
	t.Cleanup(func() { require.NoError(t, e.CloseWAL()) })
	e.cfg.FlushMemoryBytes = target
	entries := identityCacheInput(count)
	require.NoError(t, e.ensureSeriesIdentities(t.Context(), entries, nil))
	require.Len(t, e.seriesTable.roots, 1)
	root := e.seriesTable.roots[0]
	objects, err := files.List(t.Context(), root.binding.Prefix+"/")
	require.NoError(t, err)
	for _, key := range objects {
		require.NotContains(t, key, ".postings")
	}
	known, _, err := e.seriesTable.knownIdentities(t.Context(), entries)
	require.NoError(t, err)
	require.NotContains(t, known, false)
	for _, matchers := range [][]fetch.Matcher{nil, {{Name: []byte("instance"), Match: func(v signal.Value) bool { return v.Kind() == signal.KindStr }}}} {
		visited := 0
		require.NoError(t, e.seriesTable.visit(t.Context(), matchers, func(id signal.SeriesID, _ []byte) error {
			require.Equal(t, entries.view.ID(int(entries.refs[visited])), id)
			visited++
			return nil
		}))
		require.Equal(t, len(entries.refs), visited)
	}
}
