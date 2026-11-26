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
	files, err := backendfile.New(t.TempDir())
	require.NoError(t, err)
	e := separateCatalogEngine(files)
	t.Cleanup(func() { require.NoError(t, e.CloseWAL()) })
	e.cfg.FlushMemoryBytes = 1 // Every new ID spills, forcing a hierarchical reduction.
	entries := identityCacheInput(seriesSpillFanIn*2 + 1)
	require.NoError(t, e.ensureSeriesIdentities(t.Context(), entries, nil))
	require.Len(t, e.seriesTable.roots, 1)
	root := e.seriesTable.roots[0]
	objects, err := files.List(t.Context(), root.binding.Prefix+"/")
	require.NoError(t, err)
	for _, key := range objects {
		require.NotContains(t, key, ".postings")
	}
	known, err := e.seriesTable.knownIdentities(t.Context(), entries)
	require.NoError(t, err)
	require.NotContains(t, known, false)
	for _, matchers := range [][]fetch.Matcher{nil, {{Name: []byte("instance"), Match: func(v signal.Value) bool { return v.Kind() == signal.KindStr }}}} {
		visited := 0
		require.NoError(t, e.seriesTable.visit(t.Context(), matchers, func(id signal.SeriesID, raw []byte) error {
			require.Equal(t, entries.view.ID(int(entries.refs[visited])), id)
			visited++
			return nil
		}))
		require.Equal(t, len(entries.refs), visited)
	}
}
