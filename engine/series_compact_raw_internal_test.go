package engine

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend/bucketindex"
	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/internal/seriestable"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

func TestRawDescriptorMergeReusesPostingsWithDuplicates(t *testing.T) {
	t.Parallel()
	be, err := backendfile.New(t.TempDir())
	require.NoError(t, err)
	e := separateCatalogEngine(be)
	t.Cleanup(func() { require.NoError(t, e.CloseWAL()) })
	input := identityCacheInput(6)
	src := make([]*seriesRootHandle, 0, 2)
	for i, refs := range [][]uint32{input.refs[:4], input.refs[2:]} {
		b := seriestable.NewBorrowedBuilder(input.view)
		for _, ref := range refs {
			require.NoError(t, b.AddBorrowed(int(ref)))
		}
		prefix := "source-" + strconv.Itoa(i)
		run, err := seriestable.Write(t.Context(), be, prefix, &b)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, run.Close()) })
		src = append(src, &seriesRootHandle{binding: bucketindex.SeriesRoot{Prefix: prefix, ManifestDigest: run.Digest()}, run: run})
	}
	out, err := e.buildSeriesMerge(t.Context(), src, 64<<20)
	require.NoError(t, err)
	defer e.discardSeriesOutputs(t.Context(), out)
	require.Len(t, out, 1)
	require.EqualValues(t, 6, out[0].run.Count())

	for i := range 6 {
		label := strconv.Itoa(i)
		it, err := out[0].run.Resolve(t.Context(), []fetch.Matcher{{
			Name: []byte("instance"), Match: func(v signal.Value) bool { return string(v.Str()) == label },
		}})
		require.NoError(t, err)
		require.True(t, it.Next())
		require.Equal(t, benchSeries(i).Hash(), it.ID())
		require.False(t, it.Next())
		require.NoError(t, it.Err())
		require.NoError(t, it.Close())
	}
}

func TestDescriptorMergeHistorySkipsSameInputsAndBudget(t *testing.T) {
	t.Parallel()
	be, err := backendfile.New(t.TempDir())
	require.NoError(t, err)
	e := compactSeriesFixture(t, be)
	const allowance = 128 << 20
	group, scratch, noGain := e.pickDescriptorMerge(e.seriesTable.roots, 8, allowance)
	require.Len(t, group, 8)
	require.Positive(t, scratch)
	e.seriesNoGain.remember(noGain)
	next, _, retry := e.pickDescriptorMerge(e.seriesTable.roots, 8, allowance)
	require.Len(t, next, 7)
	require.NotEqual(t, noGain, retry)
	next, _, retry = e.pickDescriptorMerge(e.seriesTable.roots, 8, allowance+1)
	require.Equal(t, group, next)
	require.NotEqual(t, noGain, retry)
}
