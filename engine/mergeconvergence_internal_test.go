package engine

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/signal"
)

func TestMergeMostlyDisjointShortSeriesConvergence(t *testing.T) {
	t.Parallel()
	files, err := backendfile.New(t.TempDir())
	require.NoError(t, err)
	e, want := mostlyDisjointMergeFixture(t, files)
	changed, err := e.MergeStep(t.Context(), MergeOptions{Force: true})
	require.NoError(t, err)
	require.True(t, changed)
	plan := e.LastMergePlan()
	t.Logf("grant=%d source_parts=%d source_rows=%d selected=%d output_parts=%d remaining_parts=%d input_resident=%d open=%d output_floor=%d",
		disjointMergeGrant, disjointMergeInputs, disjointMergeInputs*disjointMergeSeries, plan.Selected, plan.OutputParts, e.PartCount(), plan.InputResidentBytes, plan.OpenBytes, plan.OutputFloorBytes)
	require.Less(t, plan.OutputParts, plan.Selected, "ordinary compaction must retire parts")
	require.Equal(t, disjointMergeInputs, plan.Selected, "exercise one merge of all mostly-disjoint sources")
	var scratch rangeBuf
	seen := make(map[signal.SeriesID]struct{}, len(want))
	for _, part := range e.parts {
		source, err := part.index.idSource(t.Context())
		require.NoError(t, err)
		stream, err := newPartStream(t.Context(), part, defaultMergeReadWindow)
		require.NoError(t, err)
		t.Cleanup(stream.close)
		for i := range source.Len() {
			id := source.At(i)
			ts, values, _, err := stream.decodeRange(source.RangeAt(i), &scratch)
			require.NoError(t, err)
			require.Equal(t, []int64{100}, ts)
			require.Equal(t, []float64{want[id]}, values, "the later source wins a duplicate timestamp")
			_, duplicate := seen[id]
			require.False(t, duplicate, "one logical series must not be duplicated across outputs")
			_, expected := want[id]
			require.True(t, expected)
			seen[id] = struct{}{}
		}
		stream.close()
	}
	require.Len(t, seen, len(want))
}

const (
	disjointMergeInputs = 6
	disjointMergeSeries = 32768
	disjointMergeGrant  = 80 << 20
)

func mostlyDisjointMergeFixture(t *testing.T, be backend.Backend) (*Engine, map[signal.SeriesID]float64) {
	t.Helper()
	e := New(Config{Backend: be, Prefix: "convergence", DiskSeries: true, SeparateSeries: true,
		MetricSeriesChunks: true, AggregateStats: true, ResidentHeadBytes: 256 << 20,
		FlushMemoryBytes: 256 << 20, MergeMemoryBytes: disjointMergeGrant, SeriesCacheBytes: 1 << 20})
	t.Cleanup(func() { require.NoError(t, e.CloseWAL()) })
	want := make(map[signal.SeriesID]float64, disjointMergeInputs*disjointMergeSeries)
	for generation := range disjointMergeInputs {
		identities := make([]signal.Series, disjointMergeSeries)
		ids := make([]signal.SeriesID, disjointMergeSeries)
		ts := make([]int64, disjointMergeSeries)
		values := make([]float64, disjointMergeSeries)
		for i := range identities {
			identity := generation*(disjointMergeSeries-1) + i
			if i == 0 {
				identity = 0
			}
			identities[i] = stagedTestSeries(strconv.Itoa(identity))
			ids[i] = identities[i].Hash()
			ts[i], values[i] = 100, float64(generation*disjointMergeSeries+i)
			want[ids[i]] = values[i]
		}
		result, err := e.AppendBatch(ids, ts, values, nil, func(i int) signal.Series { return identities[i] }, AppendLimits{})
		require.NoError(t, err)
		require.Equal(t, disjointMergeSeries, result.Accepted)
		require.NoError(t, e.Flush(t.Context()))
		require.Equal(t, generation+1, e.PartCount(), "each large flush must produce one source part")
	}
	return e, want
}
