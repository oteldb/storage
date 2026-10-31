package engine

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

func TestCountMemoryExistenceAvoidsPartReads(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name       string
		start, end int64
	}{{"covered", 0, 200}, {"edge", 50, 200}} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			counter := backendtest.NewByteCounter(backend.Memory())
			e := New(Config{Backend: counter, Prefix: "memory-count", DecodeBudget: NewDecodeBudget(4096)})
			for _, ts := range []int64{0, 100} {
				_, err := e.Append(apiSeries(), ts, 1)
				require.NoError(t, err)
			}
			require.NoError(t, e.Flush(t.Context()))
			_, err := e.Append(apiSeries(), 150, 2)
			require.NoError(t, err)
			require.Len(t, e.parts, 1)
			require.NotNil(t, e.parts[0].index.paged)
			e.parts[0].index.paged.view.Store(nil)
			counter.Reset()
			req := fetch.Request{Start: tt.start, End: tt.end}
			count, err := e.Count(t.Context(), req)
			require.NoError(t, err)
			require.Equal(t, 1, count)
			by, err := e.CountBy(t.Context(), req, []byte("job"))
			require.NoError(t, err)
			require.Equal(t, map[string]int{"api": 1}, by)
			require.Zero(t, counter.Bytes(), counter.Report())
			require.Zero(t, e.budget.inFlight())
		})
	}
}

func TestCountStopsAfterCompletePartProof(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name    string
		ids     []signal.SeriesID
		wantErr bool
	}{
		{name: "complete", ids: []signal.SeriesID{{Lo: 1}}},
		{name: "unresolved", ids: []signal.SeriesID{{Lo: 1}, {Lo: 2}}, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			counter := backendtest.NewByteCounter(backend.Memory())
			plan := enginePlan{
				ids: tt.ids, memActive: make([]bool, len(tt.ids)), start: 0, end: 10,
				liveParts: []*part{
					{minTime: 1, maxTime: 1, index: partIndex{ids: []signal.SeriesID{{Lo: 1}}, starts: []int32{0, 1}}},
					{minTime: 2, maxTime: 2, index: partIndex{paged: &pagedIndex{be: counter, key: "absent", n: 4096}}},
				},
			}
			active, err := plan.activeFlags(t.Context(), plan.ids, nil)
			if tt.wantErr {
				require.Error(t, err, "an unproven series still requires later parts")
				return
			}
			require.NoError(t, err)
			require.Equal(t, []bool{true}, active)
			require.Zero(t, counter.Reads(), "a complete existence proof needs no later index reads")
		})
	}
}
