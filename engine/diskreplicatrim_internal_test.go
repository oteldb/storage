package engine

import (
	"math"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/faultbackend"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"
)

type replicaTestSample struct {
	ts        int64
	value, sf float64
}

func replicaSamplePayload(t *testing.T, s signal.Series, points ...replicaTestSample) []byte {
	t.Helper()
	return primaryPayload(t, func(w *wal.Writer) error {
		if err := w.WriteSeries(s.Hash(), s); err != nil {
			return err
		}
		ts, values, sf := make([]int64, len(points)), make([]float64, len(points)), make([]float64, len(points))
		for i, p := range points {
			ts[i], values[i], sf[i] = p.ts, p.value, p.sf
		}
		return w.WriteSamplesSF(s.Hash(), ts, values, sf)
	})
}

func TestDiskReplicaRefreshPreservesLaterAcceptedSamples(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name          string
		durable, late replicaTestSample
	}{
		{"OOO", replicaTestSample{100, 1, 1}, replicaTestSample{95, 2, 1}},
		{"same timestamp update", replicaTestSample{100, 1, 1}, replicaTestSample{100, 2, 1}},
		{"signed zero", replicaTestSample{100, 0, 1}, replicaTestSample{100, math.Copysign(0, -1), 1}},
		{"NaN payload", replicaTestSample{100, math.Float64frombits(0x7ff8000000000001), 1}, replicaTestSample{100, math.Float64frombits(0x7ff8000000000002), 1}},
		{"scale factor", replicaTestSample{100, 1, 2}, replicaTestSample{100, 1, 3}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			be := backend.Memory()
			owner := New(Config{Backend: be, Prefix: "safe-trim", DiskSeries: true, WriterID: "primary", Term: func() uint64 { return 7 }, OOOWindow: 10})
			s := benchSeries(1)
			_, result, err := owner.ApplyPrimary(replicaSamplePayload(t, s, tc.durable), AppendLimits{})
			require.NoError(t, err)
			require.Equal(t, 1, result.Accepted)
			require.NoError(t, owner.Flush(t.Context()))
			accepted, result, err := owner.ApplyPrimary(replicaSamplePayload(t, s, tc.late), AppendLimits{})
			require.NoError(t, err)
			require.Equal(t, 1, result.Accepted)
			var term atomic.Uint64
			replica := New(Config{Backend: be, Prefix: "safe-trim", DiskSeries: true, WriterID: "replica", Term: term.Load, OOOWindow: 10})
			require.NoError(t, replica.LoadPartsUnclaimed(t.Context()))
			require.NoError(t, replica.ApplyReplicated(accepted))
			replica.cfg.ResidentHeadBytes = 1
			require.ErrorIs(t, replica.ApplyReplicated(replicaSamplePayload(t, benchSeries(2), replicaTestSample{101, 3, 1})), ErrReplicaHeadFull)
			require.False(t, replica.Stats().IndexFenced)
			buf := replica.head.samples[s.Hash()]
			require.NotNil(t, buf)
			require.Equal(t, []int64{tc.late.ts}, buf.ts)
			require.Equal(t, math.Float64bits(tc.late.value), math.Float64bits(buf.values[0]))
			require.Equal(t, math.Float64bits(tc.late.sf), math.Float64bits(bufSF(buf, 0)))
			// Promotion must persist the accepted tail that the previous primary never flushed.
			term.Store(8)
			require.NoError(t, replica.Flush(t.Context()))
			reopened := New(Config{Backend: be, Prefix: "safe-trim", DiskSeries: true, WriterID: "replica"})
			require.NoError(t, reopened.LoadParts(t.Context()))
			it, err := reopened.Fetch(t.Context(), fetch.Request{Start: tc.late.ts, End: tc.late.ts})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, it.Close()) })
			batches, err := fetch.Drain(t.Context(), it)
			require.NoError(t, err)
			require.Len(t, batches, 1)
			require.Len(t, batches[0].Values, 1)
			require.Equal(t, math.Float64bits(tc.late.value), math.Float64bits(batches[0].Values[0]))
			weight := 1.0
			if batches[0].ScaleFactors != nil {
				weight = batches[0].ScaleFactors[0]
			}
			require.Equal(t, math.Float64bits(tc.late.sf), math.Float64bits(weight))
		})
	}
}

func TestDiskReplicaRefreshRequiresUnambiguousPersistedSamples(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		parts [][]replicaTestSample
		head  []replicaTestSample
		want  int
	}{
		{"exact", [][]replicaTestSample{{{100, 1, 1}}}, []replicaTestSample{{100, 1, 1}}, 0},
		{"head conflict", [][]replicaTestSample{{{100, 1, 1}}}, []replicaTestSample{{100, 2, 1}, {100, 1, 1}}, 2},
		{"part conflict", [][]replicaTestSample{{{100, 1, 1}}, {{100, 2, 1}}}, []replicaTestSample{{100, 1, 1}}, 1},
		{"weighted tail", [][]replicaTestSample{{{100, 1, 3}}}, []replicaTestSample{{100, 1, 3}, {95, 2, 4}}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			be := backend.Memory()
			owner := New(Config{Backend: be, Prefix: "exact-trim", DiskSeries: true})
			s := benchSeries(1)
			for _, points := range tc.parts {
				require.NoError(t, owner.ApplyReplicated(replicaSamplePayload(t, s, points...)))
				require.NoError(t, owner.Flush(t.Context()))
			}
			replica := New(Config{Backend: be, Prefix: "exact-trim", DiskSeries: true})
			require.NoError(t, replica.ApplyReplicated(replicaSamplePayload(t, s, tc.head...)))
			require.NoError(t, replica.RefreshReplica(t.Context()))
			require.Equal(t, tc.want, replica.HeadSampleCount())
			if tc.name == "weighted tail" {
				buf := replica.head.samples[s.Hash()]
				require.Equal(t, []int64{95}, buf.ts)
				require.Equal(t, []float64{4}, buf.sf, "retaining an OOO tail preserves its aligned weight")
			}
		})
	}
}

func TestDiskReplicaRetainsUnsupportedCoverageProofs(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"rolled part", "oversized series"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			be := backend.Memory()
			s := benchSeries(1)
			owner := New(Config{Backend: be, Prefix: "unsupported-trim", DiskSeries: true, WriterID: "primary"})
			ok, err := owner.Append(s, 100, 1)
			require.NoError(t, err)
			require.True(t, ok)
			require.NoError(t, owner.Flush(t.Context()))
			points := []replicaTestSample{{100, 1, 1}}
			if mode == "rolled part" {
				require.NoError(t, owner.MergeWith(t.Context(), MergeOptions{Force: true, Downsample: []DownsampleTier{{Before: 1000, Interval: 10, Agg: signal.AggLast}}}))
			} else {
				points = make([]replicaTestSample, maxReplicaTrimSamples+1)
				for i := range points {
					points[i] = replicaTestSample{100, 1, 1}
				}
			}
			replica := New(Config{Backend: be, Prefix: "unsupported-trim", DiskSeries: true, WriterID: "replica", Term: func() uint64 { return 0 }})
			require.NoError(t, replica.LoadPartsUnclaimed(t.Context()))
			require.NoError(t, replica.ApplyReplicated(replicaSamplePayload(t, s, points...)))
			replica.cfg.ResidentHeadBytes = 1
			require.ErrorIs(t, replica.ApplyReplicated(replicaSamplePayload(t, s, replicaTestSample{101, 2, 1})), ErrReplicaHeadFull)
			require.Equal(t, len(points), replica.HeadSampleCount())
		})
	}
}

func TestDiskReplicaProofFailureKeepsAccountingCurrent(t *testing.T) {
	t.Parallel()
	be := faultbackend.Wrap(backend.Memory())
	owner := New(Config{Backend: be, Prefix: "proof-failure", DiskSeries: true})
	a, b := benchSeries(1), benchSeries(2)
	for i, s := range []signal.Series{a, b} {
		ok, err := owner.Append(s, int64(100+i), float64(1+i))
		require.NoError(t, err)
		require.True(t, ok)
	}
	require.NoError(t, owner.Flush(t.Context()))
	replica := New(Config{Backend: be, Prefix: "proof-failure", DiskSeries: true})
	require.NoError(t, replica.LoadPartsUnclaimed(t.Context()))
	require.NoError(t, replica.ApplyReplicated(replicaSamplePayload(t, a, replicaTestSample{100, 1, 1})))
	require.NoError(t, replica.ApplyReplicated(replicaSamplePayload(t, b, replicaTestSample{101, 2, 1})))
	// Stop the second identity's proof after the first identity has safely drained, independent of map order.
	be.Add(faultbackend.Rule{Kind: faultbackend.Read, Err: errInjectedWAL, Match: func(op faultbackend.Op) bool {
		samples := 0
		for _, buf := range replica.head.samples {
			samples += len(buf.ts)
		}
		return strings.Contains(op.Key, "/c/") && samples < 2
	}})
	require.ErrorIs(t, replica.RefreshReplica(t.Context()), errInjectedWAL)
	require.Equal(t, 1, replica.HeadSampleCount())
	require.EqualValues(t, 1, replica.Stats().HeadSamples)
	require.EqualValues(t, SampleBytes, replica.HeadBytes(), "a later proof error cannot leave stale accounting for safely trimmed samples")
}
