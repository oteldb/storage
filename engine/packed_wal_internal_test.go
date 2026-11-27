package engine

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend/bucketindex"
	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/index/series"
	"github.com/oteldb/storage/internal/vfs/faultfs"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"
)

const packedWALTime = int64(1_700_000_000_000_000_000)

func appendPackedWALFixture(t *testing.T, e *Engine, descriptors []signal.Series, timestamp int64) (AppendResult, error) {
	t.Helper()
	ids := make([]signal.SeriesID, len(descriptors))
	ts := make([]int64, len(descriptors))
	values := make([]float64, len(descriptors))
	for i := range descriptors {
		ids[i], ts[i], values[i] = descriptors[i].Hash(), timestamp, float64(i+1)
	}
	return e.AppendBatch(ids, ts, values, nil, func(i int) signal.Series { return descriptors[i] }, AppendLimits{})
}

func TestPackedWALRequiresPublishedCapability(t *testing.T) {
	t.Parallel()
	files, err := backendfile.New(t.TempDir())
	require.NoError(t, err)
	gate := &generationGateBackend{Backend: files}
	e, fs := newReturningIDEngine(t, gate)
	defer func() { require.NoError(t, e.CloseWAL()) }()
	descriptors := []signal.Series{stagedTestSeries("first"), stagedTestSeries("second")}
	_, err = appendPackedWALFixture(t, e, descriptors, packedWALTime)
	require.NoError(t, err)
	require.NoError(t, e.Flush(t.Context()))
	require.True(t, e.compactMetricMetadata)
	require.Zero(t, e.metricFeatures)

	e.cfg.PackedWAL = true
	e.mu.Lock()
	gate.commitFail.Store(true)
	landed, err := e.commitOnceLocked(t.Context(), e.parts, e.flushedEpoch, false)
	e.mu.Unlock()
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.False(t, landed)
	require.Zero(t, e.metricFeatures)

	result, err := appendPackedWALFixture(t, e, descriptors, packedWALTime+200)
	require.NoError(t, err)
	require.Equal(t, 2, result.Accepted)
	entries, err := fs.ReadDir(".")
	require.NoError(t, err)
	require.Len(t, entries, 1)
	journal, err := fs.ReadFile(entries[0].Name())
	require.NoError(t, err)
	refs := 0
	require.NoError(t, wal.Replay(journal, wal.Handlers{
		OnDescriptorReference: func(signal.SeriesID) error { refs++; return nil },
	}))
	require.Equal(t, 2, refs, "the unfenced WAL still uses singleton reference frames")
}

func TestPackedWALReplaysAfterShortWrite(t *testing.T) {
	t.Parallel()
	files, err := backendfile.New(t.TempDir())
	require.NoError(t, err)
	e, fs := newReturningIDEngine(t, files)
	e.cfg.PackedWAL = true
	defer func() { require.NoError(t, e.CloseWAL()) }()
	descriptors := []signal.Series{stagedTestSeries("alpha"), stagedTestSeries("beta"), stagedTestSeries("gamma")}
	_, err = appendPackedWALFixture(t, e, descriptors, packedWALTime)
	require.NoError(t, err)
	require.NoError(t, e.Flush(t.Context()))
	require.NotZero(t, e.metricFeatures&bucketindex.MetricFeaturePackedWAL)

	fs.Add(faultfs.Rule{
		Op:    faultfs.OpWrite,
		Match: func(c faultfs.Call) bool { return strings.HasSuffix(c.Name, ".wal") },
		Err:   errInjectedWAL,
		Short: 19,
		Times: 1,
	})
	_, err = appendPackedWALFixture(t, e, descriptors, packedWALTime+100)
	require.ErrorIs(t, err, errInjectedWAL)
	for i := range descriptors {
		require.Empty(t, headTS(e.head, descriptors[i]))
	}

	result, err := appendPackedWALFixture(t, e, descriptors, packedWALTime+200)
	require.NoError(t, err)
	require.Equal(t, len(descriptors), result.Accepted)
	entries, err := fs.ReadDir(".")
	require.NoError(t, err)
	var journal []byte
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".wal") {
			segment, err := fs.ReadFile(entry.Name())
			require.NoError(t, err)
			journal = append(journal, segment...)
		}
	}
	refs := 0
	require.NoError(t, wal.Replay(journal, wal.Handlers{
		OnPackedReference: func(signal.SeriesID, int64, float64) error { refs++; return nil },
	}))
	require.Equal(t, len(descriptors), refs)
	require.ErrorIs(t, wal.Replay(journal, wal.Handlers{}), wal.ErrCorrupt)

	cfg := e.cfg
	cfg.WAL, cfg.PackedWAL = nil, false
	reopened := New(cfg)
	defer func() { require.NoError(t, reopened.CloseWAL()) }()
	require.NoError(t, reopened.LoadPartsReadOnly(t.Context()))
	require.NotZero(t, reopened.metricFeatures&bucketindex.MetricFeaturePackedWAL)
	require.NoError(t, wal.Replay(journal, reopened.salvageHandlers(t.Context(), "")))
	for i := range descriptors {
		require.Equal(t, []int64{packedWALTime + 200}, headTS(reopened.head, descriptors[i]))
	}
}

func TestPackedWALMixedBatchAndReplication(t *testing.T) {
	t.Parallel()
	b := newWALBatch()
	names := []string{"a", "b", "cold", "weighted", "c", "d"}
	for i, name := range names {
		s := stagedTestSeries(name)
		b.order = append(b.order, s.Hash())
		a := walSeriesAcc{
			series: s, isNew: true, register: true,
			descriptorRef: name != "cold", encodedHead: name != "cold",
			ts: []int64{packedWALTime}, values: []float64{float64(i)},
		}
		if name == "weighted" {
			a.sampled = true
			a.sf = []float64{2}
		}
		b.accs = append(b.accs, a)
	}
	require.NoError(t, b.encode(series.View{}, false, true))
	require.Equal(t, 2, b.loggedPackedBlocks)
	require.Equal(t, 5, b.loggedReferences)
	var got []signal.SeriesID
	h := wal.Handlers{
		OnDescriptorReference: func(signal.SeriesID) error { return nil },
		OnSamples: func(id signal.SeriesID, _ []int64, _ []float64) error {
			got = append(got, id)
			return nil
		},
		OnSamplesSF: func(id signal.SeriesID, _ []int64, _ []float64, sf []float64) error {
			require.Equal(t, []float64{2}, sf)
			got = append(got, id)
			return nil
		},
		OnPackedReference: func(id signal.SeriesID, _ int64, _ float64) error {
			got = append(got, id)
			return nil
		},
	}
	require.NoError(t, wal.Replay(b.frames.Bytes(), h))
	require.Equal(t, b.order, got)
	for i, id := range b.order {
		a := &b.accs[i]
		if a.descriptorRef {
			raw := b.descriptors[a.rawStart : a.rawStart+a.rawSize]
			require.Equal(t, id, signal.HashBytes(raw))
		}
	}

	require.NoError(t, b.encode(series.View{}, true, true))
	require.Zero(t, b.loggedPackedBlocks)
	require.Zero(t, b.loggedReferences)
	h.OnDescriptorReference, h.OnPackedReference = nil, nil
	got = got[:0]
	seriesFrames := 0
	h.OnSeries = func(id signal.SeriesID, s signal.Series) error {
		require.Equal(t, s.Hash(), id)
		seriesFrames++
		return nil
	}
	require.NoError(t, wal.Replay(b.frames.Bytes(), h))
	require.Equal(t, len(b.order), seriesFrames)
	require.Equal(t, b.order, got)
}
