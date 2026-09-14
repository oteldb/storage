package engine

import (
	"bytes"
	"fmt"
	"path"
	"slices"
	"strings"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/internal/vfs/faultfs"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"
)

var errInjectedWAL = errors.New("injected WAL write failure")

func stagedTestSeries(name string) signal.Series {
	return signal.Series{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("__name__"), Value: signal.StringValue([]byte(name))},
	)}
}

func newFaultWALEngine(t *testing.T, oooWindow int64) (*Engine, *faultfs.FS) {
	t.Helper()

	fsys := faultfs.New()

	w, err := wal.CreateFS(fsys, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	return New(Config{WAL: w, OOOWindow: oooWindow}), fsys
}

func failNextWALWrite(fsys *faultfs.FS) {
	fsys.Add(faultfs.Rule{
		Op:    faultfs.OpWrite,
		Match: func(c faultfs.Call) bool { return strings.HasSuffix(c.Name, ".wal") },
		Err:   errInjectedWAL,
		Times: 1,
	})
}

// replayedHead rebuilds a head from every WAL segment in fsys — a restart before any flush.
func replayedHead(t *testing.T, fsys *faultfs.FS) *head {
	t.Helper()

	entries, err := fsys.ReadDir(".")
	require.NoError(t, err)

	e := New(Config{})

	for _, ent := range entries {
		if path.Ext(ent.Name()) != ".wal" {
			continue
		}

		data, err := fsys.ReadFile(ent.Name())
		require.NoError(t, err)
		require.NoError(t, e.ApplyReplicated(data))
	}

	return e.head
}

func headTS(h *head, s signal.Series) []int64 {
	if buf := h.samples[s.Hash()]; buf != nil {
		return buf.ts
	}

	return nil
}

// TestFailedWALWriteLeavesHeadUntouched is #615: a batch whose log write fails must not reach the
// head, and a new series' retry must log its identity, or replay drops the acknowledged sample.
func TestFailedWALWriteLeavesHeadUntouched(t *testing.T) {
	t.Parallel()

	e, fsys := newFaultWALEngine(t, 50)
	known, fresh := stagedTestSeries("known"), stagedTestSeries("fresh")

	_, err := e.Append(known, 100, 1)
	require.NoError(t, err)

	before := e.Stats()

	failNextWALWrite(fsys)

	ids := []signal.SeriesID{known.Hash(), fresh.Hash()}
	mat := func(i int) signal.Series { return []signal.Series{known, fresh}[i] }

	res, err := e.AppendBatch(ids, []int64{1000, 1000}, []float64{2, 2}, nil, mat, AppendLimits{})
	require.ErrorIs(t, err, errInjectedWAL)
	assert.Equal(t, AppendResult{}, res)

	after := e.Stats()
	assert.Equal(t, before.HeadSamples, after.HeadSamples)
	assert.Equal(t, before.HeadBytes, after.HeadBytes)
	assert.Equal(t, before.Series, after.Series, "the failed batch registers no identity")

	// Had the failed batch raised known's watermark to 1000, a sample at 90 would be 910 behind a
	// 50-wide window and rejected.
	accepted, err := e.Append(known, 90, 3)
	require.NoError(t, err)
	assert.True(t, accepted, "the failed batch must not raise the out-of-order watermark")

	res, err = e.AppendBatch(ids, []int64{1000, 1000}, []float64{2, 2}, nil, mat, AppendLimits{})
	require.NoError(t, err)
	assert.Equal(t, 2, res.Accepted)

	replayed := replayedHead(t, fsys)
	assert.Equal(t, []int64{100, 90, 1000}, headTS(replayed, known))
	assert.Equal(t, []int64{1000}, headTS(replayed, fresh),
		"the new series' identity is logged on retry, so replay keeps its acknowledged sample")
	assert.Equal(t, headTS(e.head, known), headTS(replayed, known))
	assert.Equal(t, headTS(e.head, fresh), headTS(replayed, fresh))
}

// TestFailedSingleAppendLeavesHeadUntouched covers the one-sample path.
func TestFailedSingleAppendLeavesHeadUntouched(t *testing.T) {
	t.Parallel()

	e, fsys := newFaultWALEngine(t, 0)
	s := stagedTestSeries("up")

	failNextWALWrite(fsys)

	accepted, err := e.Append(s, 100, 1)
	require.ErrorIs(t, err, errInjectedWAL)
	assert.False(t, accepted)
	assert.Zero(t, e.Stats().HeadSamples)
	assert.Zero(t, e.Stats().Series)

	accepted, err = e.Append(s, 100, 1)
	require.NoError(t, err)
	assert.True(t, accepted)
	assert.Equal(t, []int64{100}, headTS(replayedHead(t, fsys), s))
}

// TestFailedApplyPrimaryLeavesHeadUntouched covers the cluster primary path.
func TestFailedApplyPrimaryLeavesHeadUntouched(t *testing.T) {
	t.Parallel()

	e, fsys := newFaultWALEngine(t, 0)
	a, b := stagedTestSeries("a"), stagedTestSeries("b")

	var frames bytes.Buffer

	fw := wal.NewWriter(&frames)
	for _, s := range []signal.Series{a, b} {
		require.NoError(t, fw.WriteSeries(s.Hash(), s))
		require.NoError(t, fw.WriteSamples(s.Hash(), []int64{10, 20}, []float64{1, 2}))
	}

	payload := frames.Bytes()

	failNextWALWrite(fsys)

	accepted, res, err := e.ApplyPrimary(payload, AppendLimits{})
	require.ErrorIs(t, err, errInjectedWAL)
	assert.Nil(t, accepted)
	assert.Equal(t, AppendResult{}, res)
	assert.Zero(t, e.Stats().HeadSamples)
	assert.Zero(t, e.Stats().Series)

	_, res, err = e.ApplyPrimary(payload, AppendLimits{})
	require.NoError(t, err)
	assert.Equal(t, 4, res.Accepted)

	replayed := replayedHead(t, fsys)
	assert.Equal(t, []int64{10, 20}, headTS(replayed, a))
	assert.Equal(t, []int64{10, 20}, headTS(replayed, b))
}

// headState is everything an append changes in a head, in a comparable form.
type headState struct {
	Samples      map[signal.SeriesID]sampleBuf
	SeriesNewest map[signal.SeriesID]int64
	Registered   []signal.SeriesID // in registration order
	Bytes        int64
	Newest       int64
}

func snapshotHead(h *head) headState {
	st := headState{
		Samples:      make(map[signal.SeriesID]sampleBuf, len(h.samples)),
		SeriesNewest: h.seriesNewest,
		Bytes:        h.bytes,
		Newest:       h.newest,
	}

	for id, buf := range h.samples {
		st.Samples[id] = sampleBuf{ts: slices.Clone(buf.ts), values: slices.Clone(buf.values), sf: slices.Clone(buf.sf)}
	}

	entries := h.series.Snapshot()
	for i := range entries {
		st.Registered = append(st.Registered, entries[i].ID)
	}

	return st
}

// FuzzStagedAdmissionMatchesDirect pins the staged write — decided in full, then applied — to
// [head.appendByID], which applies each sample as it admits it. Both engines take the same batches
// through the same limits: the out-of-order window, the in-flight cap, the hard cardinality cap and
// the soft budget's overflow routing, so a rejection followed by admitted samples, a series registered
// mid-batch and an overflow series created mid-batch are all common. The head each leaves must match
// field for field, and every batch must report the same result.
func FuzzStagedAdmissionMatchesDirect(f *testing.F) {
	f.Add([]byte{1, 10, 2, 20, 1, 5, 3, 30, 4, 1, 5, 2, 1, 40}, uint8(20), uint8(3), uint8(2), uint8(12))
	f.Add([]byte{0, 0, 0, 0, 7, 255, 7, 0}, uint8(0), uint8(0), uint8(0), uint8(0))

	f.Fuzz(func(t *testing.T, script []byte, ooo, maxSeries, softSeries, inFlight uint8) {
		limits := AppendLimits{
			MaxSeries:        int64(maxSeries % 8),
			MaxSeriesSoft:    int64(softSeries % 8),
			MaxInFlightBytes: int64(inFlight) * SampleBytes,
			Overflow:         func(signal.Series) signal.Series { return stagedTestSeries("overflow") },
		}

		staged, _ := newFaultWALEngine(t, int64(ooo))
		direct := New(Config{OOOWindow: int64(ooo)})

		for batch := range slices.Chunk(script, 6) {
			var (
				ids   []signal.SeriesID
				ts    []int64
				vals  []float64
				sf    []float64
				ident []signal.Series
			)

			for j := 0; j+1 < len(batch); j += 2 {
				s := stagedTestSeries(fmt.Sprintf("s%d", batch[j]%6))
				ids = append(ids, s.Hash())
				ident = append(ident, s)
				ts = append(ts, int64(batch[j+1]))
				vals = append(vals, float64(batch[j]))
				sf = append(sf, float64(1+batch[j+1]%2))
			}

			mat := func(i int) signal.Series { return ident[i] }

			got, err := staged.AppendBatch(ids, ts, vals, sf, mat, limits)
			require.NoError(t, err)

			want, err := direct.AppendBatch(ids, ts, vals, sf, mat, limits)
			require.NoError(t, err)

			require.Equal(t, want, got)
			require.Equal(t, snapshotHead(direct.head), snapshotHead(staged.head))
		}
	})
}
