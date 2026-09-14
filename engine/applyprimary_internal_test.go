package engine

import (
	"bytes"
	"fmt"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/internal/vfs/faultfs"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"
)

func primaryPayload(t *testing.T, runs ...func(w *wal.Writer) error) []byte {
	t.Helper()

	var buf bytes.Buffer

	w := wal.NewWriter(&buf)
	for _, run := range runs {
		require.NoError(t, run(w))
	}

	return buf.Bytes()
}

func seriesRun(s signal.Series, ts []int64, values []float64) func(w *wal.Writer) error {
	return func(w *wal.Writer) error {
		if err := w.WriteSeries(s.Hash(), s); err != nil {
			return err
		}

		return w.WriteSamples(s.Hash(), ts, values)
	}
}

func TestApplyPrimaryMalformedPayloadAppliesNothing(t *testing.T) {
	t.Parallel()

	e, fsys := newFaultWALEngine(t, 0)
	a, b := stagedTestSeries("a"), stagedTestSeries("b")

	payload := primaryPayload(t, seriesRun(a, []int64{10}, []float64{1}), seriesRun(b, []int64{20}, []float64{2}))

	accepted, res, err := e.ApplyPrimary(payload[:len(payload)-1], AppendLimits{})
	require.ErrorIs(t, err, wal.ErrCorrupt)
	assert.Nil(t, accepted)
	assert.Equal(t, AppendResult{}, res)
	assert.Zero(t, e.Stats().HeadSamples, "the payload's valid prefix is not applied either")
	assert.Zero(t, e.Stats().Series)
	assert.Empty(t, headTS(replayedHead(t, fsys), a), "nor logged")
}

// TestApplyPrimaryFramesCarryIdentityForAFreshReplica: a replica that joins, or restarts, between two
// writes to a series the primary already buffers still gets the series record with the second one.
func TestApplyPrimaryFramesCarryIdentityForAFreshReplica(t *testing.T) {
	t.Parallel()

	for _, logged := range []bool{false, true} {
		t.Run(fmt.Sprintf("WAL=%v", logged), func(t *testing.T) {
			t.Parallel()

			primary := New(Config{})
			if logged {
				primary, _ = newFaultWALEngine(t, 0)
			}

			s := stagedTestSeries("up")

			_, _, err := primary.ApplyPrimary(primaryPayload(t, seriesRun(s, []int64{10}, []float64{1})), AppendLimits{})
			require.NoError(t, err)

			accepted, _, err := primary.ApplyPrimary(primaryPayload(t, seriesRun(s, []int64{20}, []float64{2})), AppendLimits{})
			require.NoError(t, err)

			fresh := New(Config{})
			require.NoError(t, fresh.ApplyReplicated(accepted))
			assert.Equal(t, []int64{20}, headTS(fresh.head, s))
		})
	}
}

// TestStagedWriteAgesHeadFromAdmission: a write that fills an empty head starts the head's age when it
// is admitted, not when its log write returns, so a slow disk does not make the head look younger.
func TestStagedWriteAgesHeadFromAdmission(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		e, fsys := newFaultWALEngine(t, 0)

		const logLatency = time.Minute

		fsys.Add(faultfs.Rule{
			Op:     faultfs.OpWrite,
			Match:  func(c faultfs.Call) bool { return strings.HasSuffix(c.Name, ".wal") },
			Before: func(faultfs.Call) { time.Sleep(logLatency) },
		})

		_, err := e.Append(stagedTestSeries("up"), 10, 1)
		require.NoError(t, err)

		assert.GreaterOrEqual(t, e.head.age(), logLatency)
	})
}

// FuzzApplyPrimaryReplicatesHead drives the primary path through the admission limits — overflow
// routing included — with repeated and sampled runs of a series in one payload, with and without a
// log. The primary must admit each run as appending it directly would, and its accepted frames must
// rebuild its head on a replica that saw every write, and on a fresh one the write's own series.
func FuzzApplyPrimaryReplicatesHead(f *testing.F) {
	f.Add([]byte{1, 1, 10, 20, 5, 2, 0, 30, 31, 32, 1, 0, 3, 4, 50, 3, 1, 60, 1, 2}, uint8(20), uint8(3), uint8(2), uint8(30), true)
	f.Add([]byte{0, 0, 0, 0, 0, 7, 1, 255, 7, 0}, uint8(0), uint8(0), uint8(0), uint8(0), false)

	f.Fuzz(func(t *testing.T, script []byte, ooo, maxSeries, softSeries, inFlight uint8, logged bool) {
		limits := AppendLimits{
			MaxSeries:        int64(maxSeries % 8),
			MaxSeriesSoft:    int64(softSeries % 8),
			MaxInFlightBytes: int64(inFlight) * SampleBytes,
			Overflow:         func(signal.Series) signal.Series { return stagedTestSeries("overflow") },
		}

		var fsys *faultfs.FS

		primary := New(Config{OOOWindow: int64(ooo)})
		if logged {
			primary, fsys = newFaultWALEngine(t, int64(ooo))
		}

		direct := New(Config{OOOWindow: int64(ooo)})
		replica := New(Config{})

		// Each run is 5 bytes: series, sampled, three timestamps; each payload is 3 runs.
		for payloadRuns := range slices.Chunk(script, 15) {
			var (
				buf  bytes.Buffer
				want AppendResult
			)

			w := wal.NewWriter(&buf)

			for r := range slices.Chunk(payloadRuns, 5) {
				if len(r) < 5 {
					break
				}

				s := stagedTestSeries(fmt.Sprintf("s%d", r[0]%5))
				ts := []int64{int64(r[2]), int64(r[3]), int64(r[4])}
				values := []float64{float64(r[2]), float64(r[3]), float64(r[4])}
				ids := []signal.SeriesID{s.Hash(), s.Hash(), s.Hash()}

				var sf []float64
				if r[1]&1 == 1 {
					sf = []float64{float64(1 + r[2]%2), float64(1 + r[3]%2), float64(1 + r[4]%2)}
				}

				require.NoError(t, w.WriteSeries(s.Hash(), s))

				if sf != nil {
					require.NoError(t, w.WriteSamplesSF(s.Hash(), ts, values, sf))
				} else {
					require.NoError(t, w.WriteSamples(s.Hash(), ts, values))
				}

				got, err := direct.AppendBatch(ids, ts, values, sf, func(int) signal.Series { return s }, limits)
				require.NoError(t, err)

				want.Accepted += got.Accepted
				want.RejectedOOO += got.RejectedOOO
				want.RejectedCardinality += got.RejectedCardinality
				want.RejectedBytes += got.RejectedBytes
				want.Overflowed += got.Overflowed
			}

			accepted, res, err := primary.ApplyPrimary(buf.Bytes(), limits)
			require.NoError(t, err)
			require.Equal(t, want, res)
			require.Equal(t, snapshotHead(direct.head), snapshotHead(primary.head))

			require.NoError(t, replica.ApplyReplicated(accepted))
			require.Equal(t, snapshotHead(primary.head), snapshotHead(replica.head))

			fresh := New(Config{})
			require.NoError(t, fresh.ApplyReplicated(accepted))

			require.Equal(t, int64(res.Accepted)*SampleBytes, fresh.head.bytes,
				"a fresh replica keeps every accepted sample")
		}

		if logged {
			require.Equal(t, snapshotHead(primary.head), snapshotHead(replayedHead(t, fsys)))
		}
	})
}
