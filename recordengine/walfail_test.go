package recordengine_test

import (
	"path"
	"slices"
	"strings"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/internal/vfs/faultfs"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/wal"
)

var errInjectedWrite = errors.New("injected WAL write failure")

func newFaultWAL(t *testing.T) (*faultfs.FS, *wal.SegmentWriter) {
	t.Helper()

	fsys := faultfs.New()

	w, err := wal.CreateFS(fsys, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	return fsys, w
}

// failWrite makes the n-th WAL write from now on (1-based) fail without writing anything.
func failWrite(fsys *faultfs.FS, n int) {
	seen := 0

	fsys.Add(faultfs.Rule{
		Op: faultfs.OpWrite,
		Match: func(c faultfs.Call) bool {
			if !strings.HasSuffix(c.Name, ".wal") {
				return false
			}

			seen++

			return seen == n
		},
		Err: errInjectedWrite,
	})
}

// replayed rebuilds an engine from every WAL segment in fsys, in order.
func replayed(t *testing.T, fsys *faultfs.FS, side recordengine.SideStore) *recordengine.Engine {
	t.Helper()

	entries, err := fsys.ReadDir(".")
	require.NoError(t, err)

	e := recordengine.New(recordengine.Config{Schema: testSchema, SideStore: side})

	for _, ent := range entries {
		if path.Ext(ent.Name()) != ".wal" {
			continue
		}

		data, err := fsys.ReadFile(ent.Name())
		require.NoError(t, err)
		require.NoError(t, e.ApplyReplicated(data))
	}

	return e
}

func TestAppendBatchWALFailureLeavesHeadUntouched(t *testing.T) {
	t.Parallel()

	fsys, w := newFaultWAL(t)
	e := recordengine.New(recordengine.Config{Schema: testSchema, WAL: w, OOOWindow: 50})

	_, err := e.AppendBatch(mkBatch("api", rrec{ts: 100, body: "first"}), recordengine.AppendLimits{})
	require.NoError(t, err)

	headBytes := e.HeadBytes()

	failWrite(fsys, 1)

	res, err := e.AppendBatch(mkBatch("api", rrec{ts: 1000, body: "second"}), recordengine.AppendLimits{})
	require.ErrorIs(t, err, errInjectedWrite)
	assert.Equal(t, recordengine.AppendResult{}, res)
	assert.Equal(t, headBytes, e.HeadBytes())
	assert.Equal(t, []string{"first"}, streamBodies(t, e), "a failed log write applies nothing")

	// Had the failed batch raised the stream's watermark to 1000, a record at 90 would now be 910 behind
	// a 50-wide window and rejected.
	res, err = e.AppendBatch(mkBatch("api", rrec{ts: 90, body: "late"}), recordengine.AppendLimits{})
	require.NoError(t, err)
	assert.Equal(t, 1, res.Accepted, "the failed batch must not raise the out-of-order watermark")

	res, err = e.AppendBatch(mkBatch("api", rrec{ts: 1000, body: "second"}), recordengine.AppendLimits{})
	require.NoError(t, err)
	assert.Equal(t, 1, res.Accepted)

	want := []string{"late", "first", "second"}
	assert.Equal(t, want, streamBodies(t, e), "the retry stores the batch once")
	assert.Equal(t, want, streamBodies(t, replayed(t, fsys, nil)), "the log holds the batch once")
}

func TestAppendBatchSideDeltaLoggedBeforeRecords(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		fail int // which write of the side-carrying batch fails: 1 = side frame, 2 = records frame
	}{
		{name: "SideWriteFails", fail: 1},
		{name: "RecordsWriteFails", fail: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			fsys, w := newFaultWAL(t)
			e := recordengine.New(recordengine.Config{Schema: testSchema, WAL: w, SideStore: newFakeSide()})

			_, err := e.AppendBatch(mkBatch("api", rrec{ts: 100, body: "first"}), recordengine.AppendLimits{})
			require.NoError(t, err)

			withSide := func() *recordengine.Batch {
				b := mkBatch("api", rrec{ts: 200, body: "symbolized"})
				b.Side = encodeSide(map[uint64][]byte{7: []byte("frame")})

				return b
			}

			failWrite(fsys, tc.fail)

			_, err = e.AppendBatch(withSide(), recordengine.AppendLimits{})
			require.ErrorIs(t, err, errInjectedWrite)
			assert.Equal(t, []string{"first"}, streamBodies(t, e))

			_, err = e.AppendBatch(withSide(), recordengine.AppendLimits{})
			require.NoError(t, err)

			want := []string{"first", "symbolized"}
			assert.Equal(t, want, streamBodies(t, e))

			side := newFakeSide()
			assert.Equal(t, want, streamBodies(t, replayed(t, fsys, side)),
				"a records frame never reaches the log ahead of its side delta, so replay has no duplicate")
			assert.Equal(t, []byte("frame"), side.acc[7])
		})
	}
}

func TestApplyPrimaryWALFailureAppliesNothing(t *testing.T) {
	t.Parallel()

	fsys, w := newFaultWAL(t)
	e := recordengine.New(recordengine.Config{Schema: testSchema, WAL: w})

	payload := recordengine.EncodeWAL(mkBatch("api", rrec{ts: 100, body: "one"}, rrec{ts: 200, body: "two"}))

	failWrite(fsys, 1)

	accepted, res, err := e.ApplyPrimary(payload, recordengine.AppendLimits{})
	require.ErrorIs(t, err, errInjectedWrite)
	assert.Nil(t, accepted)
	assert.Equal(t, recordengine.AppendResult{}, res)
	assert.Zero(t, e.HeadBytes())
	assert.Empty(t, streamBodies(t, e))

	accepted, res, err = e.ApplyPrimary(payload, recordengine.AppendLimits{})
	require.NoError(t, err)
	assert.Equal(t, 2, res.Accepted)
	assert.NotEmpty(t, accepted)

	want := []string{"one", "two"}
	assert.Equal(t, want, streamBodies(t, e))
	assert.Equal(t, want, streamBodies(t, replayed(t, fsys, nil)))
}

// FuzzAppendAdmissionMatchesUnlogged pins the logged append — which decides the accepted set before
// applying any of it — to the unlogged one, which admits and applies each record as it goes. Both run
// the same batches through the same limits; a small in-flight cap makes a byte rejection followed by
// admitted records a common case.
func FuzzAppendAdmissionMatchesUnlogged(f *testing.F) {
	f.Add([]byte{10, 200, 3, 5, 90, 1, 250, 250, 4}, uint16(60), uint8(30))
	f.Add([]byte{0, 0, 0, 255, 1, 2, 3}, uint16(0), uint8(0))

	f.Fuzz(func(t *testing.T, script []byte, maxInFlight uint16, ooo uint8) {
		_, w := newFaultWAL(t)

		logged := recordengine.New(recordengine.Config{Schema: testSchema, WAL: w, OOOWindow: int64(ooo)})
		unlogged := recordengine.New(recordengine.Config{Schema: testSchema, OOOWindow: int64(ooo)})
		limits := recordengine.AppendLimits{MaxInFlightBytes: int64(maxInFlight)}

		for batch := range slices.Chunk(script, 3) {
			svc := "api"
			if batch[0]%2 == 1 {
				svc = "db"
			}

			recs := make([]rrec, 0, len(batch))
			for i, v := range batch {
				recs = append(recs, rrec{ts: int64(v), body: strings.Repeat("x", int(v)%17+i)})
			}

			gotRes, err := logged.AppendBatch(mkBatch(svc, recs...), limits)
			require.NoError(t, err)

			wantRes, err := unlogged.AppendBatch(mkBatch(svc, recs...), limits)
			require.NoError(t, err)

			require.Equal(t, wantRes, gotRes)
			require.Equal(t, unlogged.HeadBytes(), logged.HeadBytes())
		}

		for _, svc := range []string{"api", "db"} {
			assert.Equal(t, bodiesFor(t, unlogged, svc), bodiesFor(t, logged, svc))
		}
	})
}

// TestApplyPrimaryAdmissionMatchesSequentialAppends: a multi-stream payload, with a stream repeated,
// is admitted exactly as appending its batches one by one would admit them — the in-flight cap is
// shared across streams and a repeated stream sees its earlier run in its watermark.
func TestApplyPrimaryAdmissionMatchesSequentialAppends(t *testing.T) {
	t.Parallel()

	batches := []*recordengine.Batch{
		mkBatch("api", rrec{ts: 100, body: strings.Repeat("a", 20)}, rrec{ts: 110, body: "b"}),
		mkBatch("db", rrec{ts: 50, body: strings.Repeat("c", 30)}),
		mkBatch("api", rrec{ts: 20, body: "late"}, rrec{ts: 120, body: strings.Repeat("d", 40)}),
		mkBatch("db", rrec{ts: 60, body: "e"}),
	}

	payload := make([]byte, 0, 256)
	for _, b := range batches {
		payload = append(payload, recordengine.EncodeWAL(b)...)
	}

	limits := recordengine.AppendLimits{MaxInFlightBytes: 90}

	_, w := newFaultWAL(t)
	primary := recordengine.New(recordengine.Config{Schema: testSchema, WAL: w, OOOWindow: 30})

	_, got, err := primary.ApplyPrimary(payload, limits)
	require.NoError(t, err)

	sequential := recordengine.New(recordengine.Config{Schema: testSchema, OOOWindow: 30})

	var want recordengine.AppendResult

	for _, b := range batches {
		r, err := sequential.AppendBatch(b, limits)
		require.NoError(t, err)

		want.Accepted += r.Accepted
		want.RejectedOOO += r.RejectedOOO
		want.RejectedBytes += r.RejectedBytes
	}

	require.Positive(t, want.RejectedOOO+want.RejectedBytes, "the case must exercise a rejection")
	assert.Equal(t, want, got)
	assert.Equal(t, sequential.HeadBytes(), primary.HeadBytes())

	for _, svc := range []string{"api", "db"} {
		assert.Equal(t, bodiesFor(t, sequential, svc), bodiesFor(t, primary, svc))
	}
}

func bodiesFor(t *testing.T, e *recordengine.Engine, svc string) []string {
	t.Helper()

	batches := fetchAll(t, e, req(svc))

	out := make([]string, 0, len(batches))
	for _, b := range batches {
		out = append(out, bodies(b)...)
	}

	return out
}
