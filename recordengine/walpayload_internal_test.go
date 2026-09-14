package recordengine

import (
	"bytes"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/internal/vfs/faultfs"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"
)

func payloadBatch(rows int) *Batch {
	series := signal.Series{Resource: signal.Resource{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte("svc"))},
	)}}

	b := &Batch{Stream: series.Hash(), Identity: func() signal.Series { return series }}
	b.Ints, b.Bytes = [][]int64{nil}, [][][]byte{nil}

	for i := range rows {
		b.Ts = append(b.Ts, int64(i))
		b.Ints[0] = append(b.Ints[0], -int64(i))
		b.Bytes[0] = append(b.Bytes[0], bytes.Repeat([]byte{'x'}, i%3)) // includes empty cells
	}

	return b
}

// TestSealRecsMatchesEncodeBatchRecs: a payload assembled in place is byte-identical to one encoded in
// one pass, at every width the record count's varint can take.
func TestSealRecsMatchesEncodeBatchRecs(t *testing.T) {
	t.Parallel()

	for _, rows := range []int{0, 1, 127, 128, 16383, 16384} {
		b := payloadBatch(rows)

		buf := openRecs([]byte("stale bytes from an earlier payload"))
		for i := range rows {
			buf = appendBatchRec(buf, b, i)
		}

		assert.Equal(t, encodeBatchRecs(b), sealRecs(buf, rows), "rows=%d", rows)
	}
}

// TestAppendLoggedPayloadMatchesAdmittedRecords: the records frame an append logs holds exactly the
// admitted records, in the layout replay decodes, with rejected records interleaved among them — and
// a later append, which reuses the engine's payload buffer, leaves the earlier frame as it was.
func TestAppendLoggedPayloadMatchesAdmittedRecords(t *testing.T) {
	t.Parallel()

	fsys := faultfs.New()

	w, err := wal.CreateFS(fsys, 0)
	require.NoError(t, err)

	e := New(Config{Schema: headTestSchema, WAL: w, OOOWindow: 10})

	first := payloadBatch(0)
	for _, ts := range []int64{100, 50, 105, 20, 101} { // 50 and 20 are out of order
		first.Ts = append(first.Ts, ts)
		first.Ints[0] = append(first.Ints[0], ts)
		first.Bytes[0] = append(first.Bytes[0], []byte{byte(ts)})
	}

	second := payloadBatch(300)
	for i := range second.Ts {
		second.Ts[i] += 200
	}

	res, err := e.AppendBatch(first, AppendLimits{})
	require.NoError(t, err)
	require.Equal(t, AppendResult{Accepted: 3, RejectedOOO: 2}, res)

	_, err = e.AppendBatch(second, AppendLimits{})
	require.NoError(t, err)
	require.NoError(t, w.Close())

	admitted := func(b *Batch, keep ...int) []byte {
		sub := &Batch{Ints: [][]int64{nil}, Bytes: [][][]byte{nil}}
		for _, i := range keep {
			sub.Ts = append(sub.Ts, b.Ts[i])
			sub.Ints[0] = append(sub.Ints[0], b.Ints[0][i])
			sub.Bytes[0] = append(sub.Bytes[0], b.Bytes[0][i])
		}

		return encodeBatchRecs(sub)
	}

	all := make([]int, len(second.Ts))
	for i := range all {
		all[i] = i
	}

	want := [][]byte{admitted(first, 0, 2, 4), admitted(second, all...)}

	entries, err := fsys.ReadDir(".")
	require.NoError(t, err)

	var got [][]byte

	for _, ent := range entries {
		if path.Ext(ent.Name()) != ".wal" {
			continue
		}

		data, err := fsys.ReadFile(ent.Name())
		require.NoError(t, err)
		require.NoError(t, wal.Replay(data, wal.Handlers{
			OnRecords: func(_ signal.SeriesID, blob []byte) error {
				got = append(got, bytes.Clone(blob))

				return nil
			},
		}))
	}

	assert.Equal(t, want, got)
}
