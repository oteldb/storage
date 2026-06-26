package wal

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

func TestWriteReplayRecords(t *testing.T) {
	t.Parallel()

	id := signal.SeriesID{Hi: 7, Lo: 9}
	blob := []byte("opaque-engine-encoded-record-payload")

	var buf bytes.Buffer
	require.NoError(t, NewWriter(&buf).WriteRecords(id, blob))

	var (
		gotID   signal.SeriesID
		gotBlob []byte
	)

	require.NoError(t, Replay(buf.Bytes(), Handlers{
		OnRecords: func(i signal.SeriesID, p []byte) error {
			gotID, gotBlob = i, append([]byte(nil), p...)

			return nil
		},
	}))

	assert.Equal(t, id, gotID)
	assert.Equal(t, blob, gotBlob, "the opaque payload round-trips")
}

func TestRecordsNilHandlerSkips(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	require.NoError(t, NewWriter(&buf).WriteRecords(signal.SeriesID{Lo: 1}, []byte("x")))
	require.NoError(t, Replay(buf.Bytes(), Handlers{})) // no OnRecords ⇒ skipped, no error
}

func TestParseRecordsTruncated(t *testing.T) {
	t.Parallel()

	_, _, err := parseRecords([]byte{0x01}) // shorter than a SeriesID
	require.ErrorIs(t, err, ErrCorrupt)
}

// FuzzRecordFraming asserts a records frame round-trips through Write∘Replay and that parseRecords
// never panics on arbitrary input.
func FuzzRecordFraming(f *testing.F) {
	f.Add([]byte("payload"), uint64(5), uint64(9))
	f.Add([]byte(""), uint64(0), uint64(0))

	f.Fuzz(func(t *testing.T, blob []byte, hi, lo uint64) {
		_, _, _ = parseRecords(blob) // must never panic on arbitrary bytes

		id := signal.SeriesID{Hi: hi, Lo: lo}

		var buf bytes.Buffer
		require.NoError(t, NewWriter(&buf).WriteRecords(id, blob))

		var (
			rid   signal.SeriesID
			rblob []byte
		)

		require.NoError(t, Replay(buf.Bytes(), Handlers{OnRecords: func(i signal.SeriesID, p []byte) error {
			rid, rblob = i, append([]byte(nil), p...)

			return nil
		}}))

		assert.Equal(t, id, rid)
		assert.Equal(t, normBytes(blob), normBytes(rblob))
	})
}

func normBytes(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}

	return b
}
