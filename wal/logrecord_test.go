package wal

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

func TestWriteReplayLogRecords(t *testing.T) {
	t.Parallel()

	id := signal.SeriesID{Hi: 7, Lo: 9}
	recs := []LogRecord{
		{
			Timestamp: 100, ObservedTimestamp: 101, SeverityNumber: 9, Flags: 1, Dropped: 2,
			SeverityText: []byte("INFO"), Body: []byte("hello world"),
			TraceID: []byte("0123456789abcdef"), SpanID: []byte("01234567"), Attrs: []byte("a=b"),
		},
		{Timestamp: 200, SeverityNumber: 17, Body: []byte("second")}, // sparse: empty byte fields
	}

	var buf bytes.Buffer
	require.NoError(t, NewWriter(&buf).WriteLogRecords(id, recs))

	var (
		gotID  signal.SeriesID
		gotRec []LogRecord
	)

	require.NoError(t, Replay(buf.Bytes(), Handlers{
		OnLogRecords: func(i signal.SeriesID, r []LogRecord) error {
			gotID, gotRec = i, r

			return nil
		},
	}))

	assert.Equal(t, id, gotID)
	require.Len(t, gotRec, 2)
	assert.Equal(t, recs[0], gotRec[0], "all fields round-trip")
	assert.Equal(t, int64(200), gotRec[1].Timestamp)
	assert.Equal(t, []byte("second"), gotRec[1].Body)
	assert.Nil(t, gotRec[1].SeverityText, "an empty byte field round-trips as nil")
}

func TestLogRecordsNilHandlerSkips(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	require.NoError(t, NewWriter(&buf).WriteLogRecords(signal.SeriesID{Lo: 1}, []LogRecord{{Timestamp: 1}}))
	require.NoError(t, Replay(buf.Bytes(), Handlers{})) // no OnLogRecords ⇒ skipped, no error
}

func TestParseLogRecordsTruncated(t *testing.T) {
	t.Parallel()

	_, _, err := parseLogRecords([]byte{0x01}) // shorter than a SeriesID
	require.ErrorIs(t, err, ErrCorrupt)
}

// FuzzLogRecordFraming asserts parseLogRecords never panics on arbitrary input and that any
// well-formed record set round-trips through Write∘parse unchanged.
func FuzzLogRecordFraming(f *testing.F) {
	f.Add([]byte("INFO"), []byte("body"), int64(5), int64(3))
	f.Add([]byte(""), []byte(""), int64(0), int64(-1))

	f.Fuzz(func(t *testing.T, sevText, body []byte, ts, observed int64) {
		id := signal.SeriesID{Hi: uint64(ts), Lo: uint64(observed)}
		recs := []LogRecord{{
			Timestamp: ts, ObservedTimestamp: observed, SeverityNumber: int32(len(body)),
			SeverityText: sevText, Body: body,
		}}

		payload := appendLogRecords(id.AppendBinary(nil), recs)

		// parse must never panic and must reproduce the records.
		gotID, got, err := parseLogRecords(payload)
		require.NoError(t, err)
		assert.Equal(t, id, gotID)
		require.Len(t, got, 1)
		assert.Equal(t, ts, got[0].Timestamp)
		assert.Equal(t, observed, got[0].ObservedTimestamp)
		assert.Equal(t, normalizeBytes(body), got[0].Body)

		// Arbitrary truncations of a valid payload must error cleanly, never panic.
		for i := range payload {
			_, _, _ = parseLogRecords(payload[:i])
		}
	})
}

// normalizeBytes maps an empty (len 0) slice to nil, matching takeBytes' zero-length handling.
func normalizeBytes(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}

	return b
}
