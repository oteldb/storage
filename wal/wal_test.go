package wal

import (
	"bytes"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

var (
	errWrite = errors.New("write failed")
	errBoom  = errors.New("boom")
)

type errWriter struct{}

func (errWriter) Write([]byte) (int, error) { return 0, errWrite }

func attrs(pairs ...string) signal.Attributes {
	kvs := make([]signal.KeyValue, 0, len(pairs)/2)
	for i := 0; i+1 < len(pairs); i += 2 {
		kvs = append(kvs, signal.KeyValue{Key: []byte(pairs[i]), Value: signal.StringValue([]byte(pairs[i+1]))})
	}

	return signal.NewAttributes(kvs...)
}

type captured struct {
	id signal.SeriesID
	a  signal.Attributes
}

func collect(t *testing.T, data []byte) []captured {
	t.Helper()

	var got []captured

	err := Replay(data, Handlers{OnSeries: func(id signal.SeriesID, a signal.Attributes) error {
		got = append(got, captured{id, a.Clone()})

		return nil
	}})
	require.NoError(t, err)

	return got
}

func TestWriteReplayRoundTrip(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	w := NewWriter(&buf)

	a1, a2 := attrs("job", "api"), attrs("job", "web", "env", "prod")
	require.NoError(t, w.WriteSeries(a1.Hash(), a1))
	require.NoError(t, w.WriteSeries(a2.Hash(), a2))

	got := collect(t, buf.Bytes())
	require.Len(t, got, 2)
	assert.Equal(t, a1.Hash(), got[0].id)
	assert.True(t, a1.Equal(got[0].a))
	assert.Equal(t, a2.Hash(), got[1].id)
	assert.True(t, a2.Equal(got[1].a))
}

func TestTornTailRecoversPrefix(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	w := NewWriter(&buf)
	a1, a2 := attrs("job", "api"), attrs("job", "web")
	require.NoError(t, w.WriteSeries(a1.Hash(), a1))
	full := len(buf.Bytes())
	require.NoError(t, w.WriteSeries(a2.Hash(), a2))

	// Cut the second record in the middle: replay must keep the first and stop cleanly.
	torn := buf.Bytes()[:full+2]
	got := collect(t, torn)
	require.Len(t, got, 1)
	assert.Equal(t, a1.Hash(), got[0].id)
}

func TestCorruptRecordSurfaced(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	require.NoError(t, NewWriter(&buf).WriteSeries(attrs("job", "api").Hash(), attrs("job", "api")))

	data := buf.Bytes()
	data[3] ^= 0xFF // corrupt a body byte; length stays valid, CRC fails

	err := Replay(data, Handlers{OnSeries: func(signal.SeriesID, signal.Attributes) error { return nil }})
	require.ErrorIs(t, err, ErrCorrupt)
}

func TestUnknownRecordSkipped(t *testing.T) {
	t.Parallel()

	frame := appendFrame(nil, 99, []byte("future record type"))
	called := false
	err := Replay(frame, Handlers{OnSeries: func(signal.SeriesID, signal.Attributes) error {
		called = true

		return nil
	}})
	require.NoError(t, err)
	assert.False(t, called, "unknown record types are skipped")
}

func TestReplayNilHandler(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	require.NoError(t, NewWriter(&buf).WriteSeries(attrs("a", "b").Hash(), attrs("a", "b")))
	// No handler set: records are read (and skipped) without error.
	require.NoError(t, Replay(buf.Bytes(), Handlers{}))
}

func TestWriterWriteError(t *testing.T) {
	t.Parallel()

	a := attrs("a", "b")
	err := NewWriter(errWriter{}).WriteSeries(a.Hash(), a)
	require.ErrorIs(t, err, errWrite)
}

func TestOnSeriesError(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	a := attrs("a", "b")
	require.NoError(t, NewWriter(&buf).WriteSeries(a.Hash(), a))

	err := Replay(buf.Bytes(), Handlers{OnSeries: func(signal.SeriesID, signal.Attributes) error { return errBoom }})
	require.ErrorIs(t, err, errBoom)
}

func TestParseSeriesErrors(t *testing.T) {
	t.Parallel()

	noop := Handlers{OnSeries: func(signal.SeriesID, signal.Attributes) error { return nil }}

	// Payload shorter than a SeriesID.
	short := appendFrame(nil, recordSeries, []byte{1, 2, 3})
	require.ErrorIs(t, Replay(short, noop), ErrCorrupt)

	// Valid 16-byte id, then attributes that claim more entries than the bytes allow.
	payload := append(make([]byte, seriesIDLen), 0x05) // count=5, no following data
	require.Error(t, Replay(appendFrame(nil, recordSeries, payload), noop))
}

func FuzzReplay(f *testing.F) {
	var buf bytes.Buffer
	_ = NewWriter(&buf).WriteSeries(attrs("job", "api").Hash(), attrs("job", "api"))
	f.Add(buf.Bytes())
	f.Add([]byte{})

	f.Fuzz(func(_ *testing.T, data []byte) {
		// Must never panic; corrupt input returns an error or stops cleanly.
		_ = Replay(data, Handlers{OnSeries: func(id signal.SeriesID, a signal.Attributes) error {
			_ = id
			_ = a.Clone()

			return nil
		}})
	})
}
