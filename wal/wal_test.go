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

// mkSeries builds a series identity with a fixed resource/scope and the given point
// attributes.
func mkSeries(pointPairs ...string) signal.Series {
	return signal.Series{
		Resource:   signal.Resource{Attributes: attrs("service.name", "svc")},
		Scope:      signal.Scope{Name: []byte("lib")},
		Attributes: attrs(pointPairs...),
	}
}

type captured struct {
	id signal.SeriesID
	s  signal.Series
}

func collect(t *testing.T, data []byte) []captured {
	t.Helper()

	var got []captured

	err := Replay(data, Handlers{OnSeries: func(id signal.SeriesID, s signal.Series) error {
		got = append(got, captured{id, s.Clone()})

		return nil
	}})
	require.NoError(t, err)

	return got
}

func TestWriteReplayRoundTrip(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	w := NewWriter(&buf)

	s1, s2 := mkSeries("job", "api"), mkSeries("job", "web", "env", "prod")
	require.NoError(t, w.WriteSeries(s1.Hash(), s1))
	require.NoError(t, w.WriteSeries(s2.Hash(), s2))

	got := collect(t, buf.Bytes())
	require.Len(t, got, 2)
	assert.Equal(t, s1.Hash(), got[0].id)
	assert.True(t, s1.Equal(got[0].s))
	assert.Equal(t, s2.Hash(), got[1].id)
	assert.True(t, s2.Equal(got[1].s))
}

func TestWriteReplaySamples(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	id := signal.SeriesID{Hi: 3, Lo: 7}
	ts := []int64{100, 250, -5}
	values := []float64{1.5, 2.5, -3.25}
	require.NoError(t, NewWriter(&buf).WriteSamples(id, ts, values))

	var (
		gotID     signal.SeriesID
		gotTs     []int64
		gotValues []float64
	)
	err := Replay(buf.Bytes(), Handlers{OnSamples: func(i signal.SeriesID, t []int64, v []float64) error {
		gotID, gotTs, gotValues = i, t, v

		return nil
	}})
	require.NoError(t, err)
	assert.Equal(t, id, gotID)
	assert.Equal(t, ts, gotTs)
	assert.Equal(t, values, gotValues)
}

func TestWriteReplaySamplesSF(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	id := signal.SeriesID{Hi: 9, Lo: 11}
	ts := []int64{10, 20, 30}
	values := []float64{1, 2, 3}
	sf := []float64{4, 4, 8}
	require.NoError(t, NewWriter(&buf).WriteSamplesSF(id, ts, values, sf))

	var (
		gotID  signal.SeriesID
		gotTs  []int64
		gotVal []float64
		gotSF  []float64
	)
	err := Replay(buf.Bytes(), Handlers{
		OnSamplesSF: func(i signal.SeriesID, t []int64, v, s []float64) error {
			gotID, gotTs, gotVal, gotSF = i, t, v, s

			return nil
		},
	})
	require.NoError(t, err)
	assert.Equal(t, id, gotID)
	assert.Equal(t, ts, gotTs)
	assert.Equal(t, values, gotVal)
	assert.Equal(t, sf, gotSF)
}

// A reader without OnSamplesSF still recovers the samples through OnSamples (weights dropped).
func TestSamplesSFFallsBackToOnSamples(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	id := signal.SeriesID{Hi: 1, Lo: 2}
	require.NoError(t, NewWriter(&buf).WriteSamplesSF(id, []int64{5}, []float64{42}, []float64{16}))

	var gotVal []float64
	err := Replay(buf.Bytes(), Handlers{OnSamples: func(_ signal.SeriesID, _ []int64, v []float64) error {
		gotVal = v

		return nil
	}})
	require.NoError(t, err)
	assert.Equal(t, []float64{42}, gotVal, "values recovered even without the sf-aware handler")
}

func TestParseSamplesSFErrors(t *testing.T) {
	t.Parallel()

	noop := Handlers{OnSamplesSF: func(signal.SeriesID, []int64, []float64, []float64) error { return nil }}

	// A claimed sample with a ts but no room for value+sf is corrupt.
	truncated := append(make([]byte, seriesIDLen), 0x01, 0x02) // count=1, ts=1, then nothing
	require.Error(t, Replay(appendFrame(nil, recordSamplesSF, truncated), noop))
}

func TestTornTailRecoversPrefix(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	w := NewWriter(&buf)
	s1, s2 := mkSeries("job", "api"), mkSeries("job", "web")
	require.NoError(t, w.WriteSeries(s1.Hash(), s1))
	full := len(buf.Bytes())
	require.NoError(t, w.WriteSeries(s2.Hash(), s2))

	torn := buf.Bytes()[:full+2] // cut the second record mid-frame
	got := collect(t, torn)
	require.Len(t, got, 1)
	assert.Equal(t, s1.Hash(), got[0].id)
}

func TestCorruptRecordSurfaced(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	s := mkSeries("job", "api")
	require.NoError(t, NewWriter(&buf).WriteSeries(s.Hash(), s))

	data := buf.Bytes()
	data[3] ^= 0xFF // corrupt a body byte; length stays valid, CRC fails

	err := Replay(data, Handlers{OnSeries: func(signal.SeriesID, signal.Series) error { return nil }})
	require.ErrorIs(t, err, ErrCorrupt)
}

func TestUnknownRecordSkipped(t *testing.T) {
	t.Parallel()

	frame := appendFrame(nil, 99, []byte("future record type"))
	called := false
	err := Replay(frame, Handlers{OnSeries: func(signal.SeriesID, signal.Series) error {
		called = true

		return nil
	}})
	require.NoError(t, err)
	assert.False(t, called, "unknown record types are skipped")
}

func TestReplayNilHandler(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	s := mkSeries("a", "b")
	require.NoError(t, NewWriter(&buf).WriteSeries(s.Hash(), s))
	require.NoError(t, Replay(buf.Bytes(), Handlers{}))
}

func TestWriterWriteError(t *testing.T) {
	t.Parallel()

	s := mkSeries("a", "b")
	err := NewWriter(errWriter{}).WriteSeries(s.Hash(), s)
	require.ErrorIs(t, err, errWrite)
}

func TestOnSeriesError(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	s := mkSeries("a", "b")
	require.NoError(t, NewWriter(&buf).WriteSeries(s.Hash(), s))

	err := Replay(buf.Bytes(), Handlers{OnSeries: func(signal.SeriesID, signal.Series) error { return errBoom }})
	require.ErrorIs(t, err, errBoom)
}

func TestParseSamplesErrors(t *testing.T) {
	t.Parallel()

	noop := Handlers{OnSamples: func(signal.SeriesID, []int64, []float64) error { return nil }}

	// Payload shorter than a SeriesID.
	require.ErrorIs(t, Replay(appendFrame(nil, recordSamples, []byte{1, 2, 3}), noop), ErrCorrupt)

	// Valid id, count claims more samples than the bytes allow.
	tooMany := append(make([]byte, seriesIDLen), 0x05) // count=5, no samples follow
	require.Error(t, Replay(appendFrame(nil, recordSamples, tooMany), noop))

	// Valid id, count=1, a timestamp, but a truncated value (<8 bytes).
	truncVal := append(make([]byte, seriesIDLen), 0x01, 0x02, 0x00, 0x00) // count=1, ts, 2 value bytes
	require.Error(t, Replay(appendFrame(nil, recordSamples, truncVal), noop))
}

func TestParseSeriesErrors(t *testing.T) {
	t.Parallel()

	noop := Handlers{OnSeries: func(signal.SeriesID, signal.Series) error { return nil }}

	// Payload shorter than a SeriesID.
	short := appendFrame(nil, recordSeries, []byte{1, 2, 3})
	require.ErrorIs(t, Replay(short, noop), ErrCorrupt)

	// Valid 16-byte id, then a resource whose schema-url length runs past the data.
	payload := append(make([]byte, seriesIDLen), 0x7f) // schema_url len=127, no following bytes
	require.Error(t, Replay(appendFrame(nil, recordSeries, payload), noop))
}

func FuzzReplay(f *testing.F) {
	var buf bytes.Buffer
	s := mkSeries("job", "api")
	w := NewWriter(&buf)
	_ = w.WriteSeries(s.Hash(), s)
	_ = w.WriteSamples(s.Hash(), []int64{1, 2}, []float64{1, 2})
	_ = w.WriteSamplesSF(s.Hash(), []int64{3, 4}, []float64{3, 4}, []float64{2, 2})
	f.Add(buf.Bytes())
	f.Add([]byte{})

	f.Fuzz(func(_ *testing.T, data []byte) {
		// Must never panic; corrupt input returns an error or stops cleanly. Exercise every
		// handler, including the sf-aware one.
		_ = Replay(data, Handlers{
			OnSeries: func(id signal.SeriesID, s signal.Series) error {
				_ = id
				_ = s.Clone()

				return nil
			},
			OnSamples:   func(signal.SeriesID, []int64, []float64) error { return nil },
			OnSamplesSF: func(signal.SeriesID, []int64, []float64, []float64) error { return nil },
		})
	})
}
