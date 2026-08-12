package identity_test

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/index/identity"
	"github.com/oteldb/storage/index/series"
	"github.com/oteldb/storage/signal"
)

func kv(key string, v signal.Value) signal.KeyValue {
	return signal.KeyValue{Key: []byte(key), Value: v}
}

func str(s string) signal.Value { return signal.StringValue([]byte(s)) }

// mkEntry builds one churn-shaped identity: a shared resource/scope and a per-series instance.
func mkEntry(i int) series.Entry {
	s := signal.Series{
		Resource: signal.Resource{
			SchemaURL:  []byte("https://opentelemetry.io/schemas/1.30.0"),
			Attributes: signal.NewAttributes(kv("service.name", str("api")), kv("instance.id", str("pod-"+strconv.Itoa(i)))),
		},
		Scope: signal.Scope{Name: []byte("otelcol"), Version: []byte("0.128.0")},
		Attributes: signal.NewAttributes(
			kv("__name__", str("http_requests_total")),
			kv("code", signal.IntValue(int64(200+i%5))),
		),
	}

	return series.Entry{ID: s.Hash(), Series: s}
}

// collect decodes an object into (id → identity), cloning so the result outlives the buffer.
func collect(t *testing.T, data []byte) map[signal.SeriesID]signal.Series {
	t.Helper()

	out := make(map[signal.SeriesID]signal.Series)
	require.NoError(t, identity.Decode(data, func(id signal.SeriesID, s signal.Series) error {
		out[id] = s.Clone()

		return nil
	}))

	return out
}

func TestRoundTrip(t *testing.T) {
	t.Parallel()

	entries := make([]series.Entry, 100)
	for i := range entries {
		entries[i] = mkEntry(i)
	}

	got := collect(t, identity.Encode(nil, entries))

	require.Len(t, got, len(entries))

	for _, want := range entries {
		s, ok := got[want.ID]
		require.True(t, ok, "identity %v decoded", want.ID)
		assert.True(t, want.Series.Equal(s), "identity round-trips unchanged")
		assert.Equal(t, want.ID, s.Hash(), "the decoded identity still hashes to its id")
	}
}

func TestRoundTripValueKinds(t *testing.T) {
	t.Parallel()

	// Values are interned from their type-tagged encoding, so distinct kinds must stay distinct.
	s := signal.Series{Attributes: signal.NewAttributes(
		kv("str", str("5")),
		kv("int", signal.IntValue(5)),
		kv("double", signal.DoubleValue(5)),
		kv("bool", signal.BoolValue(true)),
		kv("bytes", signal.BytesValue([]byte{0, 1, 2})),
	)}
	ent := series.Entry{ID: s.Hash(), Series: s}

	got := collect(t, identity.Encode(nil, []series.Entry{ent}))

	require.Len(t, got, 1)
	assert.True(t, s.Equal(got[ent.ID]), "every value kind round-trips")
}

func TestEncodeEmpty(t *testing.T) {
	t.Parallel()

	data := identity.Encode(nil, nil)

	n, err := identity.Count(data)
	require.NoError(t, err)
	assert.Zero(t, n)
	assert.Empty(t, collect(t, data))
}

func TestEncodeIsOrderIndependent(t *testing.T) {
	t.Parallel()

	entries := make([]series.Entry, 50)
	for i := range entries {
		entries[i] = mkEntry(i)
	}

	shuffled := make([]series.Entry, len(entries))
	for i, e := range entries {
		shuffled[len(entries)-1-i] = e
	}

	// Entries are written in id order, so the object is a function of the *set* — the caller's
	// slice order (map iteration, merge order) cannot make two equal sets differ on disk.
	assert.Equal(t, identity.Encode(nil, entries), identity.Encode(nil, shuffled))
	assert.Equal(t, entries[0], mkEntry(0), "the caller's slice is not reordered")
}

func TestCount(t *testing.T) {
	t.Parallel()

	entries := make([]series.Entry, 17)
	for i := range entries {
		entries[i] = mkEntry(i)
	}

	n, err := identity.Count(identity.Encode(nil, entries))
	require.NoError(t, err)
	assert.Equal(t, 17, n)
}

func TestDecodeStopsOnCallbackError(t *testing.T) {
	t.Parallel()

	entries := make([]series.Entry, 10)
	for i := range entries {
		entries[i] = mkEntry(i)
	}

	seen := 0
	err := identity.Decode(identity.Encode(nil, entries), func(signal.SeriesID, signal.Series) error {
		seen++
		if seen == 3 {
			return assert.AnError
		}

		return nil
	})

	require.ErrorIs(t, err, assert.AnError)
	assert.Equal(t, 3, seen)
}

func TestDecodeCorrupt(t *testing.T) {
	t.Parallel()

	entries := []series.Entry{mkEntry(0), mkEntry(1)}
	good := identity.Encode(nil, entries)

	tests := []struct {
		name string
		data []byte
	}{
		{"empty", nil},
		{"truncated header", good[:3]},
		{"bad magic", append([]byte{0, 0, 0, 0}, good[4:]...)},
		{"bad version", append(append([]byte{}, good[:4]...), append([]byte{9}, good[5:]...)...)},
		{"truncated tail", good[:len(good)-1]},
		{"truncated body", good[:len(good)/2]},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := identity.Decode(tt.data, func(signal.SeriesID, signal.Series) error { return nil })
			require.Error(t, err)
		})
	}
}

func TestDecodeCorruptSection(t *testing.T) {
	t.Parallel()

	good := identity.Encode(nil, []series.Entry{mkEntry(0), mkEntry(1)})

	// Flip a byte inside the sections region: the TOC's per-section checksum must catch it.
	bad := append([]byte{}, good...)
	bad[len(bad)/2] ^= 0xFF

	err := identity.Decode(bad, func(signal.SeriesID, signal.Series) error { return nil })
	require.Error(t, err)
	require.ErrorIs(t, err, identity.ErrCorrupt)
}

func BenchmarkEncode(b *testing.B) {
	const n = 100_000

	entries := make([]series.Entry, n)
	for i := range entries {
		entries[i] = mkEntry(i)
	}

	buf := identity.Encode(nil, entries)

	b.ReportAllocs()
	b.SetBytes(int64(len(buf)))

	for b.Loop() {
		buf = identity.Encode(buf[:0], entries)
	}
}

func BenchmarkDecode(b *testing.B) {
	const n = 100_000

	entries := make([]series.Entry, n)
	for i := range entries {
		entries[i] = mkEntry(i)
	}

	data := identity.Encode(nil, entries)

	b.ReportAllocs()
	b.SetBytes(int64(len(data)))

	for b.Loop() {
		if err := identity.Decode(data, func(signal.SeriesID, signal.Series) error { return nil }); err != nil {
			b.Fatal(err)
		}
	}
}
