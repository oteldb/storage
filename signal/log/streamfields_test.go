package log

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/signal"
)

// mkChurnLogs builds one record per instance id: the same service, differing only in a high-churn
// resource attribute — the shape that explodes stream cardinality when every attribute identifies.
func mkChurnLogs(instances ...string) Logs {
	var ld Logs

	for i, inst := range instances {
		rl := ld.AddResource()
		rl.Resource = signal.Resource{Attributes: signal.NewAttributes(
			signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte("api"))},
			signal.KeyValue{Key: []byte("service.instance.id"), Value: signal.StringValue([]byte(inst))},
		)}
		sl := rl.AddScope()
		sl.AddRecord().Timestamp = int64(100 + i)
	}

	return ld
}

func TestProjectNarrowsStreamIdentity(t *testing.T) {
	t.Parallel()

	fields := func(signal.Resource, signal.Scope) ([]string, bool) {
		return []string{"service.name"}, false
	}

	var (
		streams  []signal.SeriesID
		identity []signal.Series
		routed   []signal.Series
	)

	Project(mkChurnLogs("a", "b", "c"), fields, func(b *recordengine.Batch) {
		streams = append(streams, b.Stream)
		identity = append(identity, b.Identity().Clone())

		require.NotNil(t, b.Route, "a narrowed batch carries its full identity for tenant routing")
		routed = append(routed, b.Route().Clone())
	})

	require.Len(t, streams, 3, "one batch per (resource, scope) group, as before")
	assert.Equal(t, streams[0], streams[1], "instance.id is not identifying")
	assert.Equal(t, streams[0], streams[2])

	for i := range identity {
		require.Len(t, identity[i].Resource.Attributes, 1, "identity keeps only the stream fields")
		assert.Equal(t, []byte("service.name"), identity[i].Resource.Attributes[0].Key)
		assert.Len(t, routed[i].Resource.Attributes, 2, "routing sees every resource attribute")
	}
}

func TestProjectAllFieldsKeepsEveryAttribute(t *testing.T) {
	t.Parallel()

	var streams []signal.SeriesID

	Project(mkChurnLogs("a", "b"), nil, func(b *recordengine.Batch) {
		streams = append(streams, b.Stream)
		assert.Nil(t, b.Route, "an unnarrowed batch routes by Identity")
	})

	require.Len(t, streams, 2)
	assert.NotEqual(t, streams[0], streams[1], "every resource attribute identifies")
}

// TestProjectResourceColumnRoundTrips is the OTLP-preservation guarantee: an attribute excluded from
// the stream key is still stored per record and still reassembles into the resource on read.
func TestProjectResourceColumnRoundTrips(t *testing.T) {
	t.Parallel()

	fields := func(signal.Resource, signal.Scope) ([]string, bool) {
		return []string{"service.name"}, false
	}

	Project(mkChurnLogs("inst-7"), fields, func(b *recordengine.Batch) {
		require.Len(t, b.Bytes[bResource], b.Len())

		res, err := Resource(b.Identity(), b.Bytes[bResource][0])
		require.NoError(t, err)
		require.Len(t, res.Attributes, 2, "the whole resource is reconstructed")

		v, ok := res.Attributes.Get([]byte("service.instance.id"))
		require.True(t, ok, "the excluded attribute survives")
		assert.Equal(t, []byte("inst-7"), v.Str())

		v, ok = res.Attributes.Get([]byte("service.name"))
		require.True(t, ok, "the identifying attribute is stored too, so a condition answers it")
		assert.Equal(t, []byte("api"), v.Str())
	})
}

func TestResourceEmptyBlobFallsBackToIdentity(t *testing.T) {
	t.Parallel()

	stream := signal.Series{Resource: signal.Resource{
		SchemaURL:  []byte("https://schema"),
		Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte("api"))}),
	}}

	res, err := Resource(stream, nil)
	require.NoError(t, err)
	assert.Equal(t, stream.Resource, res)
}

func TestResourceRejectsCorruptBlob(t *testing.T) {
	t.Parallel()

	_, err := Resource(signal.Series{}, []byte{0xFF, 0xFF, 0xFF})
	require.Error(t, err)
}

func TestFilterAttrs(t *testing.T) {
	t.Parallel()

	attrs := func(keys ...string) signal.Attributes {
		kvs := make([]signal.KeyValue, 0, len(keys))
		for _, k := range keys {
			kvs = append(kvs, signal.KeyValue{Key: []byte(k), Value: signal.StringValue([]byte("v"))})
		}

		return signal.NewAttributes(kvs...)
	}

	names := func(a signal.Attributes) []string {
		out := make([]string, 0, len(a))
		for i := range a {
			out = append(out, string(a[i].Key))
		}

		return out
	}

	for _, tt := range []struct {
		name   string
		attrs  signal.Attributes
		fields []string
		want   []string
	}{
		{"empty attrs", nil, []string{"a"}, []string{}},
		{"empty fields", attrs("a", "b"), nil, []string{}},
		{"all kept", attrs("a", "b"), []string{"a", "b"}, []string{"a", "b"}},
		{"subset", attrs("a", "b", "c"), []string{"b"}, []string{"b"}},
		{"field absent", attrs("a", "c"), []string{"b"}, []string{}},
		{"prefix is not a match", attrs("service"), []string{"service.name"}, []string{}},
		{"longer key than field", attrs("service.name"), []string{"service"}, []string{}},
		{"interleaved", attrs("a", "c", "e"), []string{"b", "c", "d", "e"}, []string{"c", "e"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, names(filterAttrs(signal.Attributes{}, tt.attrs, tt.fields)))
		})
	}
}

func BenchmarkProjectNarrowed(b *testing.B) {
	ld := mkChurnLogs("a", "b", "c", "d")
	fields := func(signal.Resource, signal.Scope) ([]string, bool) {
		return []string{"service.name"}, false
	}

	b.ReportAllocs()

	for b.Loop() {
		Project(ld, fields, func(*recordengine.Batch) {})
	}
}
