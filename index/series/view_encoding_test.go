package series

import (
	"bytes"
	"math"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

func encodingSeries() signal.Series {
	return signal.Series{
		Resource: signal.Resource{SchemaURL: []byte("resource-schema"), Attributes: signal.NewAttributes(
			signal.KeyValue{Key: []byte("service.name"), Value: sv("node-exporter")},
			signal.KeyValue{Key: []byte("nested"), Value: signal.SliceValue(signal.IntValue(7), signal.MapValue(
				signal.KeyValue{Key: []byte("k"), Value: sv("v")},
			))},
		)},
		Scope: signal.Scope{Name: []byte("instrumentation"), Version: []byte("v2"), SchemaURL: []byte("scope-schema"),
			Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("scope"), Value: signal.BoolValue(true)})},
		Attributes: signal.NewAttributes(
			signal.KeyValue{Key: []byte("empty"), Value: signal.EmptyValue()},
			signal.KeyValue{Key: []byte("string"), Value: sv("5")},
			signal.KeyValue{Key: []byte("bytes"), Value: signal.BytesValue([]byte{0, 1, 255})},
			signal.KeyValue{Key: []byte("bool"), Value: signal.BoolValue(false)},
			signal.KeyValue{Key: []byte("int"), Value: signal.IntValue(math.MinInt64)},
			signal.KeyValue{Key: []byte("double"), Value: signal.DoubleValue(5)},
			signal.KeyValue{Key: []byte("nan"), Value: signal.DoubleValue(math.Float64frombits(0x7ff8000000000042))},
			signal.KeyValue{Key: []byte("negative-zero"), Value: signal.DoubleValue(math.Copysign(0, -1))},
			signal.KeyValue{Key: []byte("positive-infinity"), Value: signal.DoubleValue(math.Inf(1))},
			signal.KeyValue{Key: []byte("slice"), Value: signal.SliceValue(sv("a"), signal.IntValue(1))},
			signal.KeyValue{Key: []byte("map"), Value: signal.MapValue(
				signal.KeyValue{Key: []byte("k"), Value: signal.SliceValue(signal.IntValue(5), sv("5"))},
			)},
		),
	}
}

func TestViewEncodedIdentity(t *testing.T) {
	t.Parallel()
	cases := []signal.Series{{}, {Attributes: signal.Attributes{}}, encodingSeries(), mk("api", "mode", "idle")}
	for i, original := range cases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			t.Parallel()
			ix := New()
			id := ix.Add(original)
			v := ix.View()
			r, s := v.Context(0)
			require.Equal(t, original.Resource.AppendHashInput(nil), r.AppendHashInput(nil))
			require.Equal(t, original.Scope.AppendHashInput(nil), s.AppendHashInput(nil))
			require.Equal(t, len(original.Attributes), v.AttributeCount(0))
			at := 0
			v.EncodedAttributes(0, func(key, value []byte) {
				require.Equal(t, original.Attributes[at].Key, key)
				require.Equal(t, signal.AppendValue(nil, original.Attributes[at].Value), value)
				at++
			})
			require.Equal(t, len(original.Attributes), at)
			prefix := []byte("caller-prefix")
			got := v.AppendCanonical(bytes.Clone(prefix), 0)
			require.Equal(t, original.AppendHashInput(bytes.Clone(prefix)), got)
			require.Equal(t, id, signal.HashBytes(got[len(prefix):]))
		})
	}
}

func TestIndexAddView(t *testing.T) {
	t.Parallel()
	source, copied, materialized := New(), New(), New()
	original := []signal.Series{{}, encodingSeries(), mk("api", "mode", "idle"),
		mk("api", "mode", "user"), {Attributes: signal.Attributes{}}, encodingSeries()}
	for _, s := range original {
		source.Add(s)
	}
	view := source.View()
	for i := range view.Len() {
		id := copied.AddView(view, i)
		require.Equal(t, view.ID(i), id)
		require.Equal(t, id, materialized.Add(view.At(i, nil)))
		require.Equal(t, materialized.Len(), copied.Len())
		require.Equal(t, view.AppendCanonical(nil, i), copied.View().AppendCanonical(nil, i))
	}
	require.Equal(t, materialized.rows, copied.rows)
	require.Equal(t, materialized.values, copied.values)
	require.Equal(t, materialized.keys, copied.keys)
	require.Equal(t, materialized.sym.Snapshot(), copied.sym.Snapshot())
	require.Equal(t, materialized.SizeBytes()-int64(cap(materialized.buf)+cap(materialized.vbuf)), copied.SizeBytes())
	before := copied.SizeBytes()
	for i := range view.Len() {
		require.Equal(t, view.ID(i), copied.AddView(view, i))
		require.Equal(t, view.ID(i), copied.AddView(copied.View(), i))
	}
	require.Equal(t, before, copied.SizeBytes(), "duplicate and self-copy additions retain nothing")
}

func TestIndexAddViewOwnsCopy(t *testing.T) {
	t.Parallel()
	source, target := New(), New()
	original := encodingSeries()
	id := source.Add(original)
	target.AddView(source.View(), 0)
	want := original.AppendHashInput(nil)
	// The source is discarded after the copy; destroy its storage to expose borrowed payloads.
	for _, symbol := range source.sym.Snapshot() {
		clear(symbol)
	}
	clear(source.contexts[0].resource.Attributes)
	clear(source.contexts[0].scope.Attributes)
	source.sym.Release()
	require.Equal(t, id, target.View().ID(0))
	require.Equal(t, want, target.View().AppendCanonical(nil, 0))
	require.Equal(t, want, target.View().At(0, nil).AppendHashInput(nil))
}

func TestViewEncodingConcurrentRegistration(t *testing.T) {
	t.Parallel()
	source := New()
	original := encodingSeries()
	source.Add(original)
	view := source.View()
	want := original.AppendHashInput(nil)
	var wg sync.WaitGroup
	wg.Go(func() {
		target := New()
		var buf []byte
		for range 1000 {
			buf = view.AppendCanonical(buf[:0], 0)
			require.Equal(t, want, buf)
			require.Equal(t, view.ID(0), target.AddView(view, 0))
		}
	})
	for i := range 1000 {
		source.Add(mk(strconv.Itoa(i), "cpu", strconv.Itoa(i)))
	}
	wg.Wait()
	require.Equal(t, 1, view.Len())
	require.Equal(t, want, view.AppendCanonical(nil, 0))
}

func FuzzViewCanonical(f *testing.F) {
	corpus := []signal.Series{{}, encodingSeries(), mk("api", "mode", "idle")}
	for i := range corpus {
		f.Add(corpus[i].AppendHashInput(nil))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		s, n, err := signal.DecodeSeries(data)
		if err != nil || n != len(data) {
			return
		}
		source, target := New(), New()
		id := source.Add(s)
		v := source.View()
		if got := v.AppendCanonical(nil, 0); !bytes.Equal(got, s.AppendHashInput(nil)) || signal.HashBytes(got) != id {
			t.Fatal("compact canonical encoding changed identity")
		}
		if target.AddView(v, 0) != id || !bytes.Equal(target.View().AppendCanonical(nil, 0), v.AppendCanonical(nil, 0)) {
			t.Fatal("compact copy changed identity")
		}
	})
}
