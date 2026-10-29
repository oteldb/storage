package series

import (
	"math"
	"strconv"
	"sync"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

func TestCompactValues(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name  string
		value signal.Value
	}{
		{name: "empty", value: signal.EmptyValue()},
		{name: "string", value: sv("5")},
		{name: "integer", value: signal.IntValue(5)},
		{name: "boolean", value: signal.BoolValue(true)},
		{name: "double", value: signal.DoubleValue(5)},
		{name: "nan", value: signal.DoubleValue(math.NaN())},
		{name: "negative-zero", value: signal.DoubleValue(math.Copysign(0, -1))},
		{name: "bytes", value: signal.BytesValue([]byte{0, 1, 2})},
		{name: "slice", value: signal.SliceValue(sv("a"), signal.IntValue(1))},
		{name: "map", value: signal.MapValue(signal.KeyValue{Key: []byte("a"), Value: sv("b")})},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ix := New()
			want := mk("api")
			want.Attributes = signal.NewAttributes(signal.KeyValue{Key: []byte("value"), Value: tt.value})
			id := ix.Add(want)
			got, ok := ix.Get(id)
			require.True(t, ok)
			assert.True(t, got.Equal(want))
			assert.Equal(t, want.Hash(), got.Hash())
		})
	}
}

func TestCompactSharedKeysets(t *testing.T) {
	t.Parallel()
	ix := New()
	a := ix.Add(mk("api", "cpu", "0", "mode", "user"))
	b := ix.Add(mk("api", "cpu", "1", "mode", "system"))
	require.Len(t, ix.rows, 2)
	assert.Equal(t, ix.rows[0].keys, ix.rows[1].keys)
	assert.Equal(t, ix.rows[0].context, ix.rows[1].context)
	assert.Equal(t, uintptr(32), unsafe.Sizeof(row{}))
	assert.Len(t, ix.values, 4)
	ref, ok := ix.Ref(b)
	require.True(t, ok)
	assert.Equal(t, b, ix.ID(ref))
	assert.NotEqual(t, a, b)
}

func TestCompactSnapshot(t *testing.T) {
	t.Parallel()
	ix := New()
	want := mk("api", "cpu", "0")
	id := ix.Add(want)
	snapshot := ix.Snapshot()
	ix.Add(mk("other", "cpu", "1"))
	require.Len(t, snapshot, 1)
	assert.Equal(t, id, snapshot[0].ID)
	assert.True(t, snapshot[0].Series.Equal(want))
	assert.Equal(t, id, snapshot[0].Series.Hash())
}

//nolint:paralleltest // Allocation accounting runs serially.
func TestGetIntoReusesAttributes(t *testing.T) {
	ix := New()
	id := ix.Add(mk("api", "cpu", "0", "mode", "user"))
	got, ok := ix.Get(id)
	require.True(t, ok)
	attrs := got.Attributes
	assert.Zero(t, testing.AllocsPerRun(100, func() {
		got, ok = ix.GetInto(id, attrs)
	}))
	require.True(t, ok)
	assert.Same(t, unsafe.SliceData(attrs), unsafe.SliceData(got.Attributes))
}

func TestViewConcurrentRegistration(t *testing.T) {
	t.Parallel()
	ix := New()
	want := mk("api", "cpu", "0")
	ix.Add(want)
	v := ix.View()
	var wg sync.WaitGroup
	wg.Go(func() {
		var dst signal.Attributes
		for range 2000 {
			s := v.At(0, dst)
			dst = s.Attributes
			assert.True(t, s.Equal(want))
		}
	})
	for i := range 2000 {
		ix.Add(mk(strconv.Itoa(i), "cpu", strconv.Itoa(i), "other", strconv.Itoa(i)))
	}
	wg.Wait()
	assert.Equal(t, 1, v.Len())
	assert.True(t, v.At(0, nil).Equal(want))
}

func FuzzCompactIdentity(f *testing.F) {
	corpus := []signal.Series{mk("api", "cpu", "0"), {}, {Attributes: signal.Attributes{}},
		{Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("nested"), Value: signal.MapValue(
			signal.KeyValue{Key: []byte("v"), Value: signal.SliceValue(signal.IntValue(5), sv("5"))},
		)})}}
	for i := range corpus {
		f.Add(corpus[i].AppendHashInput(nil))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		want, _, err := signal.DecodeSeries(data)
		if err != nil {
			return
		}
		ix := New()
		id := ix.Add(want)
		got, ok := ix.Get(id)
		if !ok || !got.Equal(want) || got.Hash() != id {
			t.Fatal("compact identity changed the canonical value")
		}
	})
}
