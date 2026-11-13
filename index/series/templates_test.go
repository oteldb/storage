package series

import (
	"bytes"
	"math"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/internal/memsize"
	"github.com/oteldb/storage/signal"
)

func templateSeries(host, cpu int) signal.Series {
	return signal.Series{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("__name__"), Value: signal.StringValue([]byte("node_cpu_seconds_total"))},
		signal.KeyValue{Key: []byte("instance"), Value: signal.StringValue([]byte(strconv.Itoa(host)))},
		signal.KeyValue{Key: []byte("cpu"), Value: signal.IntValue(int64(cpu))},
		signal.KeyValue{Key: []byte("mode"), Value: signal.StringValue([]byte("idle"))},
		signal.KeyValue{Key: []byte("job"), Value: signal.StringValue([]byte("node-exporter"))},
	)}
}

func TestAttributeTemplatesMultipleVariables(t *testing.T) {
	t.Parallel()
	p := NewAttributeTemplates(8<<20, []byte("instance"), []byte("cpu"))
	ix := NewWithAttributeTemplates(nil, p)
	for host := range 50 {
		s := templateSeries(host, host%8)
		id := ix.Add(s)
		view := ix.View()
		require.Equal(t, s.AppendHashInput(nil), view.AppendCanonical(nil, host))
		require.Equal(t, s, view.At(host, nil))
		require.Equal(t, id, view.At(host, nil).Hash())
	}
	require.Len(t, ix.templateRows, 1)
	require.EqualValues(t, 2, ix.templateRows[0].dynamic)
	require.Less(t, len(ix.values), 50*5)
	require.EqualValues(t, 32, memsize.Of[row]())
	require.LessOrEqual(t, p.SizeBytes(), int64(8<<20))
}

func TestAttributeTemplatesCrossIndexAndCopy(t *testing.T) {
	t.Parallel()
	key := []byte("instance")
	p := NewAttributeTemplates(8<<20, key)
	key[0] = 'x'
	source := NewWithAttributeTemplates(nil, p)
	for i := range 3 {
		source.Add(templateSeries(i, 0))
	}
	view := source.View()
	for _, pool := range []*AttributeTemplates{nil, p, NewAttributeTemplates(8<<20, []byte("instance"), []byte("cpu"))} {
		target := NewWithAttributeTemplates(nil, pool)
		for i := range view.Len() {
			require.Equal(t, view.ID(i), target.AddView(view, i))
			require.Equal(t, view.AppendCanonical(nil, i), target.View().AppendCanonical(nil, i))
		}
		if pool == p {
			require.Same(t, source.templateRows[0], target.templateRows[0])
		}
	}
	for i := range view.Len() {
		want := bytes.Clone(view.AppendCanonical(nil, i))
		source.Add(templateSeries(i+1000, i))
		require.Equal(t, want, view.AppendCanonical(nil, i))
	}
}

func TestAttributeTemplatesOwnTypedDataAndCollisions(t *testing.T) {
	t.Parallel()
	p := NewAttributeTemplates(8<<20, []byte("instance"))
	p.hash = func([]byte) signal.SeriesID { return signal.SeriesID{} }
	ix := NewWithAttributeTemplates(nil, p)
	for cpu := range 4 {
		for host := range 3 {
			s := templateSeries(host+cpu*10, cpu)
			scope := []byte("scope")
			s.Scope.Name = scope
			want := s.AppendHashInput(nil)
			id := ix.Add(s)
			scope[0] = 'X'
			for i := range s.Attributes {
				clear(s.Attributes[i].Key)
			}
			ref, ok := ix.Ref(id)
			require.True(t, ok)
			require.Equal(t, want, ix.View().AppendCanonical(nil, int(ref)))
		}
	}
	require.Len(t, ix.templateRows, 4, "fingerprint collisions must not merge different fixed values")
}

func TestAttributeTemplatesCheckedAndBounded(t *testing.T) {
	t.Parallel()
	for _, budget := range []int64{0, 1, 80 << 10, 8 << 20} {
		t.Run(strconv.FormatInt(budget, 10), func(t *testing.T) {
			t.Parallel()
			p := NewAttributeTemplates(budget, []byte("instance"))
			ix := NewWithAttributeTemplates(nil, p)
			for i := range 200 {
				s := templateSeries(i, i%20)
				before := p.SizeBytes()
				_, ok := ix.AddChecked(signal.SeriesID{}, s)
				require.False(t, ok)
				require.Equal(t, before, p.SizeBytes())
				_, ok = ix.AddChecked(s.Hash(), s)
				require.True(t, ok)
				require.Equal(t, s.AppendHashInput(nil), ix.View().AppendCanonical(nil, i))
				require.LessOrEqual(t, p.SizeBytes(), budget)
			}
		})
	}
}

func TestAttributeTemplatesConcurrentViews(t *testing.T) {
	t.Parallel()
	p := NewAttributeTemplates(8<<20, []byte("instance"))
	ix := NewWithAttributeTemplates(nil, p)
	for i := range 8 {
		ix.Add(templateSeries(i, 0))
	}
	view := ix.View()
	var wg sync.WaitGroup
	wg.Go(func() {
		for i := range 500 {
			ix.Add(templateSeries(i+100, i%8))
		}
	})
	wg.Go(func() {
		other := NewWithAttributeTemplates(nil, p)
		for i := range 500 {
			other.Add(templateSeries(i+1000, i%8))
		}
	})
	for range 100 {
		for i := range view.Len() {
			require.Equal(t, view.ID(i), signal.HashBytes(view.AppendCanonical(nil, i)))
		}
	}
	wg.Wait()
}

func TestAttributeTemplatesTypedValues(t *testing.T) {
	t.Parallel()
	values := []signal.Value{
		signal.EmptyValue(), signal.StringValue(nil), signal.StringValue([]byte("a\x00b")),
		signal.BytesValue([]byte{0, 255}), signal.BoolValue(false), signal.BoolValue(true),
		signal.IntValue(math.MinInt64), signal.IntValue(math.MaxInt64), signal.DoubleValue(math.Inf(1)),
		signal.DoubleValue(math.Copysign(0, -1)), signal.DoubleValue(math.Float64frombits(0x7ff8000000001234)),
		signal.SliceValue(), signal.MapValue(), signal.SliceValue(signal.IntValue(5), signal.StringValue([]byte("5"))),
		signal.MapValue(signal.KeyValue{Key: []byte("x"), Value: signal.SliceValue(signal.EmptyValue(), signal.BoolValue(true))}),
	}
	p := NewAttributeTemplates(8<<20, []byte("instance"), []byte("variable"))
	ix, flat := NewWithAttributeTemplates(nil, p), New()
	for _, fixed := range values {
		for i, variable := range values {
			s := signal.Series{Attributes: signal.NewAttributes(
				signal.KeyValue{Key: []byte("fixed"), Value: fixed},
				signal.KeyValue{Key: []byte("kind"), Value: signal.IntValue(3)},
				signal.KeyValue{Key: []byte("instance"), Value: signal.StringValue([]byte(strconv.Itoa(i)))},
				signal.KeyValue{Key: []byte("variable"), Value: variable},
			)}
			id := ix.Add(s)
			require.Equal(t, flat.Add(s), id)
			ref, _ := ix.Ref(id)
			v := ix.View()
			require.Equal(t, s.AppendHashInput(nil), v.AppendCanonical(nil, int(ref)))
			require.Equal(t, s.AppendHashInput(nil), v.At(int(ref), nil).AppendHashInput(nil))
			var budget EncodingBudget
			budget.Add(v, int(ref))
			require.Positive(t, budget.Bytes())
			budget.Reset()
			budget.Add(v, int(ref))
			require.Positive(t, v.IdentityWorkspace(int(ref)))
		}
	}
	require.NotEmpty(t, ix.templateRows)
}

func TestAttributeTemplatesNoVariablesAndNoSharedFields(t *testing.T) {
	t.Parallel()
	for _, allVariable := range []bool{false, true} {
		var variable [][]byte
		if allVariable {
			variable = [][]byte{[]byte("__name__"), []byte("instance"), []byte("cpu"), []byte("mode"), []byte("job")}
		}
		pool := NewAttributeTemplates(8<<20, variable...)
		ix := NewWithAttributeTemplates(nil, pool)
		for i := range 20 {
			s := templateSeries(0, 0)
			s.Resource.SchemaURL = []byte(strconv.Itoa(i))
			ix.Add(s)
			require.Equal(t, s.AppendHashInput(nil), ix.View().AppendCanonical(nil, i))
		}
		if allVariable {
			require.Empty(t, ix.templateRows)
		} else {
			require.Len(t, ix.templateRows, 1)
			require.Zero(t, ix.templateRows[0].dynamic)
		}
	}
}

func FuzzAttributeTemplates(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 4, 5})
	f.Add([]byte("same fixed attributes across differently ordered instances"))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 1024 {
			return
		}
		p := NewAttributeTemplates(1<<20, []byte("instance"), []byte("variable"))
		ix := NewWithAttributeTemplates(nil, p)
		flat := New()
		for i, value := range data {
			s := templateSeries(i, int(value%4))
			s.Attributes = append(s.Attributes, signal.KeyValue{Key: []byte("variable"), Value: signal.StringValue([]byte{value})})
			id := ix.Add(s)
			if flat.Add(s) != id {
				t.Fatal("canonical ID changed")
			}
			ref, _ := ix.Ref(id)
			if !bytes.Equal(flat.View().AppendCanonical(nil, int(ref)), ix.View().AppendCanonical(nil, int(ref))) {
				t.Fatal("canonical bytes changed")
			}
			copied := NewWithAttributeTemplates(nil, p)
			copied.AddView(ix.View(), int(ref))
			if !bytes.Equal(s.AppendHashInput(nil), copied.View().AppendCanonical(nil, 0)) {
				t.Fatal("copied identity changed")
			}
		}
	})
}
