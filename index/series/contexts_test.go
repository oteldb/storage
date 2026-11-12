package series

import (
	"runtime"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

func contextSeries() signal.Series {
	return signal.Series{
		Resource: signal.Resource{SchemaURL: []byte("schema"), Attributes: signal.NewAttributes(
			signal.KeyValue{Key: []byte("host"), Value: signal.StringValue([]byte("one"))},
			signal.KeyValue{Key: []byte("nested"), Value: signal.MapValue(signal.KeyValue{Key: []byte("x"), Value: signal.SliceValue(signal.BytesValue([]byte("owned")), signal.IntValue(3))})})},
		Scope:      signal.Scope{Name: []byte("scope"), Version: []byte("v1"), Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("a"), Value: signal.StringValue([]byte("b"))})},
		Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("metric"), Value: signal.IntValue(1)}),
	}
}

func TestSharedContextsOwnership(t *testing.T) {
	t.Parallel()
	c := NewContexts(1 << 20)
	s := contextSeries()
	want := s.Clone()
	a, b := NewWithContexts(c), NewWithContexts(c)
	id := a.Add(s)
	require.Equal(t, id, b.Add(s))
	av, _ := a.Get(id)
	bv, _ := b.Get(id)
	require.Same(t, &av.Resource.Attributes[0], &bv.Resource.Attributes[0])
	require.Same(t, &av.Scope.Attributes[0], &bv.Scope.Attributes[0])
	clear(s.Resource.SchemaURL)
	clear(s.Resource.Attributes[0].Key)
	clear(s.Scope.Name)
	clear(s.Attributes[0].Key)
	runtime.GC()
	got, ok := b.Get(id)
	require.True(t, ok)
	require.True(t, want.Equal(got))
	require.Equal(t, id, got.Hash())
	copyIndex := New()
	copyIndex.AddView(b.View(), 0)
	copyValue, _ := copyIndex.Get(id)
	require.True(t, want.Equal(copyValue))
	require.LessOrEqual(t, c.SizeBytes(), int64(1<<20))
}

func TestSharedContextsBoundAndFallback(t *testing.T) {
	t.Parallel()
	for _, budget := range []int64{0, 1, 8192, 1 << 16} {
		t.Run(strconv.FormatInt(budget, 10), func(t *testing.T) {
			t.Parallel()
			c := NewContexts(budget)
			a, b := NewWithContexts(c), NewWithContexts(c)
			for i := range 500 {
				s := contextSeries()
				s.Resource.SchemaURL = []byte(strconv.Itoa(i))
				id := a.Add(s)
				require.Equal(t, id, b.Add(s))
				got, _ := b.Get(id)
				require.True(t, s.Equal(got))
				require.LessOrEqual(t, c.SizeBytes(), budget)
			}
		})
	}
}

func TestSharedContextsConcurrent(t *testing.T) {
	t.Parallel()
	c := NewContexts(1 << 20)
	s := contextSeries()
	indexes := make([]*Index, 16)
	var wg sync.WaitGroup
	for i := range indexes {
		wg.Go(func() {
			ix := NewWithContexts(c)
			for range 100 {
				ix.Add(s)
			}
			indexes[i] = ix
		})
	}
	wg.Wait()
	first, _ := indexes[0].Get(s.Hash())
	for _, ix := range indexes {
		got, ok := ix.Get(s.Hash())
		require.True(t, ok)
		require.True(t, got.Equal(s))
		require.Same(t, &first.Resource.Attributes[0], &got.Resource.Attributes[0])
	}
	require.LessOrEqual(t, c.SizeBytes(), int64(1<<20))
}

func TestSharedContextsCheckedIdentity(t *testing.T) {
	t.Parallel()
	c := NewContexts(1 << 20)
	ix := NewWithContexts(c)
	before := c.SizeBytes()
	_, ok := ix.AddChecked(signal.SeriesID{}, contextSeries())
	require.False(t, ok)
	require.Equal(t, before, c.SizeBytes())
	require.Zero(t, ix.Len())
}
