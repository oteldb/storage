package engine

import (
	"slices"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/internal/memsize"
	"github.com/oteldb/storage/signal"
)

func TestHeadRunBufferWorkspace(t *testing.T) {
	t.Parallel()
	for _, capacity := range []int{1, 32, 4096} {
		t.Run(strconv.Itoa(capacity), func(t *testing.T) {
			t.Parallel()
			b := headPartBuilder{e: New(Config{})}
			base := b.baseBytes()
			b.entries = make([]headRunEntry, 0, capacity)
			b.partRefs = make([]uint32, 0, capacity)
			want := int64(capacity) * (memsize.Of[headRunEntry]() + memsize.Of[uint32]())
			require.Equal(t, base+want, b.baseBytes(), "the admission includes pointer entries and part references")
		})
	}
}

func TestHeadRunSealsAtCapacityAndClearsPointers(t *testing.T) {
	t.Parallel()
	e := New(Config{Backend: backend.Memory(), Prefix: "run-capacity", FlushMemoryBytes: 9 << 20})
	series := []signal.Series{apiSeries(), {Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("job"), Value: signal.StringValue([]byte("other"))})}}
	for _, s := range series {
		e.head.registerSeries(s)
	}
	slices.SortFunc(series, func(a, b signal.Series) int { return a.Hash().Compare(b.Hash()) })
	view := e.head.series.View()
	b := headPartBuilder{ctx: t.Context(), e: e, view: view, target: e.flushMemoryBytes(),
		entries: make([]headRunEntry, 0, 1), partRefs: make([]uint32, 0, 1)}
	require.NoError(t, b.reserve(b.target))
	defer func() { b.release() }()
	first := &sampleBuf{ts: []int64{2, 1}, values: []float64{20, 10}, sf: []float64{2, 3}}
	second := &sampleBuf{ts: []int64{3, 4}, values: []float64{30, 40}}
	for i, buf := range []*sampleBuf{first, second} {
		id := series[i].Hash()
		ref, ok := e.head.series.Ref(id)
		require.True(t, ok)
		require.NoError(t, b.add(id, ref, buf))
		require.Equal(t, 1, cap(b.entries))
		require.Equal(t, 1, cap(b.partRefs))
	}
	require.Len(t, b.parts, 1, "the next identity seals the full run")
	require.NoError(t, b.flushRun())
	require.Len(t, b.parts, 2)
	require.Equal(t, 4, b.rows)
	require.Empty(t, b.entries)
	require.Nil(t, b.entries[:cap(b.entries)][0].buf, "the empty run cannot retain the old sample buffers")
	require.Equal(t, []int64{2, 1}, first.ts, "frozen input remains immutable")
	for i, part := range b.parts {
		d, err := decodePart(t.Context(), part)
		require.NoError(t, err)
		if i == 0 {
			require.Equal(t, []int64{1, 2}, d.ts)
			require.Equal(t, []float64{10, 20}, d.vals)
			require.Equal(t, []float64{3, 2}, d.sf)
		} else {
			require.Equal(t, []int64{3, 4}, d.ts)
			require.Equal(t, []float64{30, 40}, d.vals)
		}
	}
}

func TestHeadRunResetRetainsBufferReservation(t *testing.T) {
	t.Parallel()
	e := New(Config{Backend: backend.Memory(), Prefix: "small-run", FlushMemoryBytes: 1024})
	s := apiSeries()
	e.head.registerSeries(s)
	ref, ok := e.head.series.Ref(s.Hash())
	require.True(t, ok)
	b := headPartBuilder{ctx: t.Context(), e: e, view: e.head.series.View(), target: e.flushMemoryBytes(),
		entries: make([]headRunEntry, 0, 1), partRefs: make([]uint32, 0, 1)}
	require.NoError(t, b.reserve(b.baseBytes()))
	defer func() { b.release() }()
	require.NoError(t, b.add(s.Hash(), ref, &sampleBuf{ts: []int64{1}, values: []float64{2}}))
	require.NoError(t, b.flushRun())
	require.GreaterOrEqual(t, b.reserved, b.baseBytes()+b.identities.Bytes())
}
