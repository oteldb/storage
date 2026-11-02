package engine

import (
	"context"
	"slices"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/index/identity"
	"github.com/oteldb/storage/index/series"
	"github.com/oteldb/storage/internal/memsize"
	"github.com/oteldb/storage/internal/watermark"
	"github.com/oteldb/storage/signal"
)

func TestMergeFinishReferenceCapacity(t *testing.T) {
	t.Parallel()
	base := (&mergeFinishBudget{}).bytes()
	for _, capacity := range []int{1, 32, 4096} {
		t.Run(strconv.Itoa(capacity), func(t *testing.T) {
			t.Parallel()
			b := mergeFinishBudget{refs: make([]uint32, 0, capacity)}
			assert.Equal(t, base+memsize.Slice(b.refs), b.bytes())
		})
	}
}

func TestMergeFinishReusesMaterializationAndSymbols(t *testing.T) {
	t.Parallel()
	index := series.New()
	id := index.Add(signal.Series{Attributes: signal.NewAttributes(signal.KeyValue{
		Key: []byte("nested"), Value: signal.MapValue(signal.KeyValue{Key: []byte("key"), Value: signal.StringValue([]byte("shared"))}),
	})})
	ref, ok := index.Ref(id)
	require.True(t, ok)
	view := index.View()
	var budget mergeFinishBudget
	budget.add(view, ref)
	symbols, materialized := budget.symbols.Bytes(), budget.materialized
	for range 100 {
		budget.add(view, ref)
	}
	assert.Equal(t, symbols, budget.symbols.Bytes(), "shared symbols and contexts are charged once")
	assert.Equal(t, materialized, budget.materialized, "EncodeSorted reuses one decoded attribute buffer")
	assert.Equal(t, view.EncodingBytes(int(ref))*101, budget.encoded)
}

func TestMergeFinishBoundsWrittenSidecars(t *testing.T) {
	t.Parallel()
	for _, weighted := range []bool{false, true} {
		t.Run(strconv.FormatBool(weighted), func(t *testing.T) {
			t.Parallel()
			files, err := file.New(t.TempDir())
			require.NoError(t, err)
			e := New(Config{Backend: files, Prefix: "sidecars"})
			ids := make([]signal.SeriesID, 257)
			for i := range ids {
				entry := orderSeries(i)
				e.head.registerSeries(entry)
				ids[i] = entry.Hash()
			}
			slices.SortFunc(ids, signal.SeriesID.Compare)
			w, err := newPartStreamWriter(t.Context(), e, compressProfile{}, 0, weighted, true, nil)
			require.NoError(t, err)
			defer w.abort()
			w.mergeFinish = &mergeFinishBudget{}
			view := e.head.series.View()
			for _, id := range ids {
				ref, ok := e.head.series.Ref(id)
				require.True(t, ok)
				var weights []float64
				if weighted {
					weights = []float64{2}
				}
				for point := range 2 {
					require.NoError(t, w.appendMergeSeries(view, ref, true, id, []int64{int64(point + 1)}, []float64{float64(point)}, weights, int64(point+1)))
				}
			}
			require.Len(t, w.mergeFinish.refs, len(ids), "consecutive chunks retain only one reference per output identity")
			resident, bound := w.residentBytes(), w.mergeResidentBytes()
			part, err := w.finish(t.Context())
			require.NoError(t, err)
			var largest int64
			for _, key := range []string{sidxKey(part.prefix), watermark.Key(part.prefix), identityKey(part.prefix)} {
				size, err := files.Size(t.Context(), key)
				require.NoError(t, err)
				largest = max(largest, size)
			}
			assert.GreaterOrEqual(t, bound-resident, largest*4)
			if weighted {
				_, err := files.Size(t.Context(), statsKey(part.prefix))
				require.ErrorIs(t, err, backend.ErrNotExist)
			} else {
				size, err := files.Size(t.Context(), statsKey(part.prefix))
				require.NoError(t, err)
				assert.GreaterOrEqual(t, bound-resident, size*4)
			}
		})
	}
}

func FuzzMergeFinishIdentityBound(f *testing.F) {
	f.Add([]byte("shared-value"))
	f.Add(make([]byte, 2048))
	f.Fuzz(func(t *testing.T, value []byte) {
		if len(value) > 4096 {
			t.Skip()
		}
		index := series.New()
		id := index.Add(signal.Series{Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("value"), Value: signal.BytesValue(value)})})
		ref, ok := index.Ref(id)
		require.True(t, ok)
		var budget mergeFinishBudget
		view := index.View()
		budget.add(view, ref)
		data := identity.EncodeSorted(nil, 1, func(_ int, dst signal.Attributes) series.Entry {
			return series.Entry{ID: id, Series: view.At(int(ref), dst)}
		})
		assert.GreaterOrEqual(t, budget.bytes(), int64(len(data)))
	})
}

func TestMergeFinishReferencesSurviveRegistryGrowth(t *testing.T) {
	t.Parallel()
	e := New(Config{Backend: backend.Memory(), Prefix: "registry-growth",
		FlushAdmission: func(context.Context, int64) (func(), error) { return func() {}, nil }})
	unlock := e.lockMerge()
	defer unlock()
	entries := make(map[signal.SeriesID]signal.Series)
	ids := make([]signal.SeriesID, 3)
	for i := range ids {
		entry := registryGrowthSeries(i)
		e.head.registerSeries(entry)
		ids[i] = entry.Hash()
		entries[ids[i]] = entry
	}
	slices.SortFunc(ids, signal.SeriesID.Compare)
	w, err := newPartStreamWriter(t.Context(), e, compressProfile{}, 0, false, false, nil)
	require.NoError(t, err)
	defer w.abort()
	w.mergeFinish = &mergeFinishBudget{}
	view := e.head.series.View()
	for _, id := range ids {
		ref, ok := e.head.series.Ref(id)
		require.True(t, ok)
		require.NoError(t, w.appendMergeSeries(view, ref, true, id, []int64{100}, []float64{7}, nil, 100))
	}
	for i := len(ids); i < 4099; i++ {
		_, err := e.Append(registryGrowthSeries(i), 200, 9)
		require.NoError(t, err)
	}
	require.Equal(t, 4099, e.head.series.Len())
	p, err := w.finish(t.Context())
	require.NoError(t, err)
	data, err := e.cfg.Backend.Read(t.Context(), identityKey(p.prefix))
	require.NoError(t, err)
	var decodedIDs []signal.SeriesID
	require.NoError(t, identity.Decode(data, func(id signal.SeriesID, entry signal.Series) error {
		want, ok := entries[id]
		assert.True(t, ok)
		assert.True(t, want.Equal(entry), "finishing must resolve retained references against the grown registry")
		decodedIDs = append(decodedIDs, id)
		return nil
	}))
	assert.Equal(t, ids, decodedIDs)
	decoded, err := decodePart(t.Context(), p)
	require.NoError(t, err)
	assert.Equal(t, []int64{100, 100, 100}, decoded.ts)
	assert.Equal(t, []float64{7, 7, 7}, decoded.vals)
}

func registryGrowthSeries(i int) signal.Series {
	entry := orderSeries(i)
	entry.Scope.Name = []byte("scope-" + strconv.Itoa(i))
	entry.Resource.Attributes = signal.NewAttributes(signal.KeyValue{
		Key: []byte("resource"), Value: signal.StringValue([]byte("resource-" + strconv.Itoa(i))),
	})
	return entry
}
