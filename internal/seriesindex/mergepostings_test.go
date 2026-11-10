package seriesindex

import (
	"bytes"
	"encoding/binary"
	"slices"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

func openPostingFixture(tb testing.TB, input []signal.Series) *Index {
	tb.Helper()
	data, _ := buildIndex(tb, input)
	be := backend.Memory()
	require.NoError(tb, be.Write(tb.Context(), "index", data))
	idx, err := Open(tb.Context(), be, "index")
	require.NoError(tb, err)
	return idx
}

func postingMergeBuilder(tb testing.TB, input []signal.Series, indexes []*Index, sources [][]signal.Series) (*Builder, *Builder) {
	tb.Helper()
	m := NewPostingMerge(tb.Context(), indexes)
	actual, expected := &Builder{}, &Builder{}
	actual.MergePostings(m)
	for i := range input {
		s := input[i]
		meta := Metadata{Covered: int64(i + 100), HasStats: true, Stats: Stats{Count: 4, Sum: 10, Min: 1, Max: 4}}
		require.NoError(tb, actual.AddIdentity(s.Hash(), s, meta))
		require.NoError(tb, expected.Add(s.Hash(), s, meta))
		for source := range sources {
			ref, found := slices.BinarySearchFunc(sources[source], s.Hash(), func(s signal.Series, id signal.SeriesID) int { return s.Hash().Compare(id) })
			if found {
				require.NoError(tb, m.Append(source, uint32(ref), uint32(i)))
			}
		}
	}
	return actual, expected
}

func TestMergedPostingsMatchBuilder(t *testing.T) {
	t.Parallel()
	for _, input := range [][]signal.Series{nil, {{}}, builderViewFixture()} {
		indexes := []*Index{openPostingFixture(t, input), openPostingFixture(t, input)}
		actual, expected := postingMergeBuilder(t, input, indexes, [][]signal.Series{input, input})
		var got, want bytes.Buffer
		require.NoError(t, actual.Write(&got))
		require.NoError(t, expected.Write(&want))
		require.Equal(t, want.Bytes(), got.Bytes())
		require.GreaterOrEqual(t, actual.Bytes(), int64(got.Len()))
		require.GreaterOrEqual(t, actual.EncodedBound(), int64(got.Len()))
		var again bytes.Buffer
		require.NoError(t, actual.Write(&again))
		require.Equal(t, got.Bytes(), again.Bytes())
	}
}

func TestMergedPostingsSubsetAndSparseFallback(t *testing.T) {
	t.Parallel()
	input := builderViewFixture()
	for _, selected := range [][]signal.Series{input[:len(input)/2], input[len(input)/2:]} {
		actual, expected := postingMergeBuilder(t, selected, []*Index{openPostingFixture(t, input), openPostingFixture(t, input)}, [][]signal.Series{input, input})
		var got, want bytes.Buffer
		require.NoError(t, actual.Write(&got))
		require.NoError(t, expected.Write(&want))
		actualIndex, wantIndex := indexesFromBytes(t, got.Bytes(), want.Bytes())
		comparePostingQueries(t, actualIndex, wantIndex)
		require.NotNil(t, actual.mergedPostings, "broad subsets use postings merge")
	}
	large := make([]signal.Series, 100)
	for i := range large {
		large[i] = signal.Series{Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("host"), Value: signal.StringValue([]byte(strconv.Itoa(i)))})}
	}
	slices.SortFunc(large, func(a, b signal.Series) int { return a.Hash().Compare(b.Hash()) })
	actual, expected := postingMergeBuilder(t, large[:1], []*Index{openPostingFixture(t, large)}, [][]signal.Series{large})
	var got, want bytes.Buffer
	require.NoError(t, actual.Write(&got))
	require.NoError(t, expected.Write(&want))
	require.Nil(t, actual.mergedPostings, "tiny output avoids scanning all source postings")
	require.Equal(t, want.Bytes(), got.Bytes())
}

func indexesFromBytes(tb testing.TB, a, b []byte) (*Index, *Index) {
	tb.Helper()
	be := backend.Memory()
	require.NoError(tb, be.Write(tb.Context(), "a", a))
	require.NoError(tb, be.Write(tb.Context(), "b", b))
	ia, err := Open(tb.Context(), be, "a")
	require.NoError(tb, err)
	ib, err := Open(tb.Context(), be, "b")
	require.NoError(tb, err)
	return ia, ib
}

func comparePostingQueries(t *testing.T, actual, expected *Index) {
	t.Helper()
	var names, wantNames [][]byte
	require.NoError(t, actual.Names(t.Context(), func(name []byte) error { names = append(names, bytes.Clone(name)); return nil }))
	require.NoError(t, expected.Names(t.Context(), func(name []byte) error { wantNames = append(wantNames, bytes.Clone(name)); return nil }))
	require.Equal(t, wantNames, names)
	for _, name := range append(names, []byte("absent")) {
		for _, kind := range []signal.ValueKind{signal.KindEmpty, signal.KindStr, signal.KindInt, signal.KindDouble, signal.KindBytes, signal.KindMap, signal.KindSlice} {
			m := fetch.Matcher{Name: name, Match: func(v signal.Value) bool { return v.Kind() == kind }}
			got, err := actual.Resolve(t.Context(), []fetch.Matcher{m})
			require.NoError(t, err)
			want, err := expected.Resolve(t.Context(), []fetch.Matcher{m})
			require.NoError(t, err)
			var gotRefs, wantRefs []signal.SeriesID
			for got.Next() {
				gotRefs = append(gotRefs, got.At())
			}
			require.NoError(t, got.Err())
			for want.Next() {
				wantRefs = append(wantRefs, want.At())
			}
			require.NoError(t, want.Err())
			require.Equal(t, wantRefs, gotRefs, "name=%q kind=%v", name, kind)
		}
	}
}

func TestMergedPostingsDuplicateChunkBoundary(t *testing.T) {
	t.Parallel()
	input := make([]signal.Series, 2050)
	for i := range input {
		input[i] = signal.Series{Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("same"), Value: signal.StringValue([]byte("value"))}, signal.KeyValue{Key: []byte("id"), Value: signal.IntValue(int64(i))})}
	}
	slices.SortFunc(input, func(a, b signal.Series) int { return a.Hash().Compare(b.Hash()) })
	sources := [][]signal.Series{input[:1536], input[1024:]}
	actual, expected := postingMergeBuilder(t, input, []*Index{openPostingFixture(t, sources[0]), openPostingFixture(t, sources[1])}, sources)
	var got, want bytes.Buffer
	require.NoError(t, actual.Write(&got))
	require.NoError(t, expected.Write(&want))
	require.Equal(t, want.Bytes(), got.Bytes())
	require.Equal(t, -1, actual.mergedPostings.selectedSource)
}

func corruptPostingFixture(tb testing.TB, original *Index, prefix byte, transform func([]byte, []byte) ([]byte, []byte)) *Index {
	tb.Helper()
	var data bytes.Buffer
	w := NewTableWriter(&data)
	w.seriesCount = original.count
	c := original.table.Cursor(tb.Context())
	changed := false
	for ok := c.Seek(nil); ok; ok = c.Next() {
		key, value := c.Key(), c.Value()
		if !changed && len(key) > 0 && key[0] == prefix {
			key, value = transform(bytes.Clone(key), bytes.Clone(value))
			changed = true
		}
		require.NoError(tb, w.Append(key, value))
	}
	require.NoError(tb, c.Err())
	require.True(tb, changed)
	require.NoError(tb, w.Finish())
	be := backend.Memory()
	require.NoError(tb, be.Write(tb.Context(), "index", data.Bytes()))
	idx, err := Open(tb.Context(), be, "index")
	require.NoError(tb, err)
	return idx
}

func TestMergedPostingsRejectsContributingSourceCorruption(t *testing.T) {
	t.Parallel()
	input := builderViewFixture()
	good := openPostingFixture(t, input)
	for _, tc := range []struct {
		name      string
		prefix    byte
		transform func([]byte, []byte) ([]byte, []byte)
	}{
		{"reference bounds", 'h', func(key, _ []byte) ([]byte, []byte) {
			return key, binary.AppendUvarint([]byte{1}, uint64(len(input)))
		}},
		{"trailing payload", 'p', func(key, value []byte) ([]byte, []byte) { return key, append(value, 0) }},
		{"name flags", 'n', func(key, _ []byte) ([]byte, []byte) { return key, []byte{2} }},
		{"chunk key", 'h', func(key, value []byte) ([]byte, []byte) { return append(key, 0), value }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			bad := corruptPostingFixture(t, good, tc.prefix, tc.transform)
			actual := partitionPostingMergeBuilder(t, input, []*Index{good, bad})
			var got bytes.Buffer
			require.ErrorIs(t, actual.Write(&got), ErrCorrupt, "every selected membership source must be validated")
		})
	}
}

func FuzzMergedPostingsMatchesBuilder(f *testing.F) {
	input := builderViewFixture()
	for i := range input {
		s := input[i]
		f.Add(s.AppendHashInput(nil))
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 1<<16 {
			return
		}
		s, n, err := signal.DecodeSeries(raw)
		if err != nil || n != len(raw) {
			return
		}
		input := []signal.Series{s}
		actual, expected := postingMergeBuilder(t, input, []*Index{openPostingFixture(t, input), openPostingFixture(t, input)}, [][]signal.Series{input, input})
		var got, want bytes.Buffer
		require.NoError(t, actual.Write(&got))
		require.NoError(t, expected.Write(&want))
		require.Equal(t, want.Bytes(), got.Bytes())
	})
}

func TestMergedPostingsHeterogeneousSources(t *testing.T) {
	t.Parallel()
	input := builderViewFixture()
	sources := [][]signal.Series{{input[0], input[2], input[4]}, {input[1], input[2], input[3]}}
	actual, expected := postingMergeBuilder(t, input, []*Index{openPostingFixture(t, sources[0]), openPostingFixture(t, sources[1])}, sources)
	var got, want bytes.Buffer
	require.NoError(t, actual.Write(&got))
	require.NoError(t, expected.Write(&want))
	require.Equal(t, want.Bytes(), got.Bytes(), "the shared ID has different source-local references")
}

func TestMergedPostingsReferenceValidation(t *testing.T) {
	t.Parallel()
	input := builderViewFixture()
	m := NewPostingMerge(t.Context(), []*Index{openPostingFixture(t, input)})
	require.Error(t, m.Append(-1, 0, 0))
	require.Error(t, m.Append(1, 0, 0))
	require.Error(t, m.Append(0, uint32(len(input)), 0))
	require.NoError(t, m.Append(0, 0, 0))
	require.Error(t, m.Append(0, 0, 1))
	require.Error(t, m.Append(0, 1, 0))
	var builder Builder
	builder.MergePostings(m)
	require.NoError(t, builder.AddIdentity(input[0].Hash(), input[0], Metadata{}))
	require.NoError(t, m.Append(0, 1, 100))
	var buf bytes.Buffer
	require.ErrorContains(t, builder.Write(&buf), "output reference bounds")
}

func TestMergedPostingsRejectsContributingSourceCRC(t *testing.T) {
	t.Parallel()
	input := builderViewFixture()
	good, bad := openPostingFixture(t, input), openPostingFixture(t, input)
	data, err := bad.table.b.Read(t.Context(), bad.table.key)
	require.NoError(t, err)
	data[0] ^= 1
	require.NoError(t, bad.table.b.Write(t.Context(), bad.table.key, data))
	actual := partitionPostingMergeBuilder(t, input, []*Index{good, bad})
	var buf bytes.Buffer
	require.ErrorIs(t, actual.Write(&buf), ErrCorrupt)
}

func partitionPostingMergeBuilder(tb testing.TB, input []signal.Series, indexes []*Index) *Builder {
	tb.Helper()
	actual, _ := postingMergeBuilder(tb, input, indexes, [][]signal.Series{nil, nil})
	for i := range input {
		source := 0
		if i >= len(input)/2 {
			source = 1
		}
		require.NoError(tb, actual.mergedPostings.Append(source, uint32(i), uint32(i)))
	}
	return actual
}

func TestMergedPostingsChoosesSmallestCoveringSource(t *testing.T) {
	t.Parallel()
	input := builderViewFixture()
	large := slices.Clone(input)
	for i := range 20 {
		large = append(large, signal.Series{Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("extra"), Value: signal.IntValue(int64(i))})})
	}
	slices.SortFunc(large, func(a, b signal.Series) int { return a.Hash().Compare(b.Hash()) })
	bigIndex := openPostingFixture(t, large)
	unused := corruptPostingFixture(t, bigIndex, 'p', func(key, value []byte) ([]byte, []byte) { return key, append(value, 0) })
	actual, expected := postingMergeBuilder(t, input, []*Index{unused, openPostingFixture(t, input)}, [][]signal.Series{large, input})
	var got, want bytes.Buffer
	require.NoError(t, actual.Write(&got))
	require.NoError(t, expected.Write(&want))
	require.Equal(t, want.Bytes(), got.Bytes())
	require.NotNil(t, actual.mergedPostings)
	require.Equal(t, 1, actual.mergedPostings.selectedSource, "complete smaller index supplies all output memberships")
}

func TestMergedPostingsDuplicateHeavyUnionFallsBack(t *testing.T) {
	t.Parallel()
	input := builderViewFixture()
	sources := make([][]signal.Series, 0, len(input))
	indexes := make([]*Index, 0, len(input))
	for i := range input {
		subset := append(slices.Clone(input[:i]), input[i+1:]...)
		sources = append(sources, subset)
		indexes = append(indexes, openPostingFixture(t, subset))
	}
	actual, expected := postingMergeBuilder(t, input, indexes, sources)
	var got, want bytes.Buffer
	require.NoError(t, actual.Write(&got))
	require.NoError(t, expected.Write(&want))
	require.Equal(t, want.Bytes(), got.Bytes())
	require.Nil(t, actual.mergedPostings)
}
