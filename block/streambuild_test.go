package block

import (
	"bytes"
	"slices"
	"testing"

	"github.com/go-faster/errors"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/faultbackend"
	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
)

func scratchMetricWriter(tb testing.TB, b backend.Backend, prefix string, streamed, omit bool, base int64) *StreamWriter {
	tb.Helper()
	opts := []PartOption{WithSortKey("ts"), WithGranuleSize(4), WithCompressBlockBytes(16), WithCompression(compress.AlgorithmLZ4), WithSizingStats()}
	w := NewStreamWriter(opts...)
	if streamed {
		w = NewStreamWriterTo(tb.Context(), b, prefix, opts...)
	}
	require.NoError(tb, w.AddColumn(Column{Name: "series", Kind: KindInt128}))
	require.NoError(tb, w.AddColumn(Column{Name: "ts", Kind: KindInt64, Codec: chunk.CodecDoD, Block: true}))
	require.NoError(tb, w.AddColumn(Column{Name: "value", Kind: KindFloat64, Codec: chunk.CodecGorilla, Block: true}))
	if omit {
		require.NoError(tb, w.AddColumn(Column{Name: "resource", Kind: KindBytes, Codec: chunk.CodecDict, Block: true}))
		require.NoError(tb, w.AddColumn(Column{Name: "sf", Kind: KindFloat64, Block: true}))
		require.NoError(tb, w.OmitConstColumn(1))
	}
	const n = 17
	ts, values := make([]int64, n), make([]float64, n)
	for i := range ts {
		ts[i], values[i] = base+int64(i), float64(i%7)+0.25
	}
	require.NoError(tb, w.AppendU128Run(0, chunk.U128{Lo: 1}, n))
	require.NoError(tb, w.AppendInt64(1, ts))
	require.NoError(tb, w.AppendFloat64(2, values))
	if omit {
		attrs, weights := make([][]byte, n), make([]float64, n)
		for i := range attrs {
			attrs[i], weights[i] = []byte("constant-resource"), 1
		}
		require.NoError(tb, w.AppendBytes(3, attrs))
		require.NoError(tb, w.AppendFloat64(4, weights))
	}
	return w
}

func TestStreamBuildScratchPreservesParts(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"memory", "cached-memory", "file", "cached-file"} {
		for _, streamed := range []bool{false, true} {
			t.Run(kind+"/streamed="+boolText(streamed), func(t *testing.T) {
				t.Parallel()
				b := backend.Memory()
				if kind == "file" || kind == "cached-file" {
					files, err := backendfile.New(t.TempDir())
					require.NoError(t, err)
					b = files
				}
				if kind == "cached-memory" || kind == "cached-file" {
					b = backend.Cached(b, 1<<20)
				}
				var scratch StreamBuildScratch
				w := scratchMetricWriter(t, b, "first", streamed, true, 100)
				require.NoError(t, WriteStreamPartWithScratch(t.Context(), b, "first", w, &scratch))
				marks := w.Marks()
				wantMarks := slices.Clone(marks.Granules)
				firstManifest, err := backend.ReadView(t.Context(), b, manifestKey("first"))
				require.NoError(t, err)
				wantManifest := bytes.Clone(firstManifest)
				firstEncodedMarks, err := backend.ReadView(t.Context(), b, marksKey("first"))
				require.NoError(t, err)
				wantEncodedMarks := bytes.Clone(firstEncodedMarks)
				before := scratch.SizeBytes()
				require.Positive(t, before)
				manifestBuffer := &scratch.manifest[:cap(scratch.manifest)][0]
				w.Abort()
				w2 := scratchMetricWriter(t, b, "second", streamed, false, 10000)
				require.NoError(t, WriteStreamPartWithScratch(t.Context(), b, "second", w2, &scratch))
				require.Same(t, manifestBuffer, &scratch.manifest[:cap(scratch.manifest)][0])
				require.Equal(t, wantMarks, marks.Granules, "published raw marks outlive scratch reuse and Abort")
				require.Equal(t, wantManifest, firstManifest, "old cache views remain immutable")
				require.Equal(t, wantEncodedMarks, firstEncodedMarks)
				gotManifest, err := b.Read(t.Context(), manifestKey("first"))
				require.NoError(t, err)
				require.Equal(t, wantManifest, gotManifest)
				reader, err := OpenPart(t.Context(), b, "first")
				require.NoError(t, err)
				defer reader.ReleaseReaders()
				col, err := reader.Column(t.Context(), "ts")
				require.NoError(t, err)
				values, err := col.Int64(nil)
				require.NoError(t, err)
				require.EqualValues(t, 100, values[0])
				require.EqualValues(t, 116, values[len(values)-1])
				for _, desc := range scratch.descriptors[:cap(scratch.descriptors)] {
					require.Empty(t, desc.Name)
					require.Nil(t, desc.ConstBytes)
				}
				for _, object := range scratch.objects[:cap(scratch.objects)] {
					require.Nil(t, object)
				}
			})
		}
	}
}

func boolText(v bool) string {
	if v {
		return "true"
	}
	return "false"
}

func TestStreamBuildScratchOutputMatchesBuild(t *testing.T) {
	t.Parallel()
	for _, omit := range []bool{false, true} {
		b := backend.Memory()
		original := scratchMetricWriter(t, b, "expected", false, omit, 100)
		expected, err := original.build()
		require.NoError(t, err)
		var scratch StreamBuildScratch
		w := scratchMetricWriter(t, b, "actual", false, omit, 100)
		require.NoError(t, WriteStreamPartWithScratch(t.Context(), b, "actual", w, &scratch))
		got, err := b.Read(t.Context(), manifestKey("actual"))
		require.NoError(t, err)
		require.Equal(t, expected.manifest, got)
		got, err = b.Read(t.Context(), marksKey("actual"))
		require.NoError(t, err)
		require.Equal(t, expected.marks, got)
		for i, object := range expected.objects {
			if object != nil {
				got, err = b.Read(t.Context(), columnKey("actual", i))
				require.NoError(t, err)
				require.Equal(t, object, got)
			}
		}
	}
}

func TestStreamBuildScratchFailuresClearReferences(t *testing.T) {
	t.Parallel()
	for _, fail := range []string{"build", "column", "marks", "manifest", "prefix"} {
		t.Run(fail, func(t *testing.T) {
			t.Parallel()
			be := faultbackend.Wrap(backend.Memory())
			w := scratchMetricWriter(t, be, "part", fail == "prefix", true, 100)
			defer w.Abort()
			prefix := "part"
			switch fail {
			case "build":
				w.cols[1].rows++
			case "prefix":
				prefix = "wrong"
			default:
				key := map[string]string{"column": columnKey("part", 0), "marks": marksKey("part"), "manifest": manifestKey("part")}[fail]
				be.Add(faultbackend.Rule{Kind: faultbackend.Write, Match: func(op faultbackend.Op) bool { return op.Key == key }, Err: errors.New("write failed")})
			}
			scratch := StreamBuildScratch{descriptors: make([]ColumnDesc, 0, 8), objects: make([][]byte, 0, 8)}
			scratch.descriptors[:cap(scratch.descriptors)][7] = ColumnDesc{Name: "stale", ConstBytes: []byte("old")}
			scratch.objects[:cap(scratch.objects)][7] = []byte("old")
			require.Error(t, WriteStreamPartWithScratch(t.Context(), be, prefix, w, &scratch))
			for _, desc := range scratch.descriptors[:cap(scratch.descriptors)] {
				require.Empty(t, desc.Name)
				require.Nil(t, desc.ConstBytes)
			}
			for _, object := range scratch.objects[:cap(scratch.objects)] {
				require.Nil(t, object)
			}
		})
	}
}

func TestStreamBuildScratchRetainedLimit(t *testing.T) {
	t.Parallel()
	scratch := StreamBuildScratch{marks: make([]byte, 0, maxStreamBuildScratchBytes+1)}
	require.Greater(t, scratch.SizeBytes(), int64(maxStreamBuildScratchBytes))
	w := NewStreamWriter()
	require.Error(t, WriteStreamPartWithScratch(t.Context(), backend.Memory(), "empty", w, &scratch))
	require.Zero(t, scratch.SizeBytes())
}

func TestStreamBuildScratchSerializationPriced(t *testing.T) {
	t.Parallel()
	w := scratchMetricWriter(t, backend.Memory(), "part", false, true, 100)
	bound := w.buildBytes()
	require.GreaterOrEqual(t, w.FinishBytes(), bound)
	var scratch StreamBuildScratch
	_, err := w.buildInto(&scratch)
	require.NoError(t, err)
	require.GreaterOrEqual(t, bound, scratch.SizeBytes())
	const payload = 1 << 20
	large := NewStreamWriter()
	require.NoError(t, large.AddColumn(Column{Name: "constant", Kind: KindBytes, Block: true}))
	require.NoError(t, large.AppendBytes(0, [][]byte{make([]byte, payload)}))
	require.GreaterOrEqual(t, large.buildBytes(), int64(3*payload))
}

func TestStreamBuildScratchIndependentGroups(t *testing.T) {
	t.Parallel()
	be := backend.Memory()
	var scratch StreamBuildScratch
	for _, prefix := range []string{"first", "second"} {
		w := NewStreamWriterTo(t.Context(), be, prefix, WithIndependentGroups(), WithGranuleSize(4), WithSizingStats())
		require.NoError(t, w.AddColumn(Column{Name: "series", Kind: KindInt128}))
		require.NoError(t, w.AddColumn(Column{Name: "ts", Kind: KindInt64, Codec: chunk.CodecAdaptiveTimestamps, Block: true}))
		require.NoError(t, w.AddColumn(Column{Name: "value", Kind: KindFloat64, Codec: chunk.CodecFloatGroups, Block: true}))
		for i := range 3 {
			require.NoError(t, w.AppendU128Run(0, chunk.U128{Lo: uint64(i)}, 2))
			require.NoError(t, w.AppendInt64(1, []int64{100, 101}))
			require.NoError(t, w.AppendFloat64Group(2, []float64{float64(i), float64(i) + 0.25}, true))
			require.NoError(t, w.EndGroup())
		}
		require.NoError(t, WriteStreamPartWithScratch(t.Context(), be, prefix, w, &scratch))
		reader, err := OpenPart(t.Context(), be, prefix)
		require.NoError(t, err)
		col, err := reader.Column(t.Context(), "ts")
		require.NoError(t, err)
		values, err := col.Int64(nil)
		require.NoError(t, err)
		require.Equal(t, []int64{100, 101, 100, 101, 100, 101}, values)
		reader.ReleaseReaders()
	}
}

func FuzzStreamBuildScratchReuse(f *testing.F) {
	f.Add(int64(100), []byte{1, 2, 3})
	f.Add(int64(-100), []byte{0, 255, 0, 255})
	f.Fuzz(func(t *testing.T, base int64, data []byte) {
		if len(data) > 256 {
			return
		}
		be := backend.Cached(backend.Memory(), 1<<20)
		var scratch StreamBuildScratch
		for round := range 2 {
			prefix := []string{"first", "second"}[round]
			w := NewStreamWriter(WithGranuleSize(4))
			require.NoError(t, w.AddColumn(Column{Name: "ts", Kind: KindInt64, Codec: chunk.CodecDoD, Block: true}))
			values := []int64{base}
			for _, value := range data {
				values = append(values, base+int64(value)+int64(round))
			}
			require.NoError(t, w.AppendInt64(0, values))
			require.NoError(t, WriteStreamPartWithScratch(t.Context(), be, prefix, w, &scratch))
			reader, err := OpenPart(t.Context(), be, prefix)
			require.NoError(t, err)
			column, err := reader.Column(t.Context(), "ts")
			require.NoError(t, err)
			got, err := column.Int64(nil)
			require.NoError(t, err)
			require.Equal(t, values, got)
			reader.ReleaseReaders()
		}
	})
}
