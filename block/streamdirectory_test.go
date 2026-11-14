package block

import (
	"encoding/binary"
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
)

func TestDirectoryVarintBytes(t *testing.T) {
	t.Parallel()
	for _, value := range []int64{math.MinInt64, -16384, -128, -64, -1, 0, 1, 63, 64, 127, 128, 16384, math.MaxInt64} {
		t.Run(strconv.FormatInt(value, 10), func(t *testing.T) {
			t.Parallel()
			require.EqualValues(t, len(binary.AppendVarint(nil, value)), directoryVarintBytes(value))
			require.EqualValues(t, len(binary.AppendUvarint(nil, uint64(value))), directoryUvarintBytes(uint64(value)))
		})
	}
}

func directoryCounterCorpus() [][]byte {
	return [][]byte{{0, 1, 2}, {1, 0, 127, 128, 255, 17, 3}, {255, 0, 255, 0, 255}}
}

func directoryCounterWriter(t *testing.T, data []byte, mode int, be backend.Backend) *StreamWriter {
	t.Helper()
	opts := []PartOption{WithIndependentGroups(), WithGranuleSize(4), WithSortKey("ts"), WithCompression(compress.AlgorithmLZ4)}
	w := NewStreamWriter(opts...)
	if be != nil {
		w = NewStreamWriterTo(t.Context(), be, "directory", opts...)
	}
	t.Cleanup(w.Abort)
	require.NoError(t, w.AddColumn(Column{Name: "ts", Kind: KindInt64, Codec: chunk.CodecAdaptiveTimestamps, Block: true}))
	value := Column{Name: "value", Kind: KindFloat64, Codec: chunk.CodecFloatGroups, Block: true}
	if mode == 3 {
		value.AutoCodec, value.Codec = true, chunk.CodecNone
	}
	require.NoError(t, w.AddColumn(value))
	for i, v := range data {
		timestamps := []int64{int64(i)*15000000000 + int64(v), int64(i)*15000000000 + int64(v) + 1}
		values := []float64{float64(v), float64(v) + 0.5}
		if mode == 2 {
			ts, err := chunk.EncodeTimestampsAdaptive(nil, timestamps)
			require.NoError(t, err)
			td := encodedDecoder(KindInt64, chunk.CodecAdaptiveTimestamps, len(timestamps), ts)
			tg, err := td.EncodedGranule(0)
			require.NoError(t, err)
			var encoder chunk.FloatGroupEncoder
			raw, err := encoder.Encode(nil, values, []int{len(values)})
			require.NoError(t, err)
			vd := encodedDecoder(KindFloat64, chunk.CodecFloatGroups, len(values), raw)
			vg, err := vd.EncodedGranule(0)
			require.NoError(t, err)
			require.NoError(t, w.AppendEncodedRun(0, 1, tg, vg, vg.FloatStats()))
		} else {
			require.NoError(t, w.AppendInt64(0, timestamps))
			if mode == 3 {
				require.NoError(t, w.AppendFloat64(1, values))
			} else {
				require.NoError(t, w.AppendFloat64Group(1, values, true))
			}
		}
		if i+1 < len(data) || mode == 0 {
			require.NoError(t, w.EndGroup())
		}
	}
	return w
}

func checkDirectoryCounters(t *testing.T, w *StreamWriter) {
	t.Helper()
	before := make(map[*blockAccum]int64)
	for _, column := range w.cols {
		for _, a := range []*blockAccum{column.blk, column.alt} {
			if a != nil {
				before[a] = a.finishDirectoryBytes(column.timeBounds)
			}
		}
	}
	_, err := w.build()
	require.NoError(t, err)
	for a, bound := range before {
		require.True(t, a.directoryTracked)
		exact := a.rawDirectoryBytes(w.granuleSize)
		require.LessOrEqual(t, exact+footerLenBytes, bound)
		a.compactDir = false
		raw := a.encodeDir(w.granuleSize)
		require.EqualValues(t, len(raw), exact)
		require.Equal(t, len(raw), cap(raw), "raw directory is allocated exactly once")
		a.directoryTracked = false
		require.Equal(t, raw, a.encodeDir(w.granuleSize), "preallocation must preserve the existing wire bytes")
		a.directoryTracked = true
	}
}

func TestStreamDirectoryCounterPaths(t *testing.T) {
	t.Parallel()
	for mode, name := range []string{"normal", "pending", "encoded-pending", "alternate-pending"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			for _, kind := range []string{"buffered", "memory-writer", "file"} {
				t.Run(kind, func(t *testing.T) {
					t.Parallel()
					var be backend.Backend
					switch kind {
					case "memory-writer":
						be = backend.Memory()
					case "file":
						var err error
						be, err = backendfile.New(t.TempDir())
						require.NoError(t, err)
					}
					w := directoryCounterWriter(t, directoryCounterCorpus()[1], mode, be)
					switch mode {
					case 1:
						require.NotEmpty(t, w.cols[0].stageI64)
					case 2:
						require.Positive(t, w.encodedRun.rows)
					case 3:
						require.NotNil(t, w.cols[1].alt)
						require.True(t, w.cols[1].altOK)
					}
					checkDirectoryCounters(t, w)
				})
			}
		})
	}
}

func TestStreamDirectoryCounterWideTimeDeltas(t *testing.T) {
	t.Parallel()
	w := NewStreamWriter(WithIndependentGroups(), WithGranuleSize(4), WithSortKey("ts"))
	t.Cleanup(w.Abort)
	require.NoError(t, w.AddColumn(Column{Name: "ts", Kind: KindInt64, Codec: chunk.CodecAdaptiveTimestamps, Block: true}))
	for _, timestamps := range [][]int64{{math.MinInt64, math.MinInt64 + 1}, {math.MaxInt64 - 1, math.MaxInt64}, {-1, 0}} {
		require.NoError(t, w.AppendInt64(0, timestamps))
		require.NoError(t, w.EndGroup())
	}
	checkDirectoryCounters(t, w)
}

func TestStreamDirectoryBoundUsesEncodedLengths(t *testing.T) {
	t.Parallel()
	w := directoryCounterWriter(t, make([]byte, 4096), 0, nil)
	for _, column := range w.cols {
		a := column.blk
		old := compactDirBound(ColumnSizing{NumFrames: int64(a.frameCount + 1), NumGranules: int64(len(a.gLens) + 1)}, column.timeBounds)
		bound := a.finishDirectoryBytes(column.timeBounds)
		t.Logf("column=%s old_raw_bound=%d tracked_raw_bound=%d", column.name, old, bound)
		require.Less(t, bound, old/2)
	}
	checkDirectoryCounters(t, w)
}

func FuzzStreamDirectoryCounters(f *testing.F) {
	for _, data := range directoryCounterCorpus() {
		for mode := range byte(4) {
			f.Add(data, mode)
		}
	}
	f.Fuzz(func(t *testing.T, data []byte, mode byte) {
		if len(data) == 0 || len(data) > 128 {
			return
		}
		checkDirectoryCounters(t, directoryCounterWriter(t, data, int(mode%4), nil))
	})
}
