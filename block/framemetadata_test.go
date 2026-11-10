package block

import (
	"bytes"
	"context"
	"encoding/binary"
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/compress"
)

type frameMetadataRecorder struct{ writes [][]byte }

func (w *frameMetadataRecorder) Write(data []byte) (int, error) {
	w.writes = append(w.writes, bytes.Clone(data))
	return len(data), nil
}
func (*frameMetadataRecorder) Commit(context.Context) error { return nil }
func (*frameMetadataRecorder) Abort()                       {}

func TestPackedFrameMetadataMatchesDirectory(t *testing.T) {
	t.Parallel()
	comp := compress.NewFrameCompressor(compress.AlgorithmNone, 0, 10)
	a := newBlockAccum(comp, 10)
	var sink frameMetadataRecorder
	require.NoError(t, a.attach(func() (backend.ObjectWriter, error) { return &sink, nil }))
	lengths := []int{3, 7, 12, 1, 7}
	for _, n := range lengths {
		require.NoError(t, a.addGranule(func(dst []byte) ([]byte, error) {
			for range n {
				dst = append(dst, 7)
			}
			return dst, nil
		}))
	}
	_, _, err := a.finish(t.Context(), 1, nil)
	require.NoError(t, err)
	require.Len(t, sink.writes, 4)
	want := binary.AppendUvarint(nil, uint64(len(lengths)))
	want = binary.AppendUvarint(want, 1)
	want = binary.AppendUvarint(want, 3)
	maxPacked := 0
	for i, granules := range []int{2, 1, 2} {
		frame := sink.writes[i]
		maxPacked = max(maxPacked, len(frame))
		want = binary.AppendUvarint(want, uint64(granules))
		want = binary.AppendUvarint(want, uint64(len(frame)))
		want = binary.LittleEndian.AppendUint32(want, crc32.Checksum(frame, castagnoli))
	}
	for _, n := range lengths {
		want = binary.AppendUvarint(want, uint64(n))
	}
	want = binary.LittleEndian.AppendUint32(want, crc32.Checksum(want, castagnoli))
	require.Equal(t, want, a.encodeDir(1))
	require.Equal(t, binary.LittleEndian.AppendUint32(bytes.Clone(want), uint32(len(want))), sink.writes[3])
	sizing := a.sizing()
	require.EqualValues(t, 3, sizing.NumFrames)
	require.EqualValues(t, 5, sizing.NumGranules)
	require.EqualValues(t, maxPacked, sizing.MaxFrameBytes)
	require.EqualValues(t, 12, sizing.MaxFrameRaw)
	require.EqualValues(t, 12, sizing.MaxGranuleRaw)
	require.EqualValues(t, 30, sizing.StreamRaw)
	require.EqualValues(t, 30, a.sealedRaw)
}

func TestFrameMetadataReader(t *testing.T) {
	t.Parallel()
	for _, input := range [][]byte{nil, {0}, {1}, {1, 1}, {1, 1, 0, 0, 0}, bytes.Repeat([]byte{255}, 12)} {
		_, _, err := nextFrameGranules(input)
		require.ErrorIs(t, err, ErrCorrupt)
	}
	var input []byte
	for _, n := range []int{1, 127, 128, 10000} {
		input = appendFrameMetadata(input, n, 1234, 0xdeadbeef)
	}
	for _, expected := range []int{1, 127, 128, 10000} {
		rest, got, err := nextFrameGranules(input)
		require.NoError(t, err)
		require.Equal(t, expected, got)
		input = rest
	}
	require.Empty(t, input)
}

func FuzzFrameMetadataReader(f *testing.F) {
	f.Add(appendFrameMetadata(nil, 1, 1234, 0xdeadbeef))
	f.Add([]byte{0})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, input []byte) {
		rest, count, err := nextFrameGranules(input)
		if err != nil {
			return
		}
		require.Positive(t, count)
		require.Less(t, len(rest), len(input))
	})
}
