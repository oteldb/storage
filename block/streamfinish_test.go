package block

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
)

func TestStreamFinishRetainsIDObjectBound(t *testing.T) {
	t.Parallel()
	for _, algorithm := range []compress.Algorithm{compress.AlgorithmNone, compress.AlgorithmLZ4, compress.AlgorithmZSTD} {
		t.Run(algorithm.String(), func(t *testing.T) {
			t.Parallel()
			w := NewStreamWriter(WithCompression(algorithm))
			require.NoError(t, w.AddColumn(Column{Name: "ids", Kind: KindInt128}))
			for i := range 2048 {
				require.NoError(t, w.AppendU128Run(0, chunk.U128{Hi: uint64(i), Lo: uint64(i * 31)}, i+1))
			}
			retained, _ := w.cols[0].finishBytes()
			_, object, _, err := w.cols[0].finish(t.Context(), 1024, true)
			require.NoError(t, err)
			require.GreaterOrEqual(t, retained, int64(cap(object)), "ID output survives subsequent numeric column finishes")
		})
	}
}

func TestStreamFinishKeepsBufferedDirectoryCharges(t *testing.T) {
	t.Parallel()
	files, err := backendfile.New(t.TempDir())
	require.NoError(t, err)
	var native, buffered *StreamWriter
	for _, be := range []backend.Backend{files, backend.Memory()} {
		w := NewStreamWriterTo(t.Context(), be, "p", WithIndependentGroups(), WithSortKey("ts"), WithCompression(compress.AlgorithmLZ4))
		t.Cleanup(w.Abort)
		require.NoError(t, w.AddColumn(Column{Name: "ts", Kind: KindInt64, Codec: chunk.CodecAdaptiveTimestamps, Block: true}))
		for i := range 1024 {
			require.NoError(t, w.AppendInt64(0, []int64{int64(i)}))
			require.NoError(t, w.EndGroup())
		}
		require.True(t, w.cols[0].blk.streams(), "both backend paths attach an ObjectWriter")
		if backend.StreamsWrites(be) {
			native = w
		} else {
			buffered = w
		}
	}
	require.Greater(t, buffered.FinishBytes(), native.FinishBytes(), "a buffering ObjectWriter retains the directory in memory")
	require.Positive(t, native.FinishBytes(), "streamed directory serialization is still charged")
}
