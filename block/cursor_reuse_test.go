package block

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
)

func TestBlockedCursorReusesMetricDecoders(t *testing.T) {
	t.Parallel()
	for _, algorithm := range []compress.Algorithm{compress.AlgorithmNone, compress.AlgorithmLZ4, compress.AlgorithmZSTD} {
		t.Run(algorithm.String(), func(t *testing.T) {
			t.Parallel()
			be := backend.Memory()
			writer := NewStreamWriterTo(t.Context(), be, "p", WithIndependentGroups(), WithGranuleSize(16), WithCompression(algorithm))
			defer writer.Abort()
			require.NoError(t, writer.AddColumn(Column{Name: "ts", Kind: KindInt64, Codec: chunk.CodecAdaptiveTimestamps, Block: true}))
			require.NoError(t, writer.AddColumn(Column{Name: "v", Kind: KindFloat64, Codec: chunk.CodecFloatGroups, Block: true}))
			var timestamps []int64
			var values []float64
			for group, count := range []int{16, 3, 8, 1, 16} {
				ts, vs := make([]int64, count), make([]float64, count)
				for i := range ts {
					ts[i] = int64(i)*15_000_000_000 + int64(group)
					if group%2 == 1 {
						ts[i] += int64(i * i)
					}
					vs[i] = float64(i+group) / 1000
					if group == 2 {
						vs[i] = math.Copysign(0, -1)
					}
					if group == 3 {
						vs[i] = math.Float64frombits(0x7ff8000000004321)
					}
				}
				require.NoError(t, writer.AppendInt64(0, ts))
				require.NoError(t, writer.AppendFloat64Group(1, vs, true))
				require.NoError(t, writer.EndGroup())
				timestamps, values = append(timestamps, ts...), append(values, vs...)
			}
			require.NoError(t, WriteStreamPart(t.Context(), be, "p", writer))
			reader, err := OpenPart(t.Context(), be, "p")
			require.NoError(t, err)
			defer reader.ReleaseReaders()
			timestampScan, err := reader.OpenColumnScan(t.Context(), "ts", 1)
			require.NoError(t, err)
			defer func() { require.NoError(t, timestampScan.Close()) }()
			floatScan, err := reader.OpenColumnScan(t.Context(), "v", 1)
			require.NoError(t, err)
			defer func() { require.NoError(t, floatScan.Close()) }()
			tsCursor, err := timestampScan.TsCursor()
			require.NoError(t, err)
			floatCursor, err := floatScan.FloatCursor()
			require.NoError(t, err)
			tsBlocked, ok := tsCursor.(*blockedTsCursor)
			require.True(t, ok)
			floatBlocked, ok := floatCursor.(*blockedFloatCursor)
			require.True(t, ok)
			var firstTimestamp chunk.TsCursor
			var firstFloat chunk.FloatDecoder
			for i, want := range timestamps {
				got, err := tsCursor.Next()
				require.NoError(t, err)
				require.Equal(t, want, got)
				v, err := floatCursor.Next()
				require.NoError(t, err)
				require.Equal(t, math.Float64bits(values[i]), math.Float64bits(v))
				if i == 0 {
					firstTimestamp, firstFloat = tsBlocked.cur, floatBlocked.cur
				} else {
					require.Same(t, firstTimestamp, tsBlocked.cur)
					require.Same(t, firstFloat, floatBlocked.cur)
				}
			}
			_, err = tsCursor.Next()
			require.Error(t, err)
			_, err = floatCursor.Next()
			require.Error(t, err)

			// A successful reset must still check the next block's declared row count.
			require.NoError(t, floatScan.Close())
			floatScan, err = reader.OpenColumnScan(t.Context(), "v", 1)
			require.NoError(t, err)
			floatCursor, err = floatScan.FloatCursor()
			require.NoError(t, err)
			floatBlocked, ok = floatCursor.(*blockedFloatCursor)
			require.True(t, ok)
			floatBlocked.streams.dir.rowStarts = append([]int64(nil), floatBlocked.streams.dir.rowStarts...)
			floatBlocked.streams.dir.rowStarts[len(floatBlocked.streams.dir.rowStarts)-1]++
			for range len(values) - 16 {
				_, err = floatCursor.Next()
				require.NoError(t, err)
			}
			_, err = floatCursor.Next()
			require.ErrorContains(t, err, "grouped cursor row mismatch")
		})
	}
}
