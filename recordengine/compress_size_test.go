package recordengine

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
	"github.com/oteldb/storage/signal"
)

// TestMergeCompressionShrinksParts proves the cold-recompression win: merged (compacted) record parts
// written with MergeCompression=ZSTD are dramatically smaller than the codec-only (AlgorithmNone)
// default, because record byte columns are dict-coded but were never entropy-coded.
func TestMergeCompressionShrinksParts(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	schema := NewSchema(
		Column{Name: "sev", Kind: KindInt64, Codec: chunk.CodecT64},
		Column{Name: "body", Kind: KindBytes, Codec: chunk.CodecDict, Bloom: BloomFullText},
		Column{Name: "attrs", Kind: KindBytes, Codec: chunk.CodecDict, Bloom: BloomAttrs},
	)

	// One stream of repetitive, log-shaped records — the realistic case that compresses well.
	series := signal.Series{Resource: signal.Resource{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte("svc"))},
	)}}
	tmpls := []string{
		"GET /api/v1/users/%d status=200 latency=%dms done",
		"level=info msg=\"request processed\" route=/route/%d code=%d",
		"reconcile succeeded object=deploy/%d generation=%d",
	}

	// backendSize builds an engine with the given merge compression, ingests two flushes worth of
	// records, merges them into one compacted part, and returns the resulting on-disk byte total.
	backendSize := func(comp compress.Algorithm) int {
		be := backend.Memory()
		e := New(Config{
			Schema: schema, Backend: be, Prefix: "t/recs",
			MergeCompression: comp, MergeCompressionLevel: compress.LevelBest,
		})

		for round := range 2 {
			const per = 25_000
			ts := make([]int64, per)
			sev := make([]int64, per)
			body := make([][]byte, per)
			attrs := make([][]byte, per)
			for i := range per {
				n := round*per + i
				ts[i] = int64(n)
				sev[i] = int64(9 + i%9)
				body[i] = fmt.Appendf(nil, tmpls[i%len(tmpls)], i%5000, i%500)
				attrs[i] = fmt.Appendf(nil, `{"pod":"svc-%d","level":"info"}`, i%64)
			}
			b := &Batch{
				Stream: series.Hash(), Identity: func() signal.Series { return series },
				Ts: ts, Ints: [][]int64{sev}, Bytes: [][][]byte{body, attrs},
			}
			_, err := e.AppendBatch(b, AppendLimits{})
			require.NoError(t, err)
			require.NoError(t, e.Flush(ctx))
		}

		require.NoError(t, e.Merge(ctx, 0)) // compact the two flush parts into one (compressed) part

		keys, err := be.List(ctx, "")
		require.NoError(t, err)
		total := 0
		for _, k := range keys {
			obj, rerr := be.Read(ctx, k)
			require.NoError(t, rerr)
			total += len(obj)
		}
		return total
	}

	none := backendSize(compress.AlgorithmNone)
	zstd := backendSize(compress.AlgorithmZSTD)

	t.Logf("merged part on-disk: none=%d B, zstd=%d B (%.1f%% smaller)",
		none, zstd, 100*(1-float64(zstd)/float64(none)))

	// The dict-coded byte columns entropy-code well; require at least a 2× reduction (measured ≫ that
	// on log-shaped data), guarding the win without being brittle.
	assert.Less(t, zstd*2, none, "ZSTD-compressed merged part must be well under half the uncompressed size")
}
