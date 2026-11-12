package engine

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/internal/watermark"
)

func TestSharedSeriesMergeRequiresCoverage(t *testing.T) {
	t.Parallel()
	for _, damage := range []string{"missing", "corrupt", "wrong-id"} {
		t.Run(damage, func(t *testing.T) {
			t.Parallel()
			be := backend.Memory()
			e := separateCatalogEngine(be)
			defer func() { require.NoError(t, e.CloseWAL()) }()
			s := benchSeries(1)
			for i := range 2 {
				_, err := e.Append(s, int64(i+1), float64(i))
				require.NoError(t, err)
				require.NoError(t, e.Flush(t.Context()))
			}
			p := e.parts[1]
			key := watermark.Key(p.prefix)
			valid := watermark.Encode(nil, []watermark.Entry{{ID: s.Hash(), Max: 100}})
			require.NoError(t, be.Write(t.Context(), key, valid))
			p.diskWatermark = nil
			covered, err := sourceCovered(t.Context(), e.parts, s.Hash())
			require.NoError(t, err)
			require.EqualValues(t, 100, covered)
			p.diskWatermark = nil
			p.seriesMax = []watermark.Entry{{ID: s.Hash(), Max: 2}}
			switch damage {
			case "missing":
				require.NoError(t, be.Delete(t.Context(), key))
			case "corrupt":
				require.NoError(t, be.Write(t.Context(), key, []byte("invalid")))
			default:
				require.NoError(t, be.Write(t.Context(), key, watermark.Encode(nil, []watermark.Entry{{ID: benchSeries(99).Hash(), Max: 100}})))
			}
			before, err := be.Read(t.Context(), e.indexKey())
			require.NoError(t, err)
			_, err = sourceCovered(t.Context(), e.parts, s.Hash())
			require.Error(t, err, "shared coverage cannot fall back to timestamps or legacy cached values")
			preflight := mergeWatermarkPreflight{entries: make(map[*part][]watermark.Entry), fallback: make(map[*part]bool)}
			_, err = preflight.read(t.Context(), e.parts)
			require.Error(t, err)
			require.Empty(t, preflight.fallback)
			require.Error(t, e.Merge(t.Context(), 0))
			after, err := be.Read(t.Context(), e.indexKey())
			require.NoError(t, err)
			require.Equal(t, before, after)
			require.Len(t, e.parts, 2)
			require.NoError(t, be.Write(t.Context(), key, valid))
			require.NoError(t, e.Merge(t.Context(), 0))
			covered, err = sourceCovered(t.Context(), e.parts, s.Hash())
			require.NoError(t, err)
			require.EqualValues(t, 100, covered)
		})
	}
}
