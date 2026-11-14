package engine

import (
	"testing"

	"github.com/stretchr/testify/require"

	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/internal/watermark"
)

func TestStreamedSidecarsPreserveBytes(t *testing.T) {
	t.Parallel()
	files, err := backendfile.New(t.TempDir())
	require.NoError(t, err)
	p := &partStreamWriter{e: New(Config{Backend: files}), prefix: "sidecars", withStats: true}
	ids := make([]chunk.U128, 0, 10000)
	marks := make([]watermark.Entry, 0, 10000)
	for i := range 10000 {
		id := chunk.U128{Hi: uint64(i), Lo: uint64(i * 31)}
		p.runs = append(p.runs, chunk.U128Run{Value: id, Count: i%5 + 1})
		p.rows += i%5 + 1
		p.covered = append(p.covered, int64(i)-5000)
		p.stats = append(p.stats, SeriesAgg{Count: float64(i%5 + 1), Rows: int64(i%5 + 1), Sum: float64(i) * 1.25, Min: float64(i), Max: float64(i + 2)})
		ids = append(ids, id)
		marks = append(marks, watermark.Entry{ID: u128ToID(id), Max: p.covered[i]})
	}
	_, streaming := p.sidecarWorkspace()
	require.True(t, streaming)
	p.sidecarWriterBytes, _ = p.sidecarWorkspace()
	require.NoError(t, p.writeSeriesSidecars(t.Context()))
	for key, want := range map[string][]byte{sidxKey(p.prefix): encodeSeriesIndexRuns(p.runs), watermark.Key(p.prefix): watermark.Encode(nil, marks), statsKey(p.prefix): encodeSeriesStats(ids, p.stats)} {
		got, err := files.Read(t.Context(), key)
		require.NoError(t, err)
		require.Equal(t, want, got, key)
	}
}
