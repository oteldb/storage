//go:build !race

package block

import (
	"context"
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
	"github.com/oteldb/storage/internal/heaptest"
)

// TestScanBoundCoversOpenPeak: opening a column with a large incompressible trailer dictionary over
// a backend whose reads allocate holds the tail as read, the dictionary decompressed into scratch and
// its kept copy at once. Everything the open allocates, freed or not, stays inside what ScanBound
// reserves for it, which bounds the open's peak from above.
//
//nolint:paralleltest // counts the process-wide allocations
func TestScanBoundCoversOpenPeak(t *testing.T) {
	ctx := context.Background()

	const (
		distinct = 4096
		rows     = 64 << 10
	)

	pool := make([][]byte, distinct)
	for i := range pool {
		pool[i] = make([]byte, 512)
		_, _ = rand.Read(pool[i])
	}

	vals := make([][]byte, rows)
	for i := range vals {
		vals[i] = pool[i%distinct]
	}

	b, err := file.New(t.TempDir())
	require.NoError(t, err)

	w := NewPartWriter(WithGranuleSize(8192), WithCompression(compress.AlgorithmZSTD), WithSizingStats())
	require.NoError(t, w.AddColumn(Column{Name: "v", Kind: KindBytes, Codec: chunk.CodecDict, Block: true, Bytes: vals}))
	require.NoError(t, WritePart(ctx, b, "p", w))

	r, err := OpenPart(ctx, b, "p")
	require.NoError(t, err)

	desc, ok := r.ColumnDescByName("v")
	require.True(t, ok)
	require.True(t, desc.TrailerDict)
	require.Greater(t, desc.DictRaw, int64(1<<20), "the dictionary must be large for the bound to mean anything")

	steady, open, ok := r.ScanBound(ctx, "v", 1<<20)
	require.True(t, ok)

	var scan *Decoder

	allocated := heaptest.Allocated(func() {
		scan, err = r.ColumnScan(ctx, "v", 1<<20)
	})
	require.NoError(t, err)

	t.Logf("dictionary %.1f MiB: open allocated %.1f MiB, bound steady %.1f + open %.1f MiB",
		float64(desc.DictRaw)/(1<<20), float64(allocated)/(1<<20), float64(steady)/(1<<20), float64(open)/(1<<20))

	assert.LessOrEqual(t, int64(allocated), steady+open, "the open outgrew what ScanBound reserves for it")
	assert.LessOrEqual(t, scan.ResidentBytes(), steady)
}
