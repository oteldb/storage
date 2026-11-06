package block

import (
	"context"
	"io"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
)

type trackedReaderBackend struct {
	backend.Backend

	opened, closed, reads int
	first                 *byte
	grew                  bool
	short                 bool
	unpriced              bool
}

func (b *trackedReaderBackend) ReaderMemory(key string) (int64, bool) {
	if b.unpriced {
		return 0, false
	}
	return backend.ReaderMemory(b.Backend, key)
}

func TestOpenColumnScanUnpricedReaderFallsBack(t *testing.T) {
	t.Parallel()
	r, b, values := fileScanFixture(t)
	b.unpriced = true
	d, err := r.OpenColumnScan(t.Context(), "values", 4096)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, d.Close()) })
	got, err := d.DecodeInt64(0)
	require.NoError(t, err)
	require.Equal(t, values[:len(got)], got)
	require.Zero(t, b.opened, "unknown retained state must not bypass admission sizing")
}

func (b *trackedReaderBackend) ReadAt(ctx context.Context, key string, off, n int64) ([]byte, error) {
	return backend.ReadAt(ctx, b.Backend, key, off, n)
}

func (b *trackedReaderBackend) Size(ctx context.Context, key string) (int64, error) {
	return backend.SizeOf(ctx, b.Backend, key)
}

func (b *trackedReaderBackend) OpenReader(ctx context.Context, key string) (backend.ObjectReader, bool, error) {
	r, ok, err := backend.OpenReader(ctx, b.Backend, key)
	if err != nil || !ok {
		return r, ok, err
	}
	b.opened++
	return &trackedObjectReader{ObjectReader: r, owner: b}, true, nil
}

type trackedObjectReader struct {
	backend.ObjectReader

	owner  *trackedReaderBackend
	closed bool
}

func (r *trackedObjectReader) ReadAt(dst []byte, off int64) (int, error) {
	r.owner.reads++
	if len(dst) > 0 {
		if r.owner.first == nil {
			r.owner.first = &dst[0]
		} else if r.owner.first != &dst[0] {
			r.owner.grew = true
		}
	}
	if r.owner.short {
		return 0, io.EOF
	}
	return r.ObjectReader.ReadAt(dst, off)
}

func (r *trackedObjectReader) Close() error {
	if !r.closed {
		r.owner.closed++
		r.closed = true
	}
	return r.ObjectReader.Close()
}

func fileScanFixture(t *testing.T) (*PartReader, *trackedReaderBackend, []int64) {
	t.Helper()
	f, err := file.New(t.TempDir())
	require.NoError(t, err)
	values := make([]int64, 32<<10)
	rng := rand.New(rand.NewPCG(121, 127))
	for i := range values {
		values[i] = int64(rng.Uint64())
	}
	w := NewPartWriter(WithGranuleSize(128), WithCompressBlockBytes(1024),
		WithCompression(compress.AlgorithmZSTD), WithSizingStats())
	require.NoError(t, w.AddColumn(Column{Name: "values", Kind: KindInt64, Codec: chunk.CodecT64, Int64: values, Block: true}))
	require.NoError(t, WritePart(t.Context(), f, "part", w))
	b := &trackedReaderBackend{Backend: backend.Cached(f, 1<<20)}
	r, err := OpenPart(t.Context(), b, "part")
	require.NoError(t, err)
	return r, b, values
}

func TestOpenColumnScanReusesWindowAndCloses(t *testing.T) {
	t.Parallel()
	r, b, values := fileScanFixture(t)
	d, err := r.OpenColumnScan(t.Context(), "values", 4096)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, d.Close()) })
	steady, _, ok := r.ScanBound(t.Context(), "values", 4096)
	require.True(t, ok)
	require.LessOrEqual(t, d.ResidentBytes(), steady)
	var scratch []int64
	for i := range d.NumBlocks() {
		scratch, err = d.DecodeInt64Into(i, scratch[:0])
		require.NoError(t, err)
		lo, hi := d.BlockSpan(i)
		require.Equal(t, values[lo:hi], scratch)
	}
	require.Equal(t, 1, b.opened)
	require.Greater(t, b.reads, 2)
	require.False(t, b.grew, "one bounded window is reused across refills")
	first, err := d.DecodeInt64Into(0, scratch[:0])
	require.NoError(t, err)
	require.Equal(t, values[:len(first)], first, "a backwards seek refills from the same handle")
	require.NoError(t, d.Close())
	require.NoError(t, d.Close())
	require.Equal(t, 1, b.closed)
	_, err = d.DecodeInt64(1)
	require.Error(t, err)
}

func TestOpenColumnScanReadFailureCloses(t *testing.T) {
	t.Parallel()
	r, b, _ := fileScanFixture(t)
	d, err := r.OpenColumnScan(t.Context(), "values", 4096)
	require.NoError(t, err)
	b.short = true
	_, err = d.DecodeInt64(0)
	require.ErrorIs(t, err, ErrCorrupt)
	require.NoError(t, d.Close())
	require.Equal(t, b.opened, b.closed)
}

func TestOpenColumnScanLegacyMissingSize(t *testing.T) {
	t.Parallel()
	r, b, values := fileScanFixture(t)
	r.manifest.Columns[r.byName["values"]].Bytes = 0
	d, err := r.OpenColumnScan(t.Context(), "values", 4096)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, d.Close()) })
	got, err := d.DecodeInt64(0)
	require.NoError(t, err)
	require.Equal(t, values[:len(got)], got)
	require.Equal(t, 1, b.opened)
}

func TestRangeBufferCapacity(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct{ n, limit int64 }{
		{0, 0}, {1, 1}, {1000, 1024}, {1000, 1000}, {8193, 16384},
		{8193, 8193}, {1 << 20, 1 << 20}, {5 << 20, 5 << 20},
	} {
		buf := getRangeBuffer(tt.n, tt.limit)
		require.GreaterOrEqual(t, int64(cap(buf)), tt.n)
		require.LessOrEqual(t, int64(cap(buf)), tt.limit)
		putRangeBuffer(buf)
	}
}
