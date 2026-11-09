package engine

import (
	"context"
	"io"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	backendfile "github.com/oteldb/storage/backend/file"
)

// cpuFixtureFile keeps every native file capability while counting logical read bytes.
type cpuFixtureFile struct {
	*backendfile.File

	bytes atomic.Int64
}

var (
	_ backend.ObjectCreator  = (*cpuFixtureFile)(nil)
	_ backend.DeferredSyncer = (*cpuFixtureFile)(nil)
	_ backend.ReadAppender   = (*cpuFixtureFile)(nil)
	_ backend.ObjectOpener   = (*cpuFixtureFile)(nil)
)

func (f *cpuFixtureFile) Read(ctx context.Context, key string) ([]byte, error) {
	data, err := f.File.Read(ctx, key)
	f.bytes.Add(int64(len(data)))
	return data, err
}

func (f *cpuFixtureFile) ReadAt(ctx context.Context, key string, off, n int64) ([]byte, error) {
	data, err := f.File.ReadAt(ctx, key, off, n)
	f.bytes.Add(int64(len(data)))
	return data, err
}

func (f *cpuFixtureFile) ReadAtAppend(ctx context.Context, dst []byte, key string, off, n int64) ([]byte, error) {
	data, err := f.File.ReadAtAppend(ctx, dst, key, off, n)
	f.bytes.Add(int64(len(data) - len(dst)))
	return data, err
}

func (f *cpuFixtureFile) OpenReader(ctx context.Context, key string) (backend.ObjectReader, bool, error) {
	r, supported, err := f.File.OpenReader(ctx, key)
	if err != nil || !supported {
		return r, supported, err
	}
	return &cpuFixtureReader{ObjectReader: r, bytes: &f.bytes}, true, nil
}

func (f *cpuFixtureFile) Bytes() int64 { return f.bytes.Load() }
func (f *cpuFixtureFile) Reset()       { f.bytes.Store(0) }

type cpuFixtureReader struct {
	backend.ObjectReader

	bytes *atomic.Int64
}

func (r *cpuFixtureReader) ReadAt(dst []byte, off int64) (int, error) {
	n, err := r.ObjectReader.ReadAt(dst, off)
	r.bytes.Add(int64(n))
	return n, err
}

func (r *cpuFixtureReader) Read(dst []byte) (int, error) {
	n, err := r.ObjectReader.Read(dst)
	r.bytes.Add(int64(n))
	return n, err
}

func TestCPUFixtureNativeCapabilities(t *testing.T) {
	t.Parallel()
	files, err := backendfile.New(t.TempDir())
	require.NoError(t, err)
	f := &cpuFixtureFile{File: files}
	require.True(t, f.StreamsWrites())
	require.True(t, f.RetainsImmutableReaders())
	require.NoError(t, f.Write(t.Context(), "object", []byte("abcdef")))
	dst := make([]byte, 2, 8)
	copy(dst, "xy")
	got, err := f.ReadAtAppend(t.Context(), dst, "object", 1, 3)
	require.NoError(t, err)
	require.Equal(t, "xybcd", string(got))
	require.EqualValues(t, 3, f.Bytes())
	reader, supported, err := f.OpenReader(t.Context(), "object")
	require.NoError(t, err)
	require.True(t, supported)
	n, err := reader.ReadAt(dst[:cap(dst)], 0)
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, 6, n)
	require.EqualValues(t, 9, f.Bytes())
	require.NoError(t, reader.Close())
	f.Reset()
	require.Zero(t, f.Bytes())
}
