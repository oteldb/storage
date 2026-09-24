package backendtest_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
	"github.com/oteldb/storage/internal/partid"
)

func TestWithoutCapabilities(t *testing.T) {
	t.Parallel()

	backendtest.Run(t, func(*testing.T) backend.Backend {
		return backendtest.WithoutCapabilities(backend.Memory())
	})

	b := backendtest.WithoutCapabilities(backend.Memory())
	assertHides[backend.Viewer](t, b)
	assertHides[backend.Sizer](t, b)
	assertHides[backend.ReaderAt](t, b)
	assertHides[backend.ObjectCreator](t, b)
	assertHides[backend.NodeLocal](t, b)
}

func TestStreamingMemory(t *testing.T) {
	t.Parallel()

	backendtest.Run(t, func(*testing.T) backend.Backend { return backendtest.NewStreamingMemory() })

	ctx := context.Background()
	b := backendtest.NewStreamingMemory()
	require.True(t, backend.StreamsWrites(b))
	assertHides[backend.Viewer](t, b)

	w, err := backend.CreateObject(ctx, b, "k")
	require.NoError(t, err)
	_, err = w.Write([]byte("abandoned"))
	require.NoError(t, err)
	w.Abort()

	_, err = b.Read(ctx, "k")
	require.ErrorIs(t, err, backend.ErrNotExist, "an aborted object is never written")
	assert.Equal(t, int64(1), b.Creates())
}

func TestCounting(t *testing.T) {
	t.Parallel()

	backendtest.Run(t, func(*testing.T) backend.Backend { return backendtest.NewCounting(backend.Memory()) })

	ctx := context.Background()
	b := backendtest.NewCounting(backend.Memory())
	assertHides[backend.Viewer](t, b)
	assertHides[backend.Sizer](t, b)

	require.NoError(t, b.Write(ctx, "a", []byte("1")))

	_, err := backend.ReadView(ctx, b, "a")
	require.NoError(t, err)
	_, err = backend.SizeOf(ctx, b, "a")
	require.NoError(t, err)
	assert.Equal(t, map[string]int{"a": 2}, b.Reads(), "a view and a size probe are both whole reads")

	b.FailWriteAt(2)
	require.NoError(t, b.Write(ctx, "b", []byte("2")))
	require.ErrorIs(t, b.Write(ctx, "c", []byte("3")), backendtest.ErrInjected)
	require.NoError(t, b.Write(ctx, "d", []byte("4")))
	assert.Equal(t, 3, b.Writes())
}

func TestByteCounter(t *testing.T) {
	t.Parallel()

	backendtest.Run(t, func(*testing.T) backend.Backend { return backendtest.NewByteCounter(backend.Memory()) })

	ctx := context.Background()
	b := backendtest.NewByteCounter(backend.Memory())
	assertHides[backend.Sizer](t, b)
	assertHides[backend.ObjectCreator](t, b)

	require.NoError(t, b.Write(ctx, "a", []byte("0123456789")))
	require.NoError(t, b.Write(ctx, "b", []byte("xy")))

	_, err := b.Read(ctx, "a")
	require.NoError(t, err)
	_, err = backend.ReadView(ctx, b, "b")
	require.NoError(t, err)
	_, err = backend.ReadAt(ctx, b, "a", 2, 3)
	require.NoError(t, err)
	_, err = backend.SizeOf(ctx, b, "b")
	require.NoError(t, err)

	assert.Equal(t, int64(10+2+3+2), b.Bytes())
	assert.Equal(t, int64(4), b.Reads())
	assert.Equal(t, "\n  a                                                  13"+
		"\n  b                                                   4", b.Report())

	b.Reset()
	assert.Zero(t, b.Bytes())
	assert.Zero(t, b.Reads())
	assert.Empty(t, b.Report())
}

func TestSizedByteCounter(t *testing.T) {
	t.Parallel()

	backendtest.Run(t, func(*testing.T) backend.Backend { return backendtest.NewSizedByteCounter(backend.Memory()) })

	ctx := context.Background()
	b := backendtest.NewSizedByteCounter(backend.Memory())
	require.NoError(t, b.Write(ctx, "a", []byte("0123456789")))

	n, err := backend.SizeOf(ctx, b, "a")
	require.NoError(t, err)
	assert.Equal(t, int64(10), n)
	assert.Zero(t, b.Reads(), "a forwarded size probe reads nothing")
}

func TestPartDirs(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := backend.Memory()
	first, second := partid.New().String(), partid.New().String()

	for _, key := range []string{
		"e/" + second + "/c/0",
		"e/" + first + "/manifest",
		"e/" + first + "/c/0",
		"e/index",
		"e/0000000001/c/0",
		"other/" + partid.New().String() + "/manifest",
	} {
		require.NoError(t, b.Write(ctx, key, []byte("v")))
	}

	assert.Equal(t, []string{first, second}, backendtest.PartDirs(ctx, t, b, "e"))

	assert.True(t, backendtest.IsPartObject("e/"+first+"/c/0"))
	assert.True(t, backendtest.IsPartObject("e/"+first+"/manifest"))
	assert.False(t, backendtest.IsPartObject("e/index"))
	assert.False(t, backendtest.IsPartObject("e/0000000001/c/0"))
}

func TestDigest(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := backend.Memory()

	require.NoError(t, b.Write(ctx, "p1/manifest", []byte("m")))
	require.NoError(t, b.Write(ctx, "p1/c/0", []byte("")))
	require.NoError(t, b.Write(ctx, "p2/c/0", []byte("abc")))
	require.NoError(t, b.Write(ctx, "p3/ignored", []byte("x")))

	assert.Equal(t, ""+
		"c/0                             0 e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n"+
		"c/0                             3 ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad\n"+
		"manifest                        1 62c66a7a5dd70c3146618063c344e531e6d4b59e379808443ce962b3abd63c5a\n",
		backendtest.Digest(t, b, []string{"p1", "p2"}))
}

func TestMatrix(t *testing.T) {
	t.Parallel()

	var dirs []string

	onDisk := backendtest.Dir("disk", func(dir string) (backend.Backend, error) {
		dirs = append(dirs, dir)

		return backend.Memory(), nil
	})
	remote := backendtest.Case{Name: "remote", Open: func(testing.TB) backend.Backend { return backend.Memory() }}

	matrix := backendtest.Matrix(onDisk, remote)
	names := make([]string, 0, len(matrix))

	for _, c := range matrix {
		names = append(names, c.Name)

		b := c.Open(t)
		require.NoError(t, b.Write(context.Background(), "k", []byte("v")))
	}

	assert.Equal(t, []string{"memory", "disk", "cached-disk", "remote", "whole-object"}, names)
	require.Len(t, dirs, 2, "the cached case opens its own directory")
	assert.NotEqual(t, dirs[0], dirs[1])
}

func assertHides[C any](t *testing.T, b backend.Backend) {
	t.Helper()

	_, ok := b.(C)
	assert.Falsef(t, ok, "%T must hide %T", b, (*C)(nil))
}
