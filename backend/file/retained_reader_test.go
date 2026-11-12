package file

import (
	"context"
	"strconv"
	"testing"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/internal/vfs/faultfs"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/internal/readerpool"
	"github.com/oteldb/storage/internal/vfs"
)

func TestImmutableReaderScopeReusesOpenedGeneration(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	b, err := New(t.TempDir())
	require.NoError(t, err)
	require.NoError(t, b.Write(ctx, "object", []byte("original")))
	ordinary := b.open
	var roots []*readAtTestFS
	b.open = func() (vfs.FS, error) {
		root, err := ordinary()
		if err != nil {
			return nil, err
		}
		tracked := &readAtTestFS{FS: root}
		roots = append(roots, tracked)
		return tracked, nil
	}
	owner := readerpool.NewScope()
	defer func() { require.NoError(t, owner.Close()) }()
	scoped := owner.Context(ctx)
	owner.Acquire()
	defer owner.Release()
	var dst []byte
	for range 5 {
		dst, err = b.ReadAtAppend(scoped, dst[:0], "object", 1, 3)
		require.NoError(t, err)
		require.Equal(t, "rig", string(dst))
	}
	require.Len(t, roots, 1, "one opened object for all ranges")
	require.Equal(t, 1, roots[0].fileStats)
	require.Equal(t, []string{"root"}, roots[0].closed, "the root is closed immediately, the object remains open")
	require.NoError(t, b.Write(ctx, "object", []byte("replaced")), "replacement must work with an open reader on Windows too")
	dst, err = b.ReadAtAppend(scoped, dst[:0], "object", 0, 8)
	require.NoError(t, err)
	require.Equal(t, "original", string(dst))
	replacement := readerpool.NewScope()
	defer func() { require.NoError(t, replacement.Close()) }()
	got, err := b.ReadAt(replacement.Context(ctx), "object", 0, 8)
	require.NoError(t, err)
	require.Equal(t, "replaced", string(got))
	got, err = b.ReadAt(context.Background(), "object", 0, 8)
	require.NoError(t, err)
	require.Equal(t, "replaced", string(got), "ordinary mutable reads do not retain a generation")
	require.NoError(t, owner.Close())
	require.Equal(t, []string{"root", "file"}, roots[0].closed)
}

//nolint:paralleltest // This test reserves every slot of the process-wide reader budget.
func TestSaturatedRangeFallbackRejectsReplacement(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(strconv.FormatBool(batch), func(t *testing.T) { testSaturatedRangeFallbackRejectsReplacement(t, batch) })
	}
}

func testSaturatedRangeFallbackRejectsReplacement(t *testing.T, batched bool) {
	t.Helper()
	ctx := t.Context()
	if batched {
		base, batch := readerpool.WithBatch(ctx)
		ctx = base
		defer func() { require.NoError(t, batch.Close()) }()
	}
	fsys := faultfs.New()
	b := newFS(fsys)
	fill := readerpool.NewScope()
	held := make([]backend.ObjectReader, 0, 256)
	defer func() {
		for _, r := range held {
			require.NoError(t, r.Close())
		}
		require.NoError(t, fill.Close())
	}()
	for i := range 256 {
		key := "held-" + strconv.Itoa(i)
		require.NoError(t, b.Write(ctx, key, []byte("x")))
		r, supported, err := b.OpenReader(fill.Context(ctx), key)
		require.NoError(t, err)
		require.True(t, supported)
		held = append(held, r)
	}
	require.NoError(t, b.Write(ctx, "object", []byte("old")))
	require.NoError(t, b.Write(ctx, "replacement", []byte("new")))
	target := readerpool.NewScope()
	defer func() { require.NoError(t, target.Close()) }()
	old := target.Context(ctx)
	ordinary := b.open
	replaced := false
	b.open = func() (vfs.FS, error) {
		root, err := ordinary()
		if err != nil {
			return nil, err
		}
		return &readAtTestFS{FS: root, beforeOpen: func() {
			if replaced {
				return
			}
			replaced = true
			finish := readerpool.BeginMutation(b.readerNamespace(), "object")
			require.NoError(t, fsys.Rename("replacement", "object"))
			finish()
		}}, nil
	}
	got, err := b.ReadAtAppend(old, nil, "object", 0, 3)
	require.True(t, replaced)
	require.Error(t, err, "fallback must not return new bytes under old directory metadata")
	require.Empty(t, got)
}

func TestDifferentBackendForSameRootInvalidatesScope(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	root := t.TempDir()
	first, err := New(root)
	require.NoError(t, err)
	second, err := New(root)
	require.NoError(t, err)
	require.Same(t, first.readerNamespace(), second.readerNamespace())
	require.NoError(t, first.Write(ctx, "object", []byte("old")))
	scope := readerpool.NewScope()
	defer func() { require.NoError(t, scope.Close()) }()
	got, err := first.ReadAt(scope.Context(ctx), "object", 0, 3)
	require.NoError(t, err)
	require.Equal(t, "old", string(got))
	require.NoError(t, second.Write(ctx, "object", []byte("new")))
	got, err = first.ReadAt(scope.Context(ctx), "object", 0, 3)
	require.NoError(t, err)
	require.Equal(t, "new", string(got))
}
