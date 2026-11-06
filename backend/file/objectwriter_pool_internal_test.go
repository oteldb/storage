package file

import (
	"io/fs"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/internal/vfs"
	"github.com/oteldb/storage/internal/vfs/faultfs"
)

type writerCloseFS struct {
	vfs.FS

	fileClose, rootClose bool
}

func (f *writerCloseFS) OpenFile(name string, flags int, mode fs.FileMode) (vfs.File, error) {
	opened, err := f.FS.OpenFile(name, flags, mode)
	if err != nil {
		return nil, err
	}
	return &writerCloseFile{File: opened, owner: f}, nil
}

func (f *writerCloseFS) Close() error {
	err := f.FS.Close()
	if f.rootClose {
		return assert.AnError
	}
	return err
}

type writerCloseFile struct {
	vfs.File

	owner *writerCloseFS
}

func (f *writerCloseFile) Close() error {
	err := f.File.Close()
	if f.owner.fileClose {
		return assert.AnError
	}
	return err
}

func TestObjectWriterBufferTerminalPaths(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name      string
		operation faultfs.Op
		abort     bool
		fail      bool
		published bool
		fileClose bool
		rootClose bool
	}{
		{name: "commit", published: true},
		{name: "abort", abort: true},
		{name: "flush", operation: faultfs.OpWrite, fail: true},
		{name: "sync", operation: faultfs.OpSync, fail: true},
		{name: "rename", operation: faultfs.OpRename, fail: true},
		{name: "directory sync", operation: faultfs.OpSyncDir, fail: true, published: true},
		{name: "file close", fileClose: true},
		{name: "root close", rootClose: true, published: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fsys := faultfs.New()
			wrapped := &writerCloseFS{FS: fsys}
			files := newFS(wrapped)
			files.open = func() (vfs.FS, error) { return wrapped, nil }
			writer, err := files.CreateObject(t.Context(), "part/object")
			require.NoError(t, err)
			w := writer.(*objectWriter)
			require.Equal(t, objectWriteBufferBytes, w.buf.Size())
			_, err = writer.Write([]byte("payload"))
			require.NoError(t, err)
			if tt.fail {
				fsys.Add(faultfs.Rule{Op: tt.operation, Err: assert.AnError, Times: 1})
			}
			wrapped.fileClose, wrapped.rootClose = tt.fileClose, tt.rootClose
			switch {
			case tt.abort:
				writer.Abort()
				require.Zero(t, countOps(fsys.Calls(), faultfs.OpWrite), "abort must not flush buffered data")
			case tt.fail || tt.fileClose || tt.rootClose:
				require.ErrorIs(t, writer.Commit(t.Context()), assert.AnError)
			default:
				require.NoError(t, writer.Commit(t.Context()))
			}
			writer.Abort()
			writer.Abort()
			require.Nil(t, w.buf)
			require.Nil(t, w.tmp)
			wrapped.fileClose, wrapped.rootClose = false, false
			names, err := fsys.ReadDir("part")
			require.NoError(t, err)
			for _, name := range names {
				require.False(t, strings.HasPrefix(name.Name(), ".tmp-"))
			}
			_, err = writer.Write([]byte("after close"))
			require.Error(t, err)
			require.Error(t, writer.Commit(t.Context()))
			data, err := files.Read(t.Context(), "part/object")
			if tt.published {
				require.NoError(t, err)
				require.Equal(t, []byte("payload"), data)
			} else {
				require.ErrorIs(t, err, backend.ErrNotExist)
			}
			next, err := files.CreateObject(t.Context(), "part/next")
			require.NoError(t, err)
			defer next.Abort()
			require.Zero(t, next.(*objectWriter).buf.Buffered())
			_, err = next.Write([]byte("next"))
			require.NoError(t, err)
			require.NoError(t, next.Commit(t.Context()))
			require.Equal(t, []byte("next"), read(t, files, "part/next"))
		})
	}
}

func BenchmarkObjectWriterBuffer(b *testing.B) {
	for _, commit := range []bool{true, false} {
		name := "abort"
		if commit {
			name = "commit"
		}
		b.Run(name, func(b *testing.B) {
			files, err := New(b.TempDir())
			if err != nil {
				b.Fatal(err)
			}
			payload := make([]byte, 1024)
			b.SetBytes(int64(len(payload)))
			b.ReportAllocs()
			for b.Loop() {
				writer, err := files.CreateObjectDeferred(b.Context(), "part/object")
				if err != nil {
					b.Fatal(err)
				}
				if _, err := writer.Write(payload); err != nil {
					writer.Abort()
					b.Fatal(err)
				}
				if commit {
					if err := writer.Commit(b.Context()); err != nil {
						writer.Abort()
						b.Fatal(err)
					}
				} else {
					writer.Abort()
				}
			}
		})
	}
}
