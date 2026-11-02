package file

import (
	"io"
	"io/fs"
	"os"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/internal/vfs"
	"github.com/oteldb/storage/internal/vfs/faultfs"
)

type readAtTestFS struct {
	vfs.FS

	beforeOpen, afterOpen       func()
	openErr, statErr, readErr   error
	pathStats, fileStats, reads int
	closed                      []string
}

func (f *readAtTestFS) OpenFile(name string, flags int, mode fs.FileMode) (vfs.File, error) {
	if f.beforeOpen != nil {
		f.beforeOpen()
	}
	if f.openErr != nil {
		return nil, f.openErr
	}
	opened, err := f.FS.OpenFile(name, flags, mode)
	if err != nil {
		return nil, err
	}
	if f.afterOpen != nil {
		f.afterOpen()
	}
	return &readAtTestFile{File: opened, owner: f}, nil
}
func (f *readAtTestFS) Stat(name string) (fs.FileInfo, error) { f.pathStats++; return f.FS.Stat(name) }
func (f *readAtTestFS) Close() error                          { f.closed = append(f.closed, "root"); return f.FS.Close() }

type readAtTestFile struct {
	vfs.File

	owner *readAtTestFS
}

func (f *readAtTestFile) Stat() (fs.FileInfo, error) {
	f.owner.fileStats++
	if f.owner.statErr != nil {
		return nil, f.owner.statErr
	}
	return f.File.Stat()
}
func (f *readAtTestFile) ReadAt(p []byte, off int64) (int, error) {
	f.owner.reads++
	if f.owner.readErr != nil {
		return 0, f.owner.readErr
	}
	return f.File.ReadAt(p, off)
}
func (f *readAtTestFile) Close() error {
	f.owner.closed = append(f.owner.closed, "file")
	return f.File.Close()
}

func writeReadAtFixture(t *testing.T, fsys vfs.FS, name, value string) {
	t.Helper()
	file, err := fsys.OpenFile(name, os.O_CREATE|os.O_WRONLY, 0o600)
	require.NoError(t, err)
	_, err = file.Write([]byte(value))
	require.NoError(t, err)
	require.NoError(t, file.Close())
}

func TestReadAtReplacementUsesOneGeneration(t *testing.T) {
	t.Parallel()
	for _, system := range []struct {
		name string
		new  func(*testing.T) vfs.FS
	}{
		{"os", func(t *testing.T) vfs.FS {
			t.Helper()
			fsys, err := vfs.OpenRoot(t.TempDir(), 0o750)
			require.NoError(t, err)
			t.Cleanup(func() { _ = fsys.Close() })
			return fsys
		}},
		{"faultfs", func(t *testing.T) vfs.FS { t.Helper(); return faultfs.New() }},
	} {
		t.Run(system.name, func(t *testing.T) {
			t.Parallel()
			for _, tc := range []struct {
				name, old, replacement string
				off                    int64
				afterOpen              bool
				want                   string
			}{
				{"grow before open", "old", "replacement", 0, false, "replacement"},
				{"shrink before open", "original", "new", 0, false, "new"},
				{"shrink to EOF before open", "original", "new", 4, false, ""},
				{"grow after open", "old", "replacement", 0, true, "old"},
				{"shrink after open", "original", "new", 0, true, "original"},
			} {
				t.Run(tc.name, func(t *testing.T) {
					t.Parallel()
					fsys := system.new(t)
					writeReadAtFixture(t, fsys, "object", tc.old)
					writeReadAtFixture(t, fsys, "replacement", tc.replacement)
					root := &readAtTestFS{FS: fsys}
					replace := func() { require.NoError(t, fsys.Rename("replacement", "object")) }
					if tc.afterOpen {
						root.afterOpen = replace
					} else {
						root.beforeOpen = replace
					}
					b := &File{open: func() (vfs.FS, error) { return root, nil }}
					got, err := b.ReadAt(t.Context(), "object", tc.off, 128)
					require.NoError(t, err)
					require.Equal(t, tc.want, string(got))
					require.Zero(t, root.pathStats, "no pathname stat can race with the open")
					require.Equal(t, 1, root.fileStats)
					require.Equal(t, []string{"file", "root"}, root.closed)
				})
			}
		})
	}
}

func TestReadAtHandleErrorsAndClamping(t *testing.T) {
	t.Parallel()
	failure := errors.New("injected read failure")
	for _, tc := range []struct {
		name                               string
		off, length                        int64
		openErr, statErr, readErr, wantErr error
		want                               string
		reads                              int
	}{
		{name: "middle", off: 2, length: 3, want: "234", reads: 1},
		{name: "clamped", off: 8, length: 128, want: "89", reads: 1},
		{name: "zero length", off: 2, length: 0, reads: 1},
		{name: "at EOF", off: 10, length: 8},
		{name: "past EOF", off: 11, length: 8},
		{name: "missing", length: 8, openErr: fs.ErrNotExist, wantErr: backend.ErrNotExist},
		{name: "open error", length: 8, openErr: fs.ErrPermission, wantErr: fs.ErrPermission},
		{name: "stat error", length: 8, statErr: fs.ErrPermission, wantErr: fs.ErrPermission},
		{name: "stat missing", length: 8, statErr: fs.ErrNotExist, wantErr: backend.ErrNotExist},
		{name: "read error", length: 8, readErr: failure, wantErr: failure, reads: 1},
		{name: "short read", length: 8, readErr: io.EOF, wantErr: io.EOF, reads: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fsys := faultfs.New()
			writeReadAtFixture(t, fsys, "object", "0123456789")
			root := &readAtTestFS{FS: fsys, openErr: tc.openErr, statErr: tc.statErr, readErr: tc.readErr}
			b := &File{open: func() (vfs.FS, error) { return root, nil }}
			got, err := b.ReadAt(t.Context(), "object", tc.off, tc.length)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.want, string(got))
			}
			require.Equal(t, tc.reads, root.reads)
			require.Zero(t, root.pathStats)
			if tc.openErr != nil {
				require.Equal(t, []string{"root"}, root.closed)
			} else {
				require.Equal(t, []string{"file", "root"}, root.closed)
			}
		})
	}
}
