package file_test

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/file"
)

// TestCreateObjectReachesDiskBeforeCommit is the property the whole seam exists for: bytes handed to
// the writer must land in the filesystem as they arrive, not accumulate in RAM until commit. It is
// checked through the temp file the writer publishes with a rename.
func TestCreateObjectReachesDiskBeforeCommit(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	b, err := file.New(dir)
	require.NoError(t, err)

	ctx := context.Background()

	w, err := b.CreateObject(ctx, "part/c/0")
	require.NoError(t, err)
	defer w.Abort()

	// Larger than the writer's userspace buffer, so a flush is forced without asking for one.
	chunk := make([]byte, 256<<10)
	for i := range chunk {
		chunk[i] = byte(i)
	}

	_, err = w.Write(chunk)
	require.NoError(t, err)

	// The writer is file-backed, so the bytes it has accepted are in the filesystem rather than in
	// the writer. How much of that is observable differs by platform: NTFS reports a stale size for a
	// file with an open handle, so only the temp file's existence is portable and the byte count is
	// checked where it means something.
	require.NotEmpty(t, tempFiles(t, filepath.Join(dir, "part", "c")), "the object is built in a temp file")

	if runtime.GOOS != "windows" {
		assert.GreaterOrEqual(t, tempBytes(t, filepath.Join(dir, "part", "c")), int64(len(chunk))/2,
			"the bytes written so far must be on disk, not held in the writer")
	}

	_, err = b.Read(ctx, "part/c/0")
	require.ErrorIs(t, err, backend.ErrNotExist, "the object is not published until commit")

	require.NoError(t, w.Commit(ctx))

	got, err := b.Read(ctx, "part/c/0")
	require.NoError(t, err)
	assert.Equal(t, chunk, got)

	assert.Empty(t, tempFiles(t, filepath.Join(dir, "part", "c")), "the temp file is renamed, not left")
}

// TestCreateObjectAbortRemovesTemp guards the merge-abandoned path: a temp file that survives an
// abort is a leak the file backend has no other collector for.
func TestCreateObjectAbortRemovesTemp(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	b, err := file.New(dir)
	require.NoError(t, err)

	w, err := b.CreateObject(context.Background(), "part/c/0")
	require.NoError(t, err)

	_, err = w.Write(make([]byte, 128<<10))
	require.NoError(t, err)

	w.Abort()
	w.Abort() // idempotent

	assert.Empty(t, tempFiles(t, filepath.Join(dir, "part", "c")))
}

// TestCreateObjectRejectsEscapingKey keeps the incremental path under the same root check as
// [file.File.Write]: a key that escapes the root must not open a writer at all.
func TestCreateObjectRejectsEscapingKey(t *testing.T) {
	t.Parallel()

	b, err := file.New(t.TempDir())
	require.NoError(t, err)

	_, err = b.CreateObject(context.Background(), "../escape")
	require.Error(t, err)
}

// tempFiles returns the writer temp files present in dir (none if dir does not exist).
func tempFiles(t *testing.T, dir string) []os.DirEntry {
	t.Helper()

	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil
	}

	require.NoError(t, err)

	var out []os.DirEntry

	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".tmp-") {
			out = append(out, e)
		}
	}

	return out
}

// tempBytes totals the bytes those temp files hold. Meaningful only where the filesystem keeps an
// open file's reported size current — see [TestCreateObjectReachesDiskBeforeCommit].
func tempBytes(t *testing.T, dir string) int64 {
	t.Helper()

	var total int64

	for _, e := range tempFiles(t, dir) {
		info, err := e.Info()
		require.NoError(t, err)

		total += info.Size()
	}

	return total
}
