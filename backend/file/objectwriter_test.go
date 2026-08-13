package file_test

import (
	"context"
	"os"
	"path/filepath"
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

	assert.GreaterOrEqual(t, tempBytes(t, filepath.Join(dir, "part", "c")), int64(len(chunk))/2,
		"the bytes written so far must be on disk, not held in the writer")

	_, err = b.Read(ctx, "part/c/0")
	require.ErrorIs(t, err, backend.ErrNotExist, "the object is not published until commit")

	require.NoError(t, w.Commit(ctx))

	got, err := b.Read(ctx, "part/c/0")
	require.NoError(t, err)
	assert.Equal(t, chunk, got)

	assert.Zero(t, tempBytes(t, filepath.Join(dir, "part", "c")), "the temp file is renamed, not left")
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

	assert.Zero(t, tempBytes(t, filepath.Join(dir, "part", "c")))
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

// tempBytes totals the bytes held by the temp files in dir (0 if dir does not exist).
func tempBytes(t *testing.T, dir string) int64 {
	t.Helper()

	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return 0
	}

	require.NoError(t, err)

	var total int64

	for _, e := range entries {
		if !strings.HasPrefix(e.Name(), ".tmp-") {
			continue
		}

		info, err := e.Info()
		require.NoError(t, err)

		total += info.Size()
	}

	return total
}
