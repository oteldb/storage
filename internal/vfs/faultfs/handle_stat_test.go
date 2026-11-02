package faultfs_test

import (
	"io/fs"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/internal/vfs/faultfs"
)

func TestHandleStatFault(t *testing.T) {
	t.Parallel()
	f := faultfs.New()
	file, err := f.OpenFile("object", os.O_CREATE|os.O_RDWR, 0o600)
	require.NoError(t, err)
	defer func() { _ = file.Close() }()
	_, err = file.Write([]byte("data"))
	require.NoError(t, err)
	f.Add(faultfs.Rule{Op: faultfs.OpStat, Err: fs.ErrPermission, Times: 1})
	_, err = file.Stat()
	require.ErrorIs(t, err, fs.ErrPermission)
	info, err := file.Stat()
	require.NoError(t, err)
	require.EqualValues(t, 4, info.Size())
}
