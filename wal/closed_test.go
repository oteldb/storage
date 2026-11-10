package wal

import (
	"bytes"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/internal/vfs/faultfs"
	"github.com/oteldb/storage/signal"
)

func TestClosedWriterRejectsAppend(t *testing.T) {
	t.Parallel()
	for _, opened := range []bool{false, true} {
		name := "empty"
		if opened {
			name = "opened"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			fsys := faultfs.New()
			w, err := CreateFS(fsys, 0)
			require.NoError(t, err)
			var framed bytes.Buffer
			require.NoError(t, NewWriter(&framed).WriteSamples(signal.SeriesID{}, []int64{1}, []float64{2}))
			if opened {
				require.NoError(t, w.WriteFrames(framed.Bytes()))
			}
			require.NoError(t, w.Close())
			require.ErrorIs(t, w.WriteFrames(framed.Bytes()), os.ErrClosed)
			_, err = w.Write(framed.Bytes())
			require.ErrorIs(t, err, os.ErrClosed)
			require.NoError(t, w.Close())
		})
	}
}
