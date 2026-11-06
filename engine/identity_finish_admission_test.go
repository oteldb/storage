package engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/wal"
)

func TestIdentityFinishAdmissionFailurePreservesWALAndHead(t *testing.T) {
	t.Parallel()
	files, err := file.New(t.TempDir())
	require.NoError(t, err)
	b := &identityWriterBackend{Backend: files, ObjectCreator: files, DeferredSyncer: files,
		known: true, streaming: true, workspace: 128 << 20}
	dir := t.TempDir()
	w, err := wal.Create(dir, 0)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, w.Close()) })
	held, large := 0, 0
	refuse := true
	e := New(Config{Backend: b, WAL: w, Prefix: "finish-admission", FlushMemoryBytes: 16 << 20,
		FlushAdmission: func(_ context.Context, bytes int64) (func(), error) {
			if refuse && bytes >= 128<<20 {
				large++
				return nil, assert.AnError
			}
			held++
			return func() { held-- }, nil
		}})
	s := orderSeries(1)
	_, err = e.Append(s, 100, 42)
	require.NoError(t, err)
	require.ErrorIs(t, e.Flush(t.Context()), assert.AnError)
	require.Equal(t, 1, large, "the additional identity writer workspace was declined")
	require.Zero(t, held, "every existing grant was released")
	require.Zero(t, b.create, "no identity object opens before admission succeeds")
	require.Zero(t, e.PartCount())
	require.Equal(t, 1, e.HeadSampleCount())
	require.NoError(t, w.Sync())
	recovered := New(Config{})
	require.NoError(t, recovered.Replay(t.Context(), dir))
	require.Equal(t, 1, recovered.HeadSampleCount())
	require.Equal(t, []float64{42}, recovered.head.samples[s.Hash()].values)
	refuse = false
	b.workspace = 64 << 10
	require.NoError(t, e.Flush(t.Context()))
	require.Zero(t, held)
	require.Zero(t, e.HeadSampleCount())
	require.Equal(t, 1, e.PartCount())
	require.Equal(t, 1, b.create)
	var _ backend.Backend = b
}
