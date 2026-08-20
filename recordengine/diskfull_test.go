package recordengine_test

import (
	"context"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
	"github.com/oteldb/storage/backend/faultbackend"
	"github.com/oteldb/storage/recordengine"
)

// TestFullDiskRefusesWrites is #388 for the record engines: the node used to keep accepting records
// it could not store, flushing and failing on every cycle while the head grew behind it.
func TestFullDiskRefusesWrites(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	disk := backendtest.WithCapacity(backend.Memory(), 0, 1<<20)
	e := recordengine.New(recordengine.Config{
		Schema: testSchema, Backend: disk, Prefix: "t/recs", MinFreeBytes: 1 << 20, MinFreeInodes: 16,
	})

	ingest(t, e, mkBatch("api", rrec{ts: 100, body: "one"}))

	require.ErrorIs(t, e.Flush(ctx), backend.ErrNoSpace)
	assert.Zero(t, e.PartCount(), "nothing may be written on a disk that cannot hold it")
	assert.True(t, e.Stats().OutOfSpace)

	_, err := e.AppendBatch(mkBatch("api", rrec{ts: 200, body: "two"}), recordengine.AppendLimits{})
	require.ErrorIs(t, err, backend.ErrNoSpace, "a record that cannot be stored must be rejected, not acked")

	disk.SetFreeBytes(1 << 30)
	require.NoError(t, e.Flush(ctx))
	assert.Equal(t, 1, e.PartCount())
	assert.False(t, e.Stats().OutOfSpace)
	ingest(t, e, mkBatch("api", rrec{ts: 300, body: "three"}))
}

// TestExhaustedInodesRefuseWrites pins the inode axis: a record part is one object per column plus
// its blooms and footer, so the object count is what a wide schema spends first.
func TestExhaustedInodesRefuseWrites(t *testing.T) {
	t.Parallel()

	disk := backendtest.WithCapacity(backend.Memory(), 1<<30, 0)
	e := recordengine.New(recordengine.Config{
		Schema: testSchema, Backend: disk, Prefix: "t/recs", MinFreeBytes: 1 << 20, MinFreeInodes: 16,
	})

	ingest(t, e, mkBatch("api", rrec{ts: 100, body: "one"}))
	require.ErrorIs(t, e.Flush(context.Background()), backend.ErrNoSpace)

	_, err := e.AppendBatch(mkBatch("api", rrec{ts: 200, body: "two"}), recordengine.AppendLimits{})
	require.ErrorIs(t, err, backend.ErrNoSpace)
}

// TestENOSPCFromTheBackendLatches covers the disk filling between the pre-flight check and the
// write. The records fold back into the head (as any failed flush does), and ingest closes.
func TestENOSPCFromTheBackendLatches(t *testing.T) {
	t.Parallel()

	be := faultbackend.Wrap(backend.Memory()).Add(faultbackend.Rule{Kind: faultbackend.Write, Err: syscall.ENOSPC})
	e := newEngine(t, be)

	ingest(t, e, mkBatch("api", rrec{ts: 100, body: "one"}))

	require.ErrorIs(t, e.Flush(context.Background()), syscall.ENOSPC)
	assert.True(t, e.Stats().OutOfSpace)
	assert.Positive(t, e.Stats().HeadBytes, "the records fold back into the head")

	_, err := e.AppendBatch(mkBatch("api", rrec{ts: 200, body: "two"}), recordengine.AppendLimits{})
	require.ErrorIs(t, err, backend.ErrNoSpace)
}
