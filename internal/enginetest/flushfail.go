package enginetest

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/faultbackend"
	"github.com/oteldb/storage/wal"
)

// flushFailureKeepsRows: a flush that fails before publishing a part folds the detached head buffers
// back, so the rows are retried by the next flush instead of being stranded in the in-flight buffer
// (which the next flush overwrites — silent, permanent loss).
func flushFailureKeepsRows(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	be := faultbackend.Wrap(backend.Memory())
	e := k.open(t, be)

	e.Append(t, api(100, 1), api(200, 2))

	rejectWrites(be, "", errWriteRejected)
	require.Error(t, e.Flush(ctx), "flush must fail while the backend rejects writes")
	be.Reset()

	assert.Equal(t, []Row{api(100, 1), api(200, 2)}, rows(t, e, apiStream), "readable after the failed flush")
	require.Positive(t, e.HeadBytes(), "the folded-back rows are accounted as head bytes again")

	e.Append(t, api(300, 3))
	require.NoError(t, e.Flush(ctx))

	assert.Equal(t, []Row{api(100, 1), api(200, 2), api(300, 3)}, rows(t, e, apiStream),
		"the retried flush persists both the folded-back rows and the ones appended after it")
	assert.Equal(t, 1, e.PartCount())
}

// createWAL opens a segment WAL in dir, closed at cleanup unless the test closes it first.
func createWAL(t *testing.T, dir string) *wal.SegmentWriter {
	t.Helper()

	w, err := wal.Create(dir, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	return w
}

// flushFailureKeepsRowsAcrossRestart is [flushFailureKeepsRows] with durability: because the failed
// flush's rows stay in the head, the next flush persists them and its WAL checkpoint only discards
// segments the committed part supersedes.
func flushFailureKeepsRowsAcrossRestart(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	be := faultbackend.Wrap(backend.Memory())
	dir := t.TempDir()

	w := createWAL(t, dir)
	e := k.Open(t, Config{Backend: be, WAL: w})

	e.Append(t, api(100, 1), api(200, 2))
	require.NoError(t, w.Sync())

	rejectWrites(be, "", errWriteRejected)
	require.Error(t, e.Flush(ctx))
	be.Reset()

	e.Append(t, api(300, 3))
	require.NoError(t, w.Sync())
	require.NoError(t, e.Flush(ctx))
	require.NoError(t, w.Close())

	// Restart: recover the watermark from the bucket index, then replay whatever WAL is left.
	restored := k.Open(t, Config{Backend: be, WAL: createWAL(t, dir)})
	require.NoError(t, restored.LoadParts(ctx))
	require.NoError(t, restored.Replay(t.Context(), dir))

	assert.Equal(t, []Row{api(100, 1), api(200, 2), api(300, 3)}, rows(t, restored, apiStream),
		"rows logged to the WAL must survive a restart after a failed flush")
}

// flushFailureMergesConcurrentAppends covers the fold-back's merge arm: a row appended while the
// flush was writing its part lands in the fresh buffer, so the failed flush must merge the detached
// rows with it rather than replace either side. The interleaving is stated with a gate.
func flushFailureMergesConcurrentAppends(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	be := faultbackend.Wrap(backend.Memory())
	e := k.open(t, be)

	e.Append(t, api(100, 1))

	gate := faultbackend.NewGate()
	rule := gate.Rule(faultbackend.Write, func(op faultbackend.Op) bool {
		return strings.HasPrefix(op.Key, k.Prefix+"/")
	})
	rule.Err = errWriteRejected
	be.Add(rule)

	var (
		wg       sync.WaitGroup
		flushErr error
	)

	wg.Go(func() { flushErr = e.Flush(ctx) })

	gate.Await(t) // the head is detached and the part write is about to fail
	e.Append(t, api(200, 2))

	gate.Release()
	wg.Wait()
	require.Error(t, flushErr)
	be.Reset()

	assert.Equal(t, []Row{api(100, 1), api(200, 2)}, rows(t, e, apiStream),
		"the detached row and the one appended during the flush are both live")

	require.NoError(t, e.Flush(ctx))
	assert.Equal(t, []Row{api(100, 1), api(200, 2)}, rows(t, e, apiStream), "and both reach the retried part")
}
