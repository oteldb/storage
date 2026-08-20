package engine_test

import (
	"context"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
	"github.com/oteldb/storage/backend/faultbackend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/internal/diskguard"
	"github.com/oteldb/storage/signal"
)

// diskFullEngine is an engine over a backend whose capacity the test dictates. The reserve is
// explicit so the assertions do not depend on the defaults moving.
func diskFullEngine(freeBytes, freeInodes int64) (*engine.Engine, *backendtest.Capacity) {
	disk := backendtest.WithCapacity(backend.Memory(), freeBytes, freeInodes)
	e := engine.New(engine.Config{
		Backend:       disk,
		Prefix:        "default/metrics",
		MinFreeBytes:  1 << 20,
		MinFreeInodes: 16,
	})

	return e, disk
}

// TestFullDiskRefusesWrites is the reproducer for #388: a node whose disk cannot take another part
// used to accept every write anyway — the flush failed, the sequence burned, and the head grew
// behind it until the process died with everything unflushed in it.
//
// The disk is injected rather than real: an ENOSPC from a loopback filesystem needs privileges and
// only exists on Linux, while the property under test — what the *engine* does when the medium says
// no — is the same either way, and stating it with an injected report keeps the test hermetic.
func TestFullDiskRefusesWrites(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e, disk := diskFullEngine(0, 1<<20)
	api := mkSeries("job", "api")
	mustAppend(t, e, api, 100, 1.0)

	err := e.Flush(ctx)
	require.ErrorIs(t, err, backend.ErrNoSpace, "a flush with no room must refuse before it writes")
	assert.Zero(t, e.PartCount(), "nothing may be written on a disk that cannot hold it")
	assert.Positive(t, e.HeadBytes(), "the samples stay in the head, where a later flush can still take them")
	assert.True(t, e.Stats().OutOfSpace, "the state an operator sees")

	_, err = e.Append(api, 200, 2.0)
	require.ErrorIs(t, err, backend.ErrNoSpace, "a write that cannot be stored must be rejected, not acked")

	_, err = e.AppendBatch([]signal.SeriesID{{}}, []int64{300}, []float64{3}, nil,
		func(int) signal.Series { return api }, engine.AppendLimits{})
	require.ErrorIs(t, err, backend.ErrNoSpace, "the batch path rejects for the same reason")

	// Recovery is the other half: the node must not stay read-only once the disk is emptied.
	disk.SetFreeBytes(1 << 30)
	require.NoError(t, e.Flush(ctx))
	assert.Equal(t, 1, e.PartCount())
	assert.False(t, e.Stats().OutOfSpace)
	mustAppend(t, e, api, 400, 4.0)
}

// TestExhaustedInodesRefuseWrites pins the second axis: a part is many small objects, so an inode
// table can exhaust with the disk almost empty, and a byte-only check sees nothing wrong.
func TestExhaustedInodesRefuseWrites(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e, disk := diskFullEngine(1<<30, 0)
	api := mkSeries("job", "api")
	mustAppend(t, e, api, 100, 1.0)

	require.ErrorIs(t, e.Flush(ctx), backend.ErrNoSpace, "terabytes free is not room when no file can be created")
	assert.Zero(t, e.PartCount())

	_, err := e.Append(api, 200, 2.0)
	require.ErrorIs(t, err, backend.ErrNoSpace)

	disk.SetFreeInodes(1 << 20)
	require.NoError(t, e.Flush(ctx))
	assert.Equal(t, 1, e.PartCount())
}

// TestENOSPCFromTheBackendLatches covers the race the pre-flight check cannot: the disk fills
// between the check and the write, so the write itself returns ENOSPC. The engine must react to
// that exactly as it reacts to the check failing, rather than treating it as a transient fault and
// carrying on accepting.
func TestENOSPCFromTheBackendLatches(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := faultbackend.Wrap(backend.Memory()).Add(faultbackend.Rule{
		Kind: faultbackend.Write,
		Err:  syscall.ENOSPC,
	})
	e := engine.New(engine.Config{Backend: be, Prefix: "default/metrics"})

	api := mkSeries("job", "api")
	mustAppend(t, e, api, 100, 1.0)

	require.ErrorIs(t, e.Flush(ctx), syscall.ENOSPC)
	assert.True(t, e.Stats().OutOfSpace, "ENOSPC from the medium is the same verdict as an empty statfs")

	_, err := e.Append(api, 200, 2.0)
	require.ErrorIs(t, err, backend.ErrNoSpace)
}

// TestUnreportableCapacityIsUnbounded guards the default: most backends (memory, S3) cannot report
// capacity at all, and the guard must treat that as unbounded rather than as zero — which would
// make every ephemeral engine read-only.
func TestUnreportableCapacityIsUnbounded(t *testing.T) {
	t.Parallel()

	e, _ := diskFullEngine(backendtest.Unknown, backendtest.Unknown)
	api := mkSeries("job", "api")
	mustAppend(t, e, api, 100, 1.0)

	require.NoError(t, e.Flush(context.Background()))
	assert.Equal(t, 1, e.PartCount())
	assert.False(t, e.Stats().OutOfSpace)
}

// TestTransientBackendErrorDoesNotLatch keeps the valve narrow: only an out-of-space failure closes
// the ingest path. A backend that fails for any other reason must leave the node accepting, or
// every blip becomes an outage.
func TestTransientBackendErrorDoesNotLatch(t *testing.T) {
	t.Parallel()

	be := faultbackend.Wrap(backend.Memory()).Add(faultbackend.Rule{
		Kind:  faultbackend.Write,
		Err:   syscall.ECONNRESET,
		Times: 1,
	})
	e := engine.New(engine.Config{Backend: be, Prefix: "default/metrics"})

	api := mkSeries("job", "api")
	mustAppend(t, e, api, 100, 1.0)

	require.Error(t, e.Flush(context.Background()))
	assert.False(t, e.Stats().OutOfSpace)
	assert.False(t, diskguard.IsNoSpace(syscall.ECONNRESET))

	_, err := e.Append(api, 200, 2.0)
	require.NoError(t, err)
}
