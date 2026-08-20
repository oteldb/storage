package diskguard_test

import (
	"context"
	"syscall"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
	"github.com/oteldb/storage/internal/diskguard"
)

// brokenProbe reports a failure that is not a verdict: the statfs itself errored.
type brokenProbe struct {
	backend.Backend
}

var errProbe = errors.New("statfs failed")

func (brokenProbe) FreeSpace(context.Context) (int64, error) { return 0, errProbe }

func TestAdmit(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	reserve := diskguard.Reserve{Bytes: 1000, Inodes: 10}

	for _, tt := range []struct {
		name                 string
		freeBytes, freeInode int64
		needBytes, needObjs  int64
		wantNoSpace          bool
	}{
		{"fits", 5000, 100, 1000, 10, false},
		{"exactly fits", 2000, 20, 1000, 10, false},
		{"one byte short", 1999, 20, 1000, 10, true},
		{"one inode short", 5000, 19, 1000, 10, true},
		{"unreported axes are unbounded", backendtest.Unknown, backendtest.Unknown, 1 << 40, 1 << 20, false},
		{"bytes unreported, inodes exhausted", backendtest.Unknown, 0, 1 << 40, 1, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := backendtest.WithCapacity(backend.Memory(), tt.freeBytes, tt.freeInode)
			g := diskguard.New(reserve)

			err := g.Admit(ctx, b, tt.needBytes, tt.needObjs)
			if !tt.wantNoSpace {
				require.NoError(t, err)
				assert.False(t, g.Exhausted())

				return
			}

			require.ErrorIs(t, err, backend.ErrNoSpace)
			assert.True(t, g.Exhausted())
			require.ErrorIs(t, g.Err(), backend.ErrNoSpace)
		})
	}
}

func TestAdmitClearsAPreviousFailure(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := backendtest.WithCapacity(backend.Memory(), 0, 1<<20)
	g := diskguard.New(diskguard.Reserve{Bytes: 1000, Inodes: 10})

	require.ErrorIs(t, g.Admit(ctx, b, 0, 0), backend.ErrNoSpace)

	b.SetFreeBytes(1 << 30)
	require.NoError(t, g.Admit(ctx, b, 0, 0))
	assert.False(t, g.Exhausted(), "a node must recover on its own once the disk is emptied")
	assert.NoError(t, g.Err())
}

// TestBrokenProbeAdmits keeps a broken capacity check from becoming an outage of its own: it is
// reported to the caller, but it neither latches nor claims the medium is full.
func TestBrokenProbeAdmits(t *testing.T) {
	t.Parallel()

	g := diskguard.New(diskguard.Reserve{})

	err := g.Admit(context.Background(), brokenProbe{Backend: backend.Memory()}, 0, 0)
	require.ErrorIs(t, err, errProbe)
	assert.False(t, diskguard.IsNoSpace(err))
	assert.False(t, g.Exhausted())
}

func TestObserveLatchesOnlyOutOfSpace(t *testing.T) {
	t.Parallel()

	g := diskguard.New(diskguard.Reserve{})

	g.Observe(nil)
	g.Observe(errors.New("transient backend failure"))
	assert.False(t, g.Exhausted())

	g.Observe(errors.Wrap(syscall.ENOSPC, "write column 0"))
	assert.True(t, g.Exhausted())
	require.ErrorIs(t, g.Err(), backend.ErrNoSpace)
	assert.Contains(t, g.Err().Error(), "write column 0", "the latched state names what failed")
}

func TestReserveDefaultsAndDisabling(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := backendtest.WithCapacity(backend.Memory(), diskguard.DefaultReserveBytes-1, diskguard.DefaultReserveInodes-1)

	require.ErrorIs(t, diskguard.New(diskguard.Reserve{}).Admit(ctx, b, 0, 0), backend.ErrNoSpace,
		"the zero Reserve takes the defaults")
	require.NoError(t, diskguard.New(diskguard.Reserve{Bytes: -1, Inodes: -1}).Admit(ctx, b, 0, 0),
		"a negative reserve opts out of that axis")
}

// TestNilBackendAdmits covers the head-only engine: no backend means no medium to run out of.
func TestNilBackendAdmits(t *testing.T) {
	t.Parallel()

	g := diskguard.New(diskguard.Reserve{})
	require.NoError(t, g.Admit(context.Background(), nil, 1<<40, 1<<20))
}
