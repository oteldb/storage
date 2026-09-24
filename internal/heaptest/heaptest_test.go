package heaptest

import (
	"context"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend/file"
)

const size = 8 << 20

var sink []byte

//nolint:paralleltest // reads process-wide heap counters
func TestLive(t *testing.T) {
	sink = nil
	base := Live()

	sink = make([]byte, size)
	held := Live()

	runtime.KeepAlive(sink)
	sink = nil

	assert.Greater(t, held, base+size/2, "the baseline is process-wide, so it can shed garbage too")
}

//nolint:paralleltest // reads process-wide heap counters
func TestAllocated(t *testing.T) {
	assert.GreaterOrEqual(t, Allocated(func() { sink = make([]byte, size) }), uint64(size))
	sink = nil
}

//nolint:paralleltest // reads process-wide heap counters
func TestInuseGrowth(t *testing.T) {
	runtime.GC()

	// Spans the collector swept can return to the page heap inside the window, so the growth is
	// the allocation less whatever that released.
	assert.Greater(t, InuseGrowth(func() { sink = make([]byte, size) }), uint64(size/2))
	sink = nil
}

//nolint:paralleltest // reads process-wide heap counters
func TestBytesPerOp(t *testing.T) {
	assert.Zero(t, BytesPerOp(func() {}))
	assert.GreaterOrEqual(t, BytesPerOp(func() { sink = make([]byte, 1024) }), int64(1024))
	sink = nil
}

func newSampler(t *testing.T) *FileSampler {
	t.Helper()

	fb, err := file.New(t.TempDir())
	require.NoError(t, err)

	return &FileSampler{File: fb, KeyContains: "/c/"}
}

//nolint:paralleltest // reads process-wide heap counters
func TestFileSamplerKeys(t *testing.T) {
	ctx := context.Background()
	s := newSampler(t)

	require.NoError(t, s.Write(ctx, "p/c/0", make([]byte, size)))
	require.NoError(t, s.Write(ctx, "p/meta", make([]byte, size)))

	s.arm()

	_, err := s.Read(ctx, "p/meta")
	require.NoError(t, err)
	_, err = s.ReadAt(ctx, "p/meta", 0, 16)
	require.NoError(t, err)
	require.NoError(t, s.WriteDeferred(ctx, "p/c/1", []byte("x")))

	assert.Zero(t, s.disarm(), "only reads of a matching key sample")

	data, err := s.Read(ctx, "p/c/0")
	require.NoError(t, err)
	assert.Len(t, data, size)
	assert.Zero(t, s.disarm(), "a disarmed sampler does not sample")

	s.arm()

	_, err = s.ReadAt(ctx, "p/c/0", 0, 16)
	require.NoError(t, err)
	assert.Positive(t, s.disarm())

	s.Writes = true
	s.arm()

	require.NoError(t, s.WriteDeferred(ctx, "p/c/2", []byte("x")))
	assert.Positive(t, s.disarm())

	got, err := s.Read(ctx, "p/c/2")
	require.NoError(t, err)
	assert.Equal(t, []byte("x"), got)
}

//nolint:paralleltest // reads process-wide heap counters
func TestResident(t *testing.T) {
	ctx := context.Background()
	s := newSampler(t)

	require.NoError(t, s.Write(ctx, "p/c/0", make([]byte, size)))

	resident := Resident(t, s, func() {
		_, err := s.Read(ctx, "p/c/0")
		require.NoError(t, err)
	})

	assert.GreaterOrEqual(t, resident, uint64(size), "the bytes just read are live at the sample")
	assert.Zero(t, s.disarm(), "Resident disarms")
}

type recordTB struct {
	testing.TB

	errors int
}

func (*recordTB) Helper()                 {}
func (*recordTB) Logf(string, ...any)     {}
func (r *recordTB) Errorf(string, ...any) { r.errors++ }

func TestAssertFlat(t *testing.T) {
	t.Parallel()

	f := Flat{Floor: 10 << 20, MaxGrowth: 2, SourceShare: 3}
	small := Run{Resident: 4 << 20, Source: 64 << 20}

	for _, tc := range []struct {
		name   string
		large  Run
		errors int
	}{
		{"flat", Run{Resident: 12 << 20, Source: 512 << 20}, 0},
		{"floored", Run{Resident: 19 << 20, Source: 512 << 20}, 0},
		{"proportional", Run{Resident: 400 << 20, Source: 512 << 20}, 2},
		{"over source share", Run{Resident: 19 << 20, Source: 48 << 20}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tb := &recordTB{TB: t}
			AssertFlat(tb, small, tc.large, f)
			assert.Equal(t, tc.errors, tb.errors)
		})
	}
}
