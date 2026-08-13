package engine

import (
	"context"
	"runtime/debug"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/internal/memlimit"
)

// spaceBackend reports a fixed free-space figure, so the cap derivation does not depend on the test
// machine's disk.
type spaceBackend struct {
	backend.Backend

	free int64
	err  error
}

func (s spaceBackend) FreeSpace(context.Context) (int64, error) {
	if s.err != nil {
		return 0, s.err
	}

	return s.free, nil
}

// streamBackend builds objects incrementally, so a merge over it does not hold its output part.
// What it writes does not matter here — only that the capability is visible.
type streamBackend struct {
	backend.Backend

	free int64
}

func (s streamBackend) FreeSpace(context.Context) (int64, error) { return s.free, nil }

func (streamBackend) CreateObject(context.Context, string) (backend.ObjectWriter, error) {
	return nil, errors.New("not used")
}

// concurrencyFunc adapts a fixed count to the config callback; 0 means unset.
func concurrencyFunc(n int) func() int {
	if n == 0 {
		return nil
	}

	return func() int { return n }
}

func TestMergeCapBytes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	// Every case fixes MergeMemoryBytes, so the cap under test is the one the case is about and
	// not the memory of whatever machine runs it. The memory bound has its own cases below.
	const roomy = 1 << 50

	cases := []struct {
		name        string
		ceiling     int64
		memory      int64
		concurrency int
		backend     backend.Backend
		want        int64
	}{
		{
			name:    "no reporting keeps the ceiling",
			ceiling: 1 << 30,
			backend: backend.Memory(),
			want:    1 << 30,
		},
		{
			name:    "unset ceiling defaults",
			backend: backend.Memory(),
			want:    defaultMergeCeilingBytes,
		},
		{
			name:    "negative ceiling never seals",
			ceiling: -1,
			backend: backend.Memory(),
			want:    0,
		},
		{
			// 8 GiB free, 1 merge, halved for the merge's own output: 4 GiB, below the ceiling.
			name:    "free space lowers the ceiling",
			ceiling: 1 << 40,
			backend: spaceBackend{Backend: backend.Memory(), free: 8 << 30},
			want:    4 << 30,
		},
		{
			// The same disk shared by four concurrent merges gives each a quarter.
			name:        "concurrency divides the share",
			ceiling:     1 << 40,
			concurrency: 4,
			backend:     spaceBackend{Backend: backend.Memory(), free: 8 << 30},
			want:        1 << 30,
		},
		{
			name:    "ceiling wins when the disk is roomy",
			ceiling: 1 << 20,
			backend: spaceBackend{Backend: backend.Memory(), free: 1 << 40},
			want:    1 << 20,
		},
		{
			// A nearly full disk must still merge, or part count stays high exactly when compaction
			// matters most — it degrades to the smallest cap instead of sealing everything.
			name:    "nearly full falls to the floor",
			ceiling: 1 << 40,
			backend: spaceBackend{Backend: backend.Memory(), free: 1},
			want:    minMergeCapBytes,
		},
		{
			// The output part is buffered in RAM until it is sealed, so a big disk behind a small
			// memory budget must not size a part the process cannot hold: 4 GiB of merge memory,
			// halved for the serialize step, is a 2 GiB part however much disk there is.
			name:    "memory lowers the disk-derived share",
			ceiling: 1 << 40,
			memory:  4 << 30,
			backend: spaceBackend{Backend: backend.Memory(), free: 1 << 45},
			want:    2 << 30,
		},
		{
			// The memory bound applies to a backend that reports nothing too — that is the OOM
			// shape, where the ceiling alone used to stand.
			name:    "memory lowers the ceiling without space reporting",
			ceiling: 16 << 30,
			memory:  512 << 20,
			backend: backend.Memory(),
			want:    256 << 20,
		},
		{
			name:        "concurrency divides the memory too",
			ceiling:     1 << 40,
			memory:      4 << 30,
			concurrency: 4,
			backend:     backend.Memory(),
			want:        512 << 20,
		},
		{
			name:    "a memory budget below the floor still merges",
			ceiling: 1 << 40,
			memory:  1 << 10,
			backend: backend.Memory(),
			want:    minMergeCapBytes,
		},
		{
			name:    "negative memory opts out",
			ceiling: 1 << 30,
			memory:  -1,
			backend: backend.Memory(),
			want:    1 << 30,
		},
		{
			// The whole point of #296: over a backend that takes the part incrementally, the writer
			// no longer holds it, so the disk sizes the part again — half of 8 GiB, not the memory
			// share (256 MiB) that would otherwise bind.
			name:    "streaming writes leave the disk in charge",
			ceiling: 1 << 40,
			memory:  512 << 20,
			backend: streamBackend{Backend: backend.Memory(), free: 8 << 30},
			want:    4 << 30,
		},
		{
			// Memory dropping out does not make the cap unbounded: the ceiling is still the last word.
			name:    "streaming writes still respect the ceiling",
			ceiling: 1 << 30,
			memory:  512 << 20,
			backend: streamBackend{Backend: backend.Memory(), free: 1 << 45},
			want:    1 << 30,
		},
		{
			name:    "a reporting error keeps the ceiling",
			ceiling: 1 << 30,
			backend: spaceBackend{Backend: backend.Memory(), err: errors.New("statfs failed")},
			want:    1 << 30,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			memory := tc.memory
			if memory == 0 {
				memory = roomy
			}

			e := New(Config{
				Backend:           tc.backend,
				Prefix:            "t",
				MergeCeilingBytes: tc.ceiling,
				MergeMemoryBytes:  memory,
				MergeConcurrency:  concurrencyFunc(tc.concurrency),
			})

			assert.Equal(t, tc.want, e.mergeCapBytes(ctx))
		})
	}
}

// TestMergeCapFitsThePodThatOOMed replays the incident with its real numbers: a 3.6 GiB GOMEMLIMIT
// over a 464 GiB volume, where the disk-derived share (232 GiB) clamped to the 16 GiB ceiling and
// the merge OOMed building a part the pod could not hold. Not parallel: GOMEMLIMIT is process-wide.
//
//nolint:paralleltest // GOMEMLIMIT is process-wide, so this case cannot run alongside the others
func TestMergeCapFitsThePodThatOOMed(t *testing.T) {
	const podLimit = 3865470566

	restore := debug.SetMemoryLimit(podLimit)
	t.Cleanup(func() { debug.SetMemoryLimit(restore) })

	e := New(Config{
		Backend: spaceBackend{Backend: backend.Memory(), free: 464 << 30},
		Prefix:  "t",
	})

	capBytes := e.mergeCapBytes(context.Background())

	assert.Positive(t, capBytes)
	assert.LessOrEqual(t, capBytes, memlimit.MergeShare(0, 1, mergeBufferAmplification),
		"the cap must fit the merge's share of the pod's memory, not the volume behind it")
	assert.Less(t, capBytes, int64(podLimit),
		"a part the pod cannot hold is what OOMKilled it")
}

// TestMergeCapRecoversTheVolumeWhenWritesStream is the incident's numbers again, over a backend that
// builds objects incrementally: the part the pod could not hold is no longer held, so the 464 GiB
// volume sizes it and the cap returns to the ceiling — the widening #286 asked for and the memory
// bound had to take back.
//
//nolint:paralleltest // GOMEMLIMIT is process-wide, so this case cannot run alongside the others
func TestMergeCapRecoversTheVolumeWhenWritesStream(t *testing.T) {
	const podLimit = 3865470566

	restore := debug.SetMemoryLimit(podLimit)
	t.Cleanup(func() { debug.SetMemoryLimit(restore) })

	e := New(Config{
		Backend: streamBackend{Backend: backend.Memory(), free: 464 << 30},
		Prefix:  "t",
	})

	capBytes := e.mergeCapBytes(context.Background())

	assert.Equal(t, int64(defaultMergeCeilingBytes), capBytes)
	assert.Greater(t, capBytes, memlimit.MergeShare(0, 1, mergeBufferAmplification),
		"the cap must no longer be held down by memory the writer does not spend")
}

// TestMergeCapUsesRecordedPartSize pins the other half: sealing compares a part's recorded on-disk
// size, falling back to the row estimate for a part written before the manifest carried one.
func TestMergeCapUsesRecordedPartSize(t *testing.T) {
	t.Parallel()

	sized := partOfSize(0, 4096)
	require.Equal(t, int64(4096), sized.sizeBytes())

	legacy := &part{index: partIndex{starts: []int32{0, 10}}}
	assert.Equal(t, int64(10*partRowBytes), legacy.sizeBytes(),
		"a part with no recorded size falls back to the uncompressed row estimate")
}
