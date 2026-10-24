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

var _ backend.ObjectCreator = streamBackend{}

func (s streamBackend) FreeSpace(context.Context) (int64, error) { return s.free, nil }

func (streamBackend) CreateObject(context.Context, string) (backend.ObjectWriter, error) {
	return nil, errors.New("not used")
}

func (streamBackend) StreamsWrites() bool { return true }

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
			name: "small memory leaves the disk ceiling unchanged", ceiling: 1 << 30,
			memory: 1 << 20, backend: backend.Memory(), want: 1 << 30,
		},
		{
			name: "disk divides by writer fanout", ceiling: 1 << 40, memory: 256 << 20,
			concurrency: 16, backend: spaceBackend{Backend: backend.Memory(), free: 512 << 20}, want: 16 << 20,
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

//nolint:paralleltest // GOMEMLIMIT is process-wide
func TestMergeCapIndependentOfPodMemory(t *testing.T) {
	restore := debug.SetMemoryLimit(3865470566)
	t.Cleanup(func() { debug.SetMemoryLimit(restore) })
	e := New(Config{Backend: spaceBackend{Backend: backend.Memory(), free: 464 << 30}, Prefix: "t"})
	capBytes := e.mergeCapBytes(context.Background())
	assert.Equal(t, defaultMergeCeilingBytes, capBytes)
	assert.Less(t, e.mergeMemoryBudgetBytes(), capBytes)
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

	assert.Equal(t, defaultMergeCeilingBytes, capBytes)
	assert.Greater(t, capBytes, memlimit.MergeShare(0, 1),
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

// TestAdmissionRequestMatchesCapDerivation is the coherence invariant between the two halves of the
// budget: the bytes a merge reserves from the pool must be the share its cap was derived from, and
// the pool must admit at least as many merges as the cap arithmetic assumed. When those disagree
// the bound is wrong in one direction or the other — the first round of this work divided the disk
// by one number and admitted merges by another, and nothing caught it.
func TestAdmissionRequestMatchesCapDerivation(t *testing.T) {
	t.Parallel()

	budgets := []int64{16 << 20, 64 << 20, 256 << 20, 1 << 30, 3 << 30, 64 << 30}
	fanouts := []int{1, 2, 4, 16}

	for _, budget := range budgets {
		for _, fanout := range fanouts {
			e := New(Config{
				Backend: backend.Memory(), Prefix: "t",
				MergeMemoryBytes: budget,
				MergeConcurrency: concurrencyFunc(fanout),
			})

			k := e.mergeConcurrency()
			request := e.mergeMemoryBudgetBytes()

			assert.LessOrEqual(t, k, fanout, "the fan-out caps how many merges the budget admits")
			assert.Positive(t, request)
			assert.Equal(t, memlimit.MergeShare(budget, k), request,
				"a merge must reserve the share its own concurrency implies")

			// The pool is sized at the whole budget and each merge takes `request`, so this is the
			// number that actually fits. It must not be fewer than the cap arithmetic assumed.
			assert.GreaterOrEqual(t, memlimit.MergeBudget(budget)/request, int64(k),
				"the pool must admit every merge the cap was divided for")
		}
	}
}
