package engine

import (
	"context"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
)

// spaceBackend wraps a backend with a fixed free-space report, so the cap derivation can be
// exercised without depending on the test machine's actual disk.
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

// concurrencyFunc adapts a fixed count to the config callback; 0 means "unset".
func concurrencyFunc(n int) func() int {
	if n == 0 {
		return nil
	}

	return func() int { return n }
}

func TestMergeCapBytes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	cases := []struct {
		name        string
		ceiling     int64
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
			name:    "a reporting error keeps the ceiling",
			ceiling: 1 << 30,
			backend: spaceBackend{Backend: backend.Memory(), err: errors.New("statfs failed")},
			want:    1 << 30,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := New(Config{
				Backend:           tc.backend,
				Prefix:            "t",
				MergeCeilingBytes: tc.ceiling,
				MergeConcurrency:  concurrencyFunc(tc.concurrency),
			})

			assert.Equal(t, tc.want, e.mergeCapBytes(ctx))
		})
	}
}

// TestMergeCapUsesRecordedPartSize pins the other half: the cap is compared against a part's
// recorded on-disk size, so sealing means "this part is big on disk" rather than "this part has
// many rows". A part written before the manifest carried a size falls back to the row estimate.
func TestMergeCapUsesRecordedPartSize(t *testing.T) {
	t.Parallel()

	sized := partOfSize(0, 4096)
	require.Equal(t, int64(4096), sized.sizeBytes())

	legacy := &part{index: partIndex{starts: []int32{0, 10}}}
	assert.Equal(t, int64(10*partRowBytes), legacy.sizeBytes(),
		"a part with no recorded size falls back to the uncompressed row estimate")
}
