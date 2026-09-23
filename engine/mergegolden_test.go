package engine_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/go-faster/sdk/gold"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/signal"
)

// wholeObjectBackend offers no ranged or view reads, so every read a merge makes is a whole-object
// Read — the shape of a minimal embedder backend.
type wholeObjectBackend struct{ backend.Backend }

// goldenMergeCorpus covers each source-column form the metric merge reads: a noisy value column
// spanning several compression frames, a later part overwriting an earlier one's timestamps, a
// constant-collapsed value column, a weight column, and — in the second round — a source that is
// itself a merge output, so the streamed column layout is read back as well as the flushed one.
func goldenMergeCorpus(t *testing.T, b backend.Backend) *engine.Engine {
	t.Helper()

	ctx := context.Background()
	e := engine.New(engine.Config{
		Backend: b, Prefix: "golden/metrics", MaxPartBytes: 0, MergeMemoryBytes: 1 << 30,
	})

	const series, samples = 300, 64

	ser := make([]signal.Series, series)
	ids := make([]signal.SeriesID, series)

	for i := range series {
		ser[i] = mkSeries("__name__", "golden", "instance", "host-"+strconv.Itoa(i))
		ids[i] = ser[i].Hash()
	}

	r := rand.New(rand.NewPCG(7, 11))

	flush := func(lo, hi, from int, value func(s, i int) float64, weight func(s, i int) float64) {
		t.Helper()

		var (
			batch []signal.SeriesID
			ts    []int64
			vals  []float64
			sf    []float64
			owner []int
		)

		for s := lo; s < hi; s++ {
			for i := range samples {
				batch = append(batch, ids[s])
				ts = append(ts, int64(from+i)*15_000+int64(s%5))
				vals = append(vals, value(s, i))
				owner = append(owner, s)

				if weight != nil {
					sf = append(sf, weight(s, i))
				}
			}
		}

		_, err := e.AppendBatch(batch, ts, vals, sf,
			func(i int) signal.Series { return ser[owner[i]] }, engine.AppendLimits{})
		require.NoError(t, err)
		require.NoError(t, e.Flush(ctx))
	}

	noisy := func(_, _ int) float64 { return r.Float64() * 1e6 }

	mergeAll := func() {
		t.Helper()

		require.NoError(t, e.Merge(ctx, 0))

		for range 8 {
			prev := e.PartCount()

			require.NoError(t, e.MergeWith(ctx, engine.MergeOptions{Force: true}))

			if e.PartCount() == prev {
				break
			}
		}

		require.Equal(t, 1, e.PartCount())
	}

	flush(0, 200, 0, noisy, nil)
	flush(100, series, 32, noisy, nil)
	flush(0, series, 96, func(_, _ int) float64 { return 1 }, nil)
	flush(50, 250, 160, func(s, i int) float64 { return float64(s*samples + i) },
		func(_, i int) float64 { return float64(1 + i%3) })
	mergeAll()

	flush(0, series, 224, noisy, nil)
	mergeAll()

	return e
}

// partDigest renders every object of every live part, keyed by its name within the part, since the
// part's own prefix is random.
func partDigest(t *testing.T, e *engine.Engine, b backend.Backend) string {
	t.Helper()

	ctx := context.Background()

	var lines []string

	for _, prefix := range e.PartPrefixes() {
		keys, err := b.List(ctx, prefix+"/")
		require.NoError(t, err)

		for _, key := range keys {
			data, err := b.Read(ctx, key)
			require.NoError(t, err)

			lines = append(lines, fmt.Sprintf("%-24s %8d %x",
				strings.TrimPrefix(key, prefix+"/"), len(data), sha256.Sum256(data)))
		}
	}

	slices.Sort(lines)

	return strings.Join(lines, "\n") + "\n"
}

// TestMergeOutputGolden pins the merged part byte for byte, whatever backend the sources are read
// through: how a merge reads its sources is not allowed to change what it writes.
func TestMergeOutputGolden(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		open func(t *testing.T) backend.Backend
	}{
		{"memory", func(*testing.T) backend.Backend { return backend.Memory() }},
		{"file", func(t *testing.T) backend.Backend {
			t.Helper()

			b, err := file.New(t.TempDir())
			require.NoError(t, err)

			return b
		}},
		{"whole-object", func(*testing.T) backend.Backend { return wholeObjectBackend{backend.Memory()} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := tc.open(t)
			e := goldenMergeCorpus(t, b)

			gold.Str(t, partDigest(t, e, b), "merge_output.txt")
		})
	}
}
