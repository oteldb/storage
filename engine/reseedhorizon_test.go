package engine_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/signal"
)

// TestReseedHorizonRate measures how fast a shard turns over parts under a real flush/merge cycle,
// so the reseed horizons ([bucketindex.MaxRemovals] / MaxWants) can be expressed in ingest instead
// of guessed. It reports, per flushed part: removals emitted, removals no final live entry
// supersedes (the ones a tombstone alone carries), and the steady-state live part count.
func TestReseedHorizonRate(t *testing.T) {
	t.Parallel()

	const (
		cycles      = 300
		seriesCount = 400
		stepsPer    = 16
		step        = int64(time.Minute)
		rowBytes    = 32
		maxRows     = 2048
	)

	ctx := context.Background()
	b := backend.Memory()
	e := engine.New(engine.Config{
		Backend:      b,
		Prefix:       "default/metrics",
		MaxPartBytes: maxRows * rowBytes,
	})

	series := make([]signal.Series, seriesCount)
	ids := make([]signal.SeriesID, seriesCount)

	for i := range series {
		series[i] = mkSeries("__name__", "cpu", "host", "h"+strconv.Itoa(i))
		ids[i] = series[i].Hash()
	}

	tss := make([]int64, seriesCount)
	vals := make([]float64, seriesCount)

	key := "default/metrics/" + bucketindex.Object

	live := map[string]bucketindex.Entry{}
	removed := map[string]bucketindex.Entry{}
	created := map[string]struct{}{}

	observe := func() {
		ix, err := bucketindex.Load(ctx, b, key)
		require.NoError(t, err)

		now := make(map[string]bucketindex.Entry, len(ix.Entries))

		for i := range ix.Entries {
			if ix.Entries[i].Hole {
				continue
			}

			now[ix.Entries[i].Prefix] = ix.Entries[i]
			created[ix.Entries[i].Prefix] = struct{}{}
		}

		for p, ent := range live {
			if _, ok := now[p]; !ok {
				removed[p] = ent
			}
		}

		live = now
	}

	var (
		ts   = int64(1_700_000_000) * int64(time.Second)
		rows int
	)

	for range cycles {
		for range stepsPer {
			for j := range seriesCount {
				tss[j], vals[j] = ts, float64(j)
			}

			_, err := e.AppendBatch(ids, tss, vals, nil, func(k int) signal.Series { return series[k] }, engine.AppendLimits{})
			require.NoError(t, err)

			ts += step
			rows += seriesCount
		}

		require.NoError(t, e.Flush(ctx))
		observe()
		require.NoError(t, e.Merge(ctx, 0))
		observe()
	}

	ixFinal, err := bucketindex.Load(ctx, b, key)
	require.NoError(t, err)

	var unexplained int

	for _, ent := range removed {
		sup := false

		for i := range ixFinal.Entries {
			if ixFinal.Entries[i].Supersedes(ent) {
				sup = true

				break
			}
		}

		if !sup {
			unexplained++
		}
	}

	logical := int64(rows) * rowBytes
	perRemoval := logical / int64(max(len(removed), 1))
	span := time.Duration(ts - int64(1_700_000_000)*int64(time.Second))

	t.Logf("ingest: rows=%d logical=%s wallSpan=%s cycles=%d", rows, human(logical), span, cycles)
	t.Logf("parts: created=%d live=%d maxPartBytes=%s", len(created), len(ixFinal.Entries), human(maxRows*rowBytes))
	t.Logf("removals: total=%d unexplainedBySupersession=%d tombstonesKept=%d",
		len(removed), unexplained, len(ixFinal.Removed))
	t.Logf("removals per created part = %.3f", float64(len(removed))/float64(max(len(created), 1)))
	t.Logf("logical bytes per removal = %s", human(perRemoval))
	t.Logf("=> MaxRemovals(%d) ≈ %s of logical ingest per shard at MaxPartBytes=%s",
		bucketindex.MaxRemovals, human(perRemoval*bucketindex.MaxRemovals), human(maxRows*rowBytes))
	t.Logf("=> scaled to the 64 MiB production MaxPartBytes: %s",
		human(perRemoval*bucketindex.MaxRemovals*(64<<20)/(maxRows*rowBytes)))
	t.Logf("=> live parts per logical byte: 1 part per %s => %d live parts at %s",
		human(logical/int64(max(len(ixFinal.Entries), 1))), len(ixFinal.Entries), human(logical))
}

// TestReseedHorizonWithRetention is [TestReseedHorizonRate] with retention running, which is the
// only source of a removal no successor supersedes: a merged-away part lives on inside its
// successor's block interval forever, a retention-dropped one does not.
func TestReseedHorizonWithRetention(t *testing.T) {
	t.Parallel()

	const (
		cycles      = 300
		seriesCount = 400
		stepsPer    = 16
		step        = int64(time.Minute)
		rowBytes    = 32
		maxRows     = 2048
		retain      = int64(12 * time.Hour)
	)

	ctx := context.Background()
	b := backend.Memory()
	e := engine.New(engine.Config{
		Backend:           b,
		Prefix:            "default/metrics",
		MaxPartBytes:      maxRows * rowBytes,
		MergeCeilingBytes: 8 << 20,
	})

	series := make([]signal.Series, seriesCount)
	ids := make([]signal.SeriesID, seriesCount)

	for i := range series {
		series[i] = mkSeries("__name__", "cpu", "host", "h"+strconv.Itoa(i))
		ids[i] = series[i].Hash()
	}

	tss := make([]int64, seriesCount)
	vals := make([]float64, seriesCount)

	key := "default/metrics/" + bucketindex.Object

	live := map[string]bucketindex.Entry{}
	removed := map[string]bucketindex.Entry{}
	created := map[string]struct{}{}

	observe := func() {
		ix, err := bucketindex.Load(ctx, b, key)
		require.NoError(t, err)

		now := make(map[string]bucketindex.Entry, len(ix.Entries))

		for i := range ix.Entries {
			if ix.Entries[i].Hole {
				continue
			}

			now[ix.Entries[i].Prefix] = ix.Entries[i]
			created[ix.Entries[i].Prefix] = struct{}{}
		}

		for p, ent := range live {
			if _, ok := now[p]; !ok {
				removed[p] = ent
			}
		}

		live = now
	}

	var (
		start = int64(1_700_000_000) * int64(time.Second)
		ts    = start
		rows  int
	)

	for range cycles {
		for range stepsPer {
			for j := range seriesCount {
				tss[j], vals[j] = ts, float64(j)
			}

			_, err := e.AppendBatch(ids, tss, vals, nil, func(k int) signal.Series { return series[k] }, engine.AppendLimits{})
			require.NoError(t, err)

			ts += step
			rows += seriesCount
		}

		require.NoError(t, e.Flush(ctx))
		observe()
		require.NoError(t, e.Merge(ctx, ts-retain))
		observe()
	}

	ixFinal, err := bucketindex.Load(ctx, b, key)
	require.NoError(t, err)

	var unexplained int

	for _, ent := range removed {
		sup := false

		for i := range ixFinal.Entries {
			if ixFinal.Entries[i].Supersedes(ent) {
				sup = true

				break
			}
		}

		if !sup {
			unexplained++
		}
	}

	logical := int64(rows) * rowBytes
	perUnexplained := logical / int64(max(unexplained, 1))

	t.Logf("ingest: rows=%d logical=%s wallSpan=%s retention=%s",
		rows, human(logical), time.Duration(ts-start), time.Duration(retain))
	t.Logf("parts: created=%d live=%d", len(created), len(ixFinal.Entries))
	t.Logf("removals: total=%d unexplainedBySupersession=%d tombstonesKept=%d",
		len(removed), unexplained, len(ixFinal.Removed))
	t.Logf("logical bytes per unexplained removal = %s", human(perUnexplained))
	t.Logf("=> MaxRemovals(%d) unexplained removals ≈ %s of logical ingest at MaxPartBytes=%s",
		bucketindex.MaxRemovals, human(perUnexplained*bucketindex.MaxRemovals), human(maxRows*rowBytes))
}

func human(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.2f GiB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.2f MiB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.2f KiB", float64(n)/(1<<10))
	default:
		return fmt.Sprintf("%d B", n)
	}
}
