package enginetest

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/internal/timebucket"
)

const (
	hour = int64(time.Hour)
	day  = 24 * hour
	// staleTs is where a producer's late rows land, pinning every straddler's MinTime.
	staleTs = 9 * hour
	// convergeCycles bounds a merge loop that must reach its fixpoint well before it.
	convergeCycles = 500

	staleStream = "stale"
	freshStream = "fresh"
)

// flushSpans flushes one part per span, holding a row at each end of it in a stream of its own.
func flushSpans(t *testing.T, e Engine, spans [][2]int64) {
	t.Helper()

	for i, s := range spans {
		stream := fmt.Sprintf("p%d", i)
		e.Append(t, Row{Stream: stream, Ts: s[0], Val: int64(2 * i)}, Row{Stream: stream, Ts: s[1], Val: int64(2*i + 1)})
		require.NoError(t, e.Flush(context.Background()))
	}
}

// flushStraddler flushes one part in the shape the exemplar stand produced: a re-exported stale row
// plus a fresh one at now.
func flushStraddler(t *testing.T, e Engine, now int64, i int) {
	t.Helper()

	e.Append(t, Row{Stream: staleStream, Ts: staleTs + int64(i), Val: int64(i)}, Row{Stream: freshStream, Ts: now, Val: int64(i)})
	require.NoError(t, e.Flush(context.Background()))
}

// mergeToFixpoint merges until no part is a candidate, returning the cycles it took.
func mergeToFixpoint(t *testing.T, e Engine) int {
	t.Helper()

	ctx := context.Background()

	for cycle := range convergeCycles {
		if e.MergeShape(0).Candidates == 0 {
			return cycle
		}

		require.NoError(t, e.Merge(ctx, 0))
	}

	require.FailNow(t, "merge did not reach a fixpoint", "%d cycles", convergeCycles)

	return 0
}

// requireLadderFit fails when a part fits no ladder level.
func requireLadderFit(t *testing.T, e Engine) {
	t.Helper()

	for _, p := range e.Parts() {
		_, ok := timebucket.Finest(p.MinTime, p.MaxTime)
		require.True(t, ok, "part %s spans [%v, %v], across a day boundary",
			p.ID, time.Duration(p.MinTime), time.Duration(p.MaxTime))
	}
}

// storedRows returns every row of the named streams as "stream/ts/val", sorted.
func storedRows(t *testing.T, e Engine, streams ...string) []string {
	t.Helper()

	var out []string

	for _, s := range streams {
		for _, r := range rows(t, e, s) {
			out = append(out, fmt.Sprintf("%s/%d/%d", s, r.Ts, r.Val))
		}
	}

	slices.Sort(out)

	return out
}

func streamNames(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("p%d", i)
	}

	return out
}

// dayBoundarySpans returns five 2-minute spans at base, 10s apart.
func dayBoundarySpans(base int64) [][2]int64 {
	out := make([][2]int64, 0, 5)
	for i := range int64(5) {
		start := base + i*int64(10*time.Second)
		out = append(out, [2]int64{start, start + 2*int64(time.Minute)})
	}

	return out
}

// straddlingPartsCompact is #450: a part crossing a day boundary fits no ladder level, so the ladder
// never grouped it, however small. Five such parts two minutes apart must still merge, into parts
// that each fit a level; the assertion prescribes no more of their shape.
func straddlingPartsCompact(t *testing.T, k Kind) {
	t.Helper()

	e := k.open(t, backend.Memory())
	spans := dayBoundarySpans(day - int64(time.Minute))
	flushSpans(t, e, spans)

	want := storedRows(t, e, streamNames(len(spans))...)

	mergeToFixpoint(t, e)
	assert.Less(t, e.PartCount(), len(spans), "repeated merges must reduce the part count")
	requireLadderFit(t, e)
	assert.Equal(t, want, storedRows(t, e, streamNames(len(spans))...))
}

// coLocatedPartsCompact is the control for [straddlingPartsCompact]: the same spans inside one hour.
func coLocatedPartsCompact(t *testing.T, k Kind) {
	t.Helper()

	e := k.open(t, backend.Memory())
	spans := dayBoundarySpans(hour + int64(time.Minute))
	flushSpans(t, e, spans)

	mergeToFixpoint(t, e)
	assert.Less(t, e.PartCount(), len(spans), "repeated merges must reduce the part count")
}

// straddlerBacklogConverges is the stand's backlog after the fix: every part straddles, and merging
// converges to a few day-aligned parts holding the same rows.
func straddlerBacklogConverges(t *testing.T, k Kind) {
	t.Helper()

	const flushes = 64

	e := k.open(t, backend.Memory())
	for i := range flushes {
		flushStraddler(t, e, 2*day+int64(i)*int64(30*time.Minute), i)
	}

	require.Equal(t, flushes, e.PartCount())
	require.Positive(t, e.MergeShape(0).Candidates, "straddlers are merge candidates")

	want := storedRows(t, e, staleStream, freshStream)
	cycles := mergeToFixpoint(t, e)

	requireLadderFit(t, e)
	assert.Equal(t, want, storedRows(t, e, staleStream, freshStream), "splitting must neither drop nor duplicate a row")
	assert.LessOrEqual(t, e.PartCount(), 4, "the backlog must collapse to a few parts per day")

	t.Logf("%d straddlers converged in %d cycles to %d parts", flushes, cycles, e.PartCount())
}

// straddlersDoNotAccumulate runs a merge after every flush while a producer keeps re-exporting its
// stale row: the part count stays bounded rather than growing a part per flush.
func straddlersDoNotAccumulate(t *testing.T, k Kind) {
	t.Helper()

	const flushes = 192

	ctx := context.Background()
	e := k.open(t, backend.Memory())
	peak := 0

	for i := range flushes {
		flushStraddler(t, e, 2*day+int64(i)*int64(30*time.Minute), i)
		require.NoError(t, e.Merge(ctx, 0))

		peak = max(peak, e.PartCount())
	}

	mergeToFixpoint(t, e)
	requireLadderFit(t, e)

	days := int(int64(flushes) * int64(30*time.Minute) / day)
	assert.LessOrEqual(t, peak, 4*(days+1), "%d flushes over %d days peaked at %d parts", flushes, days, peak)
}

// forceReachesStraddlers is the #450 escape: a part fitting no level was invisible to every group,
// forced or not, so a forced merge was a no-op on it.
func forceReachesStraddlers(t *testing.T, k Kind) {
	t.Helper()

	e := k.open(t, backend.Memory())
	for i := range 5 {
		flushStraddler(t, e, day+int64(i)*int64(time.Minute), i)
	}

	require.Equal(t, 5, e.MergeShape(0).ForceCandidates)
	require.NoError(t, e.ForceMerge(context.Background()))

	requireLadderFit(t, e)
	assert.Equal(t, 2, e.PartCount(), "five straddlers of two days split into one part per day")
	assert.Zero(t, e.MergeShape(0).ForceCandidates)
}

// retentionSplitsStraddler checks a retention rewrite of a straddler drops the expired rows and
// splits what survives on day boundaries.
func retentionSplitsStraddler(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	e := k.open(t, backend.Memory())

	e.Append(t, Row{Stream: staleStream, Ts: staleTs, Val: 1},
		Row{Stream: freshStream, Ts: day + hour, Val: 2}, Row{Stream: freshStream, Ts: 2*day + hour, Val: 3})
	require.NoError(t, e.Flush(ctx))
	require.NoError(t, e.Merge(ctx, day))

	requireLadderFit(t, e)
	assert.Equal(t, 2, e.PartCount())
	assert.Empty(t, rows(t, e, staleStream), "retention must drop the expired row")
	assert.Equal(t, []Row{
		{Stream: freshStream, Ts: day + hour, Val: 2},
		{Stream: freshStream, Ts: 2*day + hour, Val: 3},
	}, rows(t, e, freshStream))
}

// mergeConvergesWithStraddlers is the convergence property over random populations: in-bucket parts
// mixed with straddlers of up to four days reach a fixpoint holding no straddler and every row.
func mergeConvergesWithStraddlers(t *testing.T, k Kind) {
	t.Helper()

	for seed := range uint64(16) {
		rng := rand.New(rand.NewPCG(seed, seed+1)) //nolint:gosec // a reproducible test population, not a secret
		spans := make([][2]int64, 1+rng.IntN(16))

		for i := range spans {
			start := rng.Int64N(7 * day)
			if rng.IntN(3) == 0 {
				spans[i] = [2]int64{start, start + 1 + rng.Int64N(4*day)}

				continue
			}

			level := timebucket.Ladder[rng.IntN(len(timebucket.Ladder))]
			lo := timebucket.Of(start, level)
			spans[i] = [2]int64{lo + rng.Int64N(level/2), timebucket.End(lo, level) - rng.Int64N(level/2)}
		}

		e := k.open(t, backend.Memory())
		flushSpans(t, e, spans)
		want := storedRows(t, e, streamNames(len(spans))...)

		mergeToFixpoint(t, e)
		requireLadderFit(t, e)
		require.Equal(t, want, storedRows(t, e, streamNames(len(spans))...), "seed %d", seed)
	}
}
