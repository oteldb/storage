package enginetest

import (
	"cmp"
	"context"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
)

// retainAt is the retention cutoff the forced-cap tests merge under: every part they flush holds one
// row before it and the rest after, all inside the hour starting at [hour].
const retainAt = hour + int64(30*time.Minute)

// flushForced flushes one part per entry of rowsPer into one hour bucket, part i holding the expired
// row hour+first+i and rowsPer[i] rows retention keeps.
func flushForced(t *testing.T, e Engine, first int, rowsPer ...int) {
	t.Helper()

	for i, n := range rowsPer {
		idx := int64(first + i)
		batch := []Row{api(hour+idx, -idx-1)}

		for j := range int64(n) {
			ts := retainAt + j*int64(time.Second) + idx*int64(time.Millisecond)
			batch = append(batch, api(ts, ts))
		}

		e.Append(t, batch...)
		require.NoError(t, e.Flush(context.Background()))
	}
}

// reopenCapped opens a second engine over be with the merge cap set, and loads what the first
// flushed.
func reopenCapped(t *testing.T, k Kind, be backend.Backend, capBytes int64) Engine {
	t.Helper()

	e := k.Open(t, Config{Backend: be, MergeCapBytes: capBytes})
	require.NoError(t, e.LoadParts(context.Background()))

	return e
}

// keptRows returns the api rows retainAt keeps, by timestamp, and how many it drops.
func keptRows(t *testing.T, e Engine) (kept []Row, dropped int) {
	t.Helper()

	for _, r := range rows(t, e, apiStream) {
		if r.Ts < retainAt {
			dropped++

			continue
		}

		kept = append(kept, r)
	}

	slices.SortFunc(kept, func(a, b Row) int { return cmp.Compare(a.Ts, b.Ts) })

	return kept, dropped
}

// mergeForcedWithinCap merges under retainAt until no part holds an expired row, failing when a
// merge of several parts exceeds capBytes, when a cycle consumes nothing, or when the candidate count
// stops matching what the next merge takes. It returns the cycles taken and the first one's inputs.
func mergeForcedWithinCap(t *testing.T, e Engine, capBytes int64) (cycles int, first []Part) {
	t.Helper()

	ctx := context.Background()

	for cycle := range convergeCycles {
		before := e.Parts()
		if expired(before) == 0 {
			return cycle, first
		}

		sh := e.MergeShape(retainAt)
		assert.Positive(t, sh.Candidates, "cycle %d: forced parts a cap deferred are still due", cycle)

		require.NoError(t, e.Merge(ctx, retainAt))

		consumed := removedParts(before, e.Parts())
		require.NotEmpty(t, consumed, "cycle %d made no progress", cycle)

		if cycle == 0 {
			first = consumed
		} else {
			// The first merge is what reports the cap in effect to an engine that derives it per merge.
			assert.Len(t, consumed, sh.Candidates, "cycle %d: the candidate count is what the merge takes", cycle)
		}

		if len(consumed) > 1 {
			var total int64
			for _, p := range consumed {
				total += p.Bytes
			}

			assert.LessOrEqual(t, total, capBytes, "cycle %d merged %d parts past the cap", cycle, len(consumed))
		}
	}

	require.FailNow(t, "forced work did not converge", "%d cycles", convergeCycles)

	return 0, nil
}

func expired(parts []Part) int {
	n := 0

	for _, p := range parts {
		if p.MinTime < retainAt {
			n++
		}
	}

	return n
}

func removedParts(before, after []Part) []Part {
	live := make(map[string]struct{}, len(after))
	for _, p := range after {
		live[p.ID] = struct{}{}
	}

	var out []Part

	for _, p := range before {
		if _, ok := live[p.ID]; !ok {
			out = append(out, p)
		}
	}

	return out
}

func largestPart(parts []Part) int64 {
	var n int64
	for _, p := range parts {
		n = max(n, p.Bytes)
	}

	return n
}

// forcedBacklogStaysWithinCap is #705: retention forces every part of one bucket, far more than the
// cap. Each merge must stay within the cap, the rest waiting for later cycles and still counted as
// due, and the backlog must drain with every surviving row kept.
func forcedBacklogStaysWithinCap(t *testing.T, k Kind) {
	t.Helper()

	const parts = 12

	be := backend.Memory()
	w := k.open(t, be)

	rowsPer := make([]int, parts)
	for i := range rowsPer {
		rowsPer[i] = 40
	}

	flushForced(t, w, 0, rowsPer...)
	want, _ := keptRows(t, w)

	capBytes := 3*largestPart(w.Parts()) + largestPart(w.Parts())/2
	e := reopenCapped(t, k, be, capBytes)
	require.Equal(t, parts, expired(e.Parts()))

	cycles, first := mergeForcedWithinCap(t, e, capBytes)

	assert.Less(t, len(first), parts, "the cap must spread the backlog over several merges")
	requireKept(t, e, want)

	t.Logf("%d forced parts under a %d B cap drained in %d cycles, first merge took %d", parts, capBytes, cycles, len(first))
}

// oversizedForcedPartProgresses checks the oldest forced part is rewritten even when it alone
// exceeds the cap, and that the parts behind it still drain.
func oversizedForcedPartProgresses(t *testing.T, k Kind) {
	t.Helper()

	be := backend.Memory()
	w := k.open(t, be)

	flushForced(t, w, 0, 1000)
	big := w.Parts()[0]

	flushForced(t, w, 1, 20, 20, 20, 20)
	want, _ := keptRows(t, w)

	var small int64
	for _, p := range w.Parts() {
		if p.ID != big.ID {
			small = max(small, p.Bytes)
		}
	}

	capBytes := 2*small + small/2
	require.Greater(t, big.Bytes, capBytes, "the oldest part must not fit the cap")

	e := reopenCapped(t, k, be, capBytes)
	_, first := mergeForcedWithinCap(t, e, capBytes)

	require.Len(t, first, 1)
	assert.Equal(t, big.ID, first[0].ID, "the oldest forced part goes first, alone")
	requireKept(t, e, want)
}

// requireKept fails unless e holds exactly want and no row retention drops.
func requireKept(t *testing.T, e Engine, want []Row) {
	t.Helper()

	got, dropped := keptRows(t, e)
	require.Zero(t, dropped, "retention must drop every expired row")
	require.Equal(t, want, got)
}
