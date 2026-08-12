package engine

import (
	"maps"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

// benchSeries builds a churn-shaped identity: a shared resource and scope, a per-series instance
// label — the shape whose identity outlives its data.
func benchSeries(i int) signal.Series {
	return signal.Series{
		Resource: signal.Resource{Attributes: signal.NewAttributes(
			signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte("api"))},
		)},
		Scope: signal.Scope{Name: []byte("lib"), Version: []byte("1.0")},
		Attributes: signal.NewAttributes(
			signal.KeyValue{Key: []byte("__name__"), Value: signal.StringValue([]byte("http_requests_total"))},
			signal.KeyValue{Key: []byte("instance"), Value: signal.StringValue([]byte(strconv.Itoa(i)))},
			signal.KeyValue{Key: []byte("code"), Value: signal.StringValue([]byte(strconv.Itoa(200 + i%5)))},
		),
	}
}

// headWith returns a head holding n registered identities, and the id set of the first live of them.
func headWith(n, live int) (*head, map[signal.SeriesID]struct{}) {
	h := newHead()
	ids := make(map[signal.SeriesID]struct{}, live)

	for i := range n {
		s := benchSeries(i)
		id := s.Hash()
		h.registerSeries(s)
		h.seriesNewest[id] = int64(i)

		if i < live {
			ids[id] = struct{}{}
		}
	}

	return h, ids
}

func TestRebuildIdentity(t *testing.T) {
	t.Parallel()

	h, live := headWith(100, 10)
	h.newest = 1234
	h.bytes = 4096

	out := rebuildIdentity(h.series.Snapshot(), live)

	assert.Equal(t, 10, out.series.Len(), "only the live identities are carried over")
	assert.True(t, out.indexSorted(), "the new index is sorted before it is published")
	assert.False(t, out.series.Has(benchSeries(99).Hash()))
	require.Len(t, out.resolve(nil), 10, "the postings resolve the survivors and nothing else")
}

func TestRebuildIdentitySkipsUnknownIdentity(t *testing.T) {
	t.Parallel()

	h, live := headWith(10, 10)
	live[signal.SeriesID{Hi: 1, Lo: 2}] = struct{}{} // an id no identity backs (a lost identity object)

	out := rebuildIdentity(h.series.Snapshot(), live)

	assert.Equal(t, 10, out.series.Len(), "an unresolvable id is skipped, not invented")
}

// TestHeadSwapIdentity covers the swap, which has to reconcile what changed while the rebuild ran
// off-lock: a newly registered series (past the snapshot's end) and a dead one that regained
// samples without re-registering, since the old index still held its identity.
func TestHeadSwapIdentity(t *testing.T) {
	t.Parallel()

	h, live := headWith(100, 10)
	snap := h.series.Snapshot()
	dead := deadPositions(snap, live)
	require.Len(t, dead, 90)

	rebuilt := rebuildIdentity(snap, live)

	// Meanwhile: one brand-new series registers, and one the prune considers dead starts buffering
	// samples again (no re-registration — the old index still has its identity).
	fresh := benchSeries(1000)
	h.registerSeries(fresh)

	revived := benchSeries(50).Hash()
	h.samples[revived] = &sampleBuf{ts: []int64{1}, values: []float64{1}}

	tail := h.series.Snapshot()[len(snap):]
	require.Len(t, tail, 1)

	h.swapIdentity(rebuilt, snap, tail, dead)

	assert.Equal(t, 12, h.series.Len(), "survivors, the new registration and the revived series")
	assert.True(t, h.series.Has(fresh.Hash()), "a series registered mid-rebuild is kept")
	assert.True(t, h.series.Has(revived), "a series that regained samples mid-rebuild is kept")
	assert.False(t, h.series.Has(benchSeries(99).Hash()), "a series that stayed dead is gone")
	assert.Len(t, h.resolve(nil), 12)

	_, kept := h.seriesNewest[revived]
	assert.True(t, kept, "the revived series keeps its lateness bound")
	_, dropped := h.seriesNewest[benchSeries(99).Hash()]
	assert.False(t, dropped, "a pruned series' watermark goes with it")
	assert.Len(t, h.seriesNewest, 11, "10 survivors + the revived one")
}

// BenchmarkRebuildIdentity measures the identity prune's dominant cost: re-interning every
// surviving identity into fresh symbol/series/postings structures. It runs off the engine lock
// (against the series index's append-only snapshot) precisely because of what this reports — under
// the lock it would stall ingest for the whole duration.
// BenchmarkSwapIdentity measures what the prune actually holds the engine lock for: replaying what
// registered during the rebuild and forgetting the dead series' watermarks. It is O(dead), not
// O(cardinality) — the rebuild itself (see BenchmarkRebuildIdentity) is off-lock.
func BenchmarkSwapIdentity(b *testing.B) {
	for _, live := range []int{200_000, 1_000_000} {
		b.Run(strconv.Itoa(live/1000)+"k", func(b *testing.B) {
			h, ids := headWith(live*5/4, live)
			snap := h.series.Snapshot()
			dead := deadPositions(snap, ids)
			rebuilt := rebuildIdentity(snap, ids)
			watermarks := maps.Clone(h.seriesNewest)

			b.ReportAllocs()

			for b.Loop() {
				// The swap deletes as it goes, so each iteration starts from a full watermark map —
				// otherwise every run after the first would measure an already-pruned one.
				b.StopTimer()

				h.seriesNewest = maps.Clone(watermarks)

				b.StartTimer()
				h.swapIdentity(rebuilt, snap, nil, dead)
			}
		})
	}
}

func BenchmarkRebuildIdentity(b *testing.B) {
	for _, live := range []int{200_000, 1_000_000} {
		b.Run(strconv.Itoa(live/1000)+"k", func(b *testing.B) {
			// A quarter dead — the smallest set the prune's gate acts on, so the rebuild is at its
			// most expensive relative to what it reclaims.
			h, ids := headWith(live*5/4, live)

			b.ReportAllocs()
			b.ResetTimer()

			snap := h.series.Snapshot()

			for b.Loop() {
				out := rebuildIdentity(snap, ids)
				b.SetBytes(out.identityBytes())
			}
		})
	}
}
