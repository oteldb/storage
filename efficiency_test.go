package storage

// Efficiency regression gates. Unlike the benchmarks in density_bench_test.go (which *measure*),
// these tests *fail* when efficiency regresses past a budget. They guard the two properties that
// are deterministic enough to assert hard ceilings on:
//
//   - On-disk density (bytes/point) — fully reproducible: fixed seed, fixed codecs, in-memory
//     backend, byte-exact. The ceilings below are measured headroom over the current numbers;
//     they catch a codec or part-format regression that bloats storage, while still passing when
//     a change *improves* density (the assertion is an upper bound).
//   - Hot-path allocations (ingest steady state, fetch per row) — measured with
//     testing.AllocsPerRun, which disables the GC and averages over several runs, so the counts
//     are stable. These are ratchets: when the read/write path gets leaner, tighten the ceiling.
//
// Throughput (ns/op, points/sec) is deliberately NOT gated here — it is machine-dependent and
// would flake in CI. Time-based numbers live in the benchmarks; correctness-of-efficiency lives
// here.
//
// When a ceiling legitimately needs to move (a new part column, a new index), update the budget
// in the same change and say why — that conscious step is the point of the gate.

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/query/fetch"
)

// densityBudget is the per-profile upper bound on bytes/point: maxParts for the value columns
// alone, maxTotal including the identity/index overhead. Keyed by corpusProfile.name.
type densityBudget struct {
	maxParts, maxTotal float64
}

// densityBudgets sets a ceiling for every densityProfile. Numbers are the measured B/point plus
// ~10% headroom (rounded), so an unrelated change does not flake but a real regression trips.
// The constant-value profile is held to the project's 0.4–0.8 B/point target on the value parts;
// the rest document where each value shape currently sits (and how far the codecs still have to
// go on the random-walk / noisy cases).
var densityBudgets = map[string]densityBudget{
	"counter_200k":        {maxParts: 1.6, maxTotal: 3.5},  // adaptive decimal codec: integer-valued ⇒ ~1.5
	"gauge_randwalk_200k": {maxParts: 9.2, maxTotal: 11.0}, // full-entropy doubles: near the lossless floor (Gorilla)
	"gauge_bounded_200k":  {maxParts: 1.6, maxTotal: 3.5},  // realistic 1-decimal gauge ⇒ adaptive decimal ~1.5
	"gauge_constant_200k": {maxParts: 0.8, maxTotal: 2.5},  // parts held to the B/point target
	"gauge_noisy_200k":    {maxParts: 8.4, maxTotal: 10.2},
	"wide_shallow_100k":   {maxParts: 27.0, maxTotal: 120.0},
}

// TestDensityBudget gates on-disk bytes/point against densityBudgets for every density profile,
// failing if any value shape regresses past its ceiling. Fully deterministic.
func TestDensityBudget(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("density budget ingests sizable corpora; skipped under -short")
	}

	for _, p := range densityProfiles {
		budget, ok := densityBudgets[p.name]
		require.Truef(t, ok, "no density budget for profile %q — add one when adding a profile", p.name)

		r := measureDensity(t, p)

		require.LessOrEqualf(t, r.bppParts, budget.maxParts,
			"%s: value parts %.3f B/point exceeds budget %.3f (total %d B over %d points)",
			p.name, r.bppParts, budget.maxParts, r.totalBytes, r.points)
		require.LessOrEqualf(t, r.bppTotal, budget.maxTotal,
			"%s: total %.3f B/point exceeds budget %.3f (index %d B)",
			p.name, r.bppTotal, budget.maxTotal, r.indexBytes)
	}
}

// maxIngestAllocsPer100 bounds steady-state allocations for one WriteMetrics of a warm
// 100-series batch. The per-point append is meant to be allocation-free; the residual is fixed
// per-call/per-series projection + identity work. Current ≈ 59; the ceiling leaves headroom for
// the amortized per-series buffer growth without admitting a per-point allocation regression.
const maxIngestAllocsPer100 = 80.0

// TestIngestAllocBudget guards the zero-alloc ingest hot path: a warm re-append of a fixed batch
// must not allocate more than maxIngestAllocsPer100. A regression to even one alloc per point
// (100 points ⇒ +100 allocs) trips this immediately.
//
//nolint:paralleltest // AllocsPerRun reads process-global malloc counters; parallel tests corrupt it.
func TestIngestAllocBudget(t *testing.T) {
	ctx := context.Background()

	s, err := InMemory()
	require.NoError(t, err)
	defer func() { require.NoError(t, s.Close(ctx)) }()

	md := benchBatch(100, 1) // 100 series, 1 point each
	_, err = s.WriteMetrics(ctx, md)
	require.NoError(t, err) // warm: register the series so steady-state appends are measured

	allocs := testing.AllocsPerRun(20, func() {
		if _, err := s.WriteMetrics(ctx, md); err != nil {
			t.Fatal(err)
		}
	})

	require.LessOrEqualf(t, allocs, maxIngestAllocsPer100,
		"steady-state ingest allocated %.0f allocs for 100 points (%.3f/point); budget %.0f",
		allocs, allocs/100, maxIngestAllocsPer100)
}

// maxFetchAllocsPerRow ratchets the per-row allocation of the fetch path. After the decode-once
// fix (each part decodes a single time per fetch, not once per matched series) this sits at
// ≈ 0.08; the ceiling locks that win in. Tighten it further whenever the read path gets leaner.
const maxFetchAllocsPerRow = 0.10

// TestFetchAllocBudget guards fetch-path allocations per row: select an entire metric over its
// full range, drain it, and assert allocations-per-row stay under the ratchet ceiling.
//
//nolint:paralleltest // AllocsPerRun reads process-global malloc counters; parallel tests corrupt it.
func TestFetchAllocBudget(t *testing.T) {
	if testing.Short() {
		t.Skip("fetch alloc budget builds and scans a corpus; skipped under -short")
	}

	const series, points = 500, 100
	rows := float64(series * points)

	ctx := context.Background()
	s, err := Open(ctx, Options{}, WithBackend(backend.Memory()))
	require.NoError(t, err)
	defer func() { require.NoError(t, s.Close(ctx)) }()

	_, err = s.WriteMetrics(ctx, buildCorpus(corpusProfile{
		name: "fetch_alloc", series: series, points: points,
		interval: 15_000_000_000, pattern: patRandWalk,
	}, 1))
	require.NoError(t, err)

	eng := mustEngine(s.engineFor("default"))
	require.NoError(t, eng.Flush(ctx))
	require.NoError(t, eng.Merge(ctx, 0))

	req := fetch.Request{Start: 0, End: 1 << 62, Matchers: []fetch.Matcher{nameMatcher("bench.metric")}}
	f := s.Fetcher("default")

	allocs := testing.AllocsPerRun(5, func() {
		it, err := f.Fetch(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		for {
			batch, err := it.Next(ctx)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				t.Fatal(err)
			}
			_ = batch
		}
		if err := it.Close(); err != nil {
			t.Fatal(err)
		}
	})

	require.LessOrEqualf(t, allocs/rows, maxFetchAllocsPerRow,
		"fetch allocated %.0f allocs for %.0f rows (%.4f/row); ratchet %.4f/row",
		allocs, rows, allocs/rows, maxFetchAllocsPerRow)
}
