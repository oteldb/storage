package engine

import (
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

func tierOf(agg signal.Aggregation) []DownsampleTier {
	return []DownsampleTier{{Before: 100, Interval: 10, Agg: agg}}
}

// sourceRun is a run with its source's recorded layout.
type sourceRun struct {
	run    tsRun
	layout []DownsampleTier
}

func rolledRun(ts []int64, vals, sf []float64, layout []DownsampleTier) sourceRun {
	return sourceRun{run: run(ts, vals, sf), layout: layout}
}

func rawRun(ts []int64, vals, sf []float64) sourceRun { return sourceRun{run: run(ts, vals, sf)} }

// TestFoldTie pins what one timestamp held by several sources collapses to.
func TestFoldTie(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		runs  []sourceRun
		v, w  float64
		agg   signal.Aggregation
		isRep bool
	}{
		{
			name: "raw duplicates: freshest wins",
			runs: []sourceRun{rawRun([]int64{10}, []float64{1}, nil), rawRun([]int64{10}, []float64{2}, []float64{3})},
			v:    2, w: 3,
		},
		{
			name: "two Sum representatives add",
			runs: []sourceRun{
				rolledRun([]int64{10}, []float64{4}, nil, tierOf(signal.AggSum)),
				rolledRun([]int64{10}, []float64{5}, nil, tierOf(signal.AggSum)),
			},
			v: 9, w: 1, agg: signal.AggSum, isRep: true,
		},
		{
			name: "a Count representative adds its count, a raw sample one",
			runs: []sourceRun{
				rolledRun([]int64{10}, []float64{7}, nil, tierOf(signal.AggCount)),
				rawRun([]int64{10}, []float64{123}, nil),
			},
			v: 8, w: 1, agg: signal.AggCount, isRep: true,
		},
		{
			name: "raw duplicates fold into a representative once",
			runs: []sourceRun{
				rolledRun([]int64{10}, []float64{7}, nil, tierOf(signal.AggCount)),
				rawRun([]int64{10}, []float64{1}, nil),
				rawRun([]int64{10}, []float64{1}, nil),
			},
			v: 8, w: 1, agg: signal.AggCount, isRep: true,
		},
		{
			name: "Avg representatives weigh by population",
			runs: []sourceRun{
				rolledRun([]int64{10}, []float64{2}, []float64{3}, tierOf(signal.AggAvg)),
				rolledRun([]int64{10}, []float64{6}, nil, tierOf(signal.AggAvg)),
			},
			v: 3, w: 4, agg: signal.AggAvg, isRep: true,
		},
		{
			name: "Min compares",
			runs: []sourceRun{
				rolledRun([]int64{12}, []float64{1}, nil, tierOf(signal.AggMin)),
				rawRun([]int64{12}, []float64{5}, nil),
			},
			v: 1, w: 1, agg: signal.AggMin, isRep: true,
		},
		{
			name: "Last takes the freshest",
			runs: []sourceRun{
				rolledRun([]int64{12}, []float64{1}, nil, tierOf(signal.AggLast)),
				rawRun([]int64{12}, []float64{5}, nil),
			},
			v: 5, w: 1, agg: signal.AggLast, isRep: true,
		},
		{
			name: "past the recorded cutoff a sample is raw",
			runs: []sourceRun{
				rolledRun([]int64{110}, []float64{4}, nil, tierOf(signal.AggSum)),
				rawRun([]int64{110}, []float64{5}, nil),
			},
			v: 5, w: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			runs := make([]tsRun, len(tc.runs))
			layouts := make([][]DownsampleTier, len(tc.runs))

			for i, r := range tc.runs {
				runs[i], layouts[i] = r.run, r.layout
			}

			cur := make([]int, len(runs))
			v, w, tag := foldTie(runs, layouts, cur, runs[0].ts[0])
			assert.InDelta(t, tc.v, v, 0)
			assert.InDelta(t, tc.w, w, 0)
			assert.Equal(t, tc.isRep, tag.interval > 0)

			if tc.isRep {
				assert.Equal(t, tc.agg, tag.agg)
			}

			for i := range cur {
				assert.Equal(t, 1, cur[i], "every tied run advances")
			}
		})
	}
}

// TestCollectTaggedMarksRepresentatives checks the tags a merge sees: a sample is a representative
// where its source's layout assigns a tier, raw otherwise, and a tie folds to one tagged sample.
func TestCollectTaggedMarksRepresentatives(t *testing.T) {
	t.Parallel()

	var m sampleMerge
	m.add([]int64{10, 20, 105}, []float64{3, 4, 5}, nil, tierOf(signal.AggCount), minInt64, maxInt64)
	m.add([]int64{10, 25}, []float64{9, 9}, nil, nil, minInt64, maxInt64)

	ts, vals, sf, tags := m.collectTagged()
	assert.Equal(t, []int64{10, 20, 25, 105}, ts)
	assert.Equal(t, []float64{4, 4, 9, 5}, vals)
	assert.Nil(t, sf)

	count := rollupTag{interval: 10, before: 100, agg: signal.AggCount}
	assert.Equal(t, []rollupTag{count, count, {}, {}}, tags)

	plain, plainVals, _ := m.collect(nil, nil)
	assert.Equal(t, ts, plain, "a read folds the same tie")
	assert.Equal(t, vals, plainVals)

	var raw sampleMerge
	raw.add([]int64{1, 2}, []float64{1, 2}, nil, nil, minInt64, maxInt64)

	var rawTags []rollupTag

	raw.gather(nil, nil, &rawTags)
	assert.Nil(t, rawTags, "no representative, no tags")
}

// TestRollSeriesUnknownIsRaw pins the rule for a source without a marker: its samples are raw to the
// fold, so a legacy Count representative beside a marked one counts as one sample.
func TestRollSeriesUnknownIsRaw(t *testing.T) {
	t.Parallel()

	var m sampleMerge
	m.add([]int64{10}, []float64{6}, nil, nil, minInt64, maxInt64)
	m.add([]int64{10, 30}, []float64{3, 2}, nil, tierOf(signal.AggCount), minInt64, maxInt64)

	ts, vals, _, _ := rollSeries(&m, tierOf(signal.AggCount))
	assert.Equal(t, []int64{10, 30}, ts)
	assert.Equal(t, []float64{4, 2}, vals)
}

// TestRollSeriesKeepsRecordedAgg checks a raw sample landing in a range rolled with Sum folds by Sum,
// and the same Agg rolls the range past the recorded cutoff.
func TestRollSeriesKeepsRecordedAgg(t *testing.T) {
	t.Parallel()

	recorded := []DownsampleTier{{Before: 30, Interval: 10, Agg: signal.AggSum}}

	var m sampleMerge
	m.add([]int64{10, 20}, []float64{5, 7}, nil, recorded, minInt64, maxInt64)
	m.add([]int64{12, 34, 36}, []float64{4, 1, 2}, nil, nil, minInt64, maxInt64)

	ts, vals, _, _ := rollSeries(&m, compactLayout(append(slices.Clone(recorded), tierOf(signal.AggSum)...)))
	assert.Equal(t, []int64{10, 20, 30}, ts)
	assert.Equal(t, []float64{9, 7, 3}, vals)
}

// TestRollSeriesNoTiersFoldsSharedBuckets checks a merge applying no tiers still folds a tie on a
// representative, and leaves a lone representative and every raw sample untouched. A merge whose
// sources hold raw samples in a rolled range applies the recorded layout instead ([planRollup]).
func TestRollSeriesNoTiersFoldsSharedBuckets(t *testing.T) {
	t.Parallel()

	var lone sampleMerge
	lone.add([]int64{10, 20, 105}, []float64{3, 4, 5}, nil, tierOf(signal.AggCount), minInt64, maxInt64)
	lone.add([]int64{33, 101}, []float64{9, 9}, nil, nil, minInt64, maxInt64)

	ts, vals, _, covered := rollSeries(&lone, nil)
	assert.Equal(t, []int64{10, 20, 33, 101, 105}, ts)
	assert.Equal(t, []float64{3, 4, 9, 9, 5}, vals)
	assert.Nil(t, covered)

	var split sampleMerge
	split.add([]int64{10, 20}, []float64{3, 4}, nil, tierOf(signal.AggCount), minInt64, maxInt64)
	split.add([]int64{10, 27}, []float64{2, 9}, nil, nil, minInt64, maxInt64)

	ts, vals, _, _ = rollSeries(&split, nil)
	assert.Equal(t, []int64{10, 20, 27}, ts)
	assert.Equal(t, []float64{4, 4, 9}, vals, "a tie folds")

	ts, vals, _, _ = rollSeries(&split, tierOf(signal.AggCount))
	assert.Equal(t, []int64{10, 20}, ts)
	assert.Equal(t, []float64{4, 5}, vals, "under the recorded layout a late sample folds too")
}

// checkRollupCombine splits raw samples across two parts rolled up on their own under fine and a
// third left raw, as late samples, then merges them under target and requires one rollup of every
// sample under target: exactly for the value-selecting aggregations and Count, within
// [regroupTolerance] for Sum and Avg. The merged result must itself be a fixed point.
func checkRollupCombine(t *testing.T, ts []int64, vals []float64, groups []uint8, fine, target []DownsampleTier) {
	t.Helper()

	if len(ts) == 0 {
		return
	}

	var src [3]struct {
		ts   []int64
		vals []float64
	}

	for i := range ts {
		g := groups[i] % 3
		src[g].ts = append(src[g].ts, ts[i])
		src[g].vals = append(src[g].vals, vals[i])
	}

	var m sampleMerge

	for g := range 2 {
		rts, rvals, rsf := downsample(src[g].ts, src[g].vals, nil, fine)
		m.add(rts, rvals, rsf, fine, minInt64, maxInt64)
	}

	m.add(src[2].ts, src[2].vals, nil, nil, minInt64, maxInt64)

	gotTs, gotVals, gotSF, _ := rollSeries(&m, target)
	wantTs, wantVals, wantSF := downsample(ts, vals, nil, target)

	tol := 0.0
	if agg := target[0].Agg; agg == signal.AggSum || agg == signal.AggAvg {
		tol = regroupTolerance(vals, nil)
	}

	require.Equal(t, wantTs, gotTs, "timestamps")
	require.Equal(t, wantSF, gotSF, "weights")
	requireSameFloats(t, wantVals, gotVals, tol, "values")

	var again sampleMerge
	again.add(gotTs, gotVals, gotSF, target, minInt64, maxInt64)

	ts2, vals2, sf2, _ := rollSeries(&again, target)
	require.Equal(t, gotTs, ts2, "fixed point")
	require.Equal(t, gotSF, sf2, "fixed point")
	requireSameFloats(t, gotVals, vals2, 0, "fixed point")
}

func combineTiers(agg signal.Aggregation, before, interval int64, coarsen uint8) (fine, target []DownsampleTier) {
	fine = []DownsampleTier{{Before: before, Interval: interval, Agg: agg}}
	if coarsen%3 == 0 || interval > 1<<40 {
		return fine, fine
	}

	return fine, append(slices.Clone(fine), DownsampleTier{Before: before, Interval: interval * int64(1+coarsen%3), Agg: agg})
}

// TestRollupCombineMatchesOneRollup runs [checkRollupCombine] over random series for every Agg.
func TestRollupCombineMatchesOneRollup(t *testing.T) {
	t.Parallel()

	rng := rand.New(rand.NewPCG(7, 26))

	for iter := range 400 {
		agg := allAggs[iter%len(allAggs)]

		n := rng.IntN(60)
		ts := make([]int64, 0, n)
		vals := make([]float64, 0, n)
		groups := make([]uint8, 0, n)

		for t0 := int64(rng.IntN(5)); len(ts) < n; t0 += 1 + rng.Int64N(4) {
			ts = append(ts, t0)
			vals = append(vals, float64(rng.IntN(200)-100))
			groups = append(groups, uint8(rng.IntN(3)))
		}

		fine, target := combineTiers(agg, int64(rng.IntN(200)), int64(1+rng.IntN(12)), uint8(iter))

		t.Run(fmt.Sprintf("%d/%s", iter, agg), func(t *testing.T) {
			t.Parallel()
			checkRollupCombine(t, ts, vals, groups, fine, target)
		})
	}
}

// FuzzRollupCombine is [TestRollupCombineMatchesOneRollup] over fuzzed series: values span twenty
// decades and both signs, so sums cancel, and include NaN and ±Inf.
func FuzzRollupCombine(f *testing.F) {
	f.Add([]byte{0, 1, 2, 10, 11, 20, 21, 30}, uint64(0b100110), int64(100), int64(10), uint8(4), uint8(0))
	f.Add([]byte{0, 5, 10, 15, 20, 25}, uint64(0b011000), int64(22), int64(5), uint8(6), uint8(1))
	f.Add([]byte{3, 4, 9, 12, 40, 41}, uint64(0b101001), int64(50), int64(7), uint8(5), uint8(2))

	f.Fuzz(func(t *testing.T, raw []byte, groupBits uint64, before, interval int64, aggByte, coarsen uint8) {
		if interval <= 0 {
			return
		}

		seen := map[int64]struct{}{}

		var (
			ts     []int64
			vals   []float64
			groups []uint8
		)

		for i, b := range raw {
			v := int64(b)
			if _, ok := seen[v]; ok {
				continue
			}

			seen[v] = struct{}{}
			ts = append(ts, v)
			groups = append(groups, uint8(groupBits>>(2*(i%32))))

			val := float64(int8(b*37)) * math.Pow10(i%20)
			switch b % 23 {
			case 0:
				val = math.NaN()
			case 1:
				val = math.Inf(1)
			case 2:
				val = math.Inf(-1)
			}

			vals = append(vals, val)
		}

		order := make([]int, len(ts))
		for i := range order {
			order[i] = i
		}

		slices.SortFunc(order, func(a, b int) int { return int(ts[a] - ts[b]) })

		sortedTs := make([]int64, len(ts))
		sortedVals := make([]float64, len(ts))
		sortedGroups := make([]uint8, len(ts))

		for i, j := range order {
			sortedTs[i], sortedVals[i], sortedGroups[i] = ts[j], vals[j], groups[j]
		}

		fine, target := combineTiers(signal.Aggregation(aggByte%7), before, interval, coarsen)
		checkRollupCombine(t, sortedTs, sortedVals, sortedGroups, fine, target)
	})
}

// TestReadTieMixedAggsIndependentOfOrder: parts that disagree on the Agg (only a quarantined part
// can) must read the same whatever order they arrive in, and whatever merges combine meanwhile. Here
// A and C hold Sum representatives and B a Count one at the same bucket start. A merge takes A and C
// together (never B, which records another Agg), and the merged part lands after B.
func TestReadTieMixedAggsIndependentOfOrder(t *testing.T) {
	t.Parallel()

	sum, count := tierOf(signal.AggSum), tierOf(signal.AggCount)

	type source struct {
		ts     []int64
		vals   []float64
		layout []DownsampleTier
	}

	a := source{[]int64{10, 20}, []float64{5, 7}, sum}
	b := source{[]int64{10, 22}, []float64{3, 4}, count}
	c := source{[]int64{10, 30}, []float64{2, 1}, sum}

	read := func(srcs ...source) ([]int64, []float64) {
		var m sampleMerge
		for _, s := range srcs {
			m.add(s.ts, s.vals, nil, s.layout, minInt64, maxInt64)
		}

		ts, vals, _ := m.collect(nil, nil)

		return ts, vals
	}

	wantTs, wantVals := read(a, b, c)
	assert.Equal(t, []int64{10, 20, 22, 30}, wantTs)
	assert.Equal(t, []float64{7, 7, 4, 1}, wantVals, "Sum, the smaller Agg, folds A and C; B's Count is left out")

	for _, order := range [][]source{{a, c, b}, {b, a, c}, {b, c, a}, {c, a, b}, {c, b, a}} {
		ts, vals := read(order...)
		assert.Equal(t, wantTs, ts)
		assert.Equal(t, wantVals, vals)
	}

	var m sampleMerge
	m.add(a.ts, a.vals, nil, sum, minInt64, maxInt64)
	m.add(c.ts, c.vals, nil, sum, minInt64, maxInt64)

	acTs, acVals, _, _ := rollSeries(&m, sum)
	ac := source{acTs, acVals, sum}

	for _, order := range [][]source{{b, ac}, {ac, b}} {
		ts, vals := read(order...)
		assert.Equal(t, wantTs, ts, "after merging A and C")
		assert.Equal(t, wantVals, vals, "after merging A and C")
	}
}
