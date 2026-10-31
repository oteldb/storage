package recordengine

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/encoding/compress"
)

func TestMergeFeedbackInvalidation(t *testing.T) {
	t.Parallel()
	a, b, c := partOfSize(0, 10), partOfSize(1, 10), partOfSize(2, 10)
	profile := mergeProfile{memory: 32 << 20}
	for _, tc := range []struct {
		name        string
		diskLimited bool
		capBytes    int64
		memory      int64
		want        int
	}{
		{"same settings", false, 100, profile.memory, 2},
		{"larger disk cap cannot fix resident splitting", false, 200, profile.memory, 2},
		{"more memory", false, 100, 128 << 20, 1},
		{"disk split unchanged", true, 100, profile.memory, 2},
		{"disk split larger cap", true, 200, profile.memory, 1},
		{"disk split unlimited cap", true, 0, profile.memory, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var feedback mergeFeedback
			feedback.record([]*part{a, b}, profile, 100, []*part{{mergeDiskSealed: tc.diskLimited}, {}})
			current := profile
			current.memory = tc.memory
			assert.Equal(t, tc.want, feedback.expected([]*part{b, a}, current, tc.capBytes), "input order must not affect feedback")
			assert.Equal(t, 1, feedback.expected([]*part{a, c}, current, tc.capBytes), "a different group remains untried")
			feedback.prune([]*part{a, c})
			assert.Empty(t, feedback.runs, "retired inputs must release their feedback references")
			feedback.record([]*part{a, b}, profile, 100, []*part{a, b})
			current.level = compress.LevelBest
			assert.Equal(t, 1, feedback.expected([]*part{a, b}, current, tc.capBytes), "a new compression profile can retry")
			feedback.record([]*part{a, b}, current, tc.capBytes, []*part{a, b})
			assert.Len(t, feedback.runs, 1, "a new trial replaces the old profile observation")
		})
	}
}

func TestMergeFeedbackAllowsOtherWork(t *testing.T) {
	t.Parallel()
	e := New(Config{Schema: headTestSchema, MergeMemoryBytes: -1})
	a, b, c, d := partOfSize(0, 1000), partOfSize(1, 1000), partOfSize(2, 1000), partOfSize(3, 1000)
	a.minTime, a.maxTime, b.minTime, b.maxTime = 1, 2, 3, 4
	c.minTime, c.maxTime, d.minTime, d.maxTime = 2*hour, 2*hour+1, 2*hour+2, 2*hour+3
	e.mergeFeedback.record([]*part{a, b}, e.mergeProfile(), 0, []*part{a, b})
	assert.ElementsMatch(t, []*part{c, d}, e.selectMergeParts([]*part{a, b, c, d}, MergeOptions{Force: true}, 0))
	assert.Empty(t, e.selectMergeParts([]*part{a, b}, MergeOptions{Force: true}, 0))
	assert.NotEmpty(t, e.selectMergeParts([]*part{a, b}, MergeOptions{RetainFrom: 2, Force: true}, 0), "retention bypasses ordinary feedback")
	c.minTime, c.maxTime = 1, 2*streamedDay
	assert.Equal(t, []*part{c}, e.selectMergeParts([]*part{a, b, c}, MergeOptions{Force: true}, 0), "an old failed group must not starve straddlers")
	d.minTime, d.maxTime = 5, 6
	assert.NotEmpty(t, e.selectMergeParts([]*part{a, b, d}, MergeOptions{Force: true}, 0), "new inputs remain eligible")
	require.NoError(t, e.Reset(t.Context()))
	assert.Empty(t, e.mergeFeedback.runs)
}

func TestRecordMergeCostPerRemovedPart(t *testing.T) {
	t.Parallel()
	const mib = 1 << 20
	parts := []*part{partOfSize(0, mib), partOfSize(1, mib), partOfSize(2, 10*mib), partOfSize(3, 10*mib), partOfSize(4, 10*mib)}
	assert.Equal(t, parts[:2], pickMergeRun(parts, 0, 0), "the cheapest part removal wins over the largest multiplier")
	outputCount := func(run []*part) int {
		if len(run) == 2 && run[0] == parts[0] {
			return 2
		}
		return 1
	}
	selected := pickMergeRunExpected(parts, 0, 0, outputCount)
	assert.Equal(t, parts[2:], selected, "a measured non-reducing run must give way")
}

func TestUsefulMergeTransformations(t *testing.T) {
	t.Parallel()
	e := compressionEngine(t, []int64{1, 2, 3})
	raw := e.parts[0]
	changed, err := e.MergeStep(t.Context(), MergeOptions{})
	require.NoError(t, err)
	require.True(t, changed)
	compressed := e.parts[0]
	assert.True(t, e.usefulMerge([]*part{raw}, []*part{compressed}), "compression can preserve part count")
	assert.False(t, e.usefulMerge([]*part{compressed}, []*part{compressed}))
	assert.True(t, e.usefulMerge([]*part{compressed, compressed}, []*part{compressed}))
	e.cfg.MergeCompression = compress.AlgorithmNone
	straddler := &part{minTime: compressed.minTime, maxTime: 2 * streamedDay, ranges: compressed.ranges}
	assert.True(t, e.usefulMerge([]*part{straddler}, []*part{compressed, compressed}), "time normalization may increase part count")
	retained := &part{reader: compressed.reader, ranges: []streamRange{{rowRange: rowRange{start: 0, end: 1}}}}
	assert.True(t, e.usefulMerge([]*part{compressed}, []*part{retained}), "partial retention may preserve part count")
	manifest := compressed.reader.Manifest()
	manifest.Columns = slices.Clone(manifest.Columns)
	for i := range manifest.Columns {
		manifest.Columns[i].HasSizing = false
	}
	require.NoError(t, e.cfg.Backend.Write(t.Context(), compressed.prefix+"/manifest", manifest.Encode(nil)))
	legacy, err := openPart(t.Context(), e.cfg.Backend, e.cfg.Schema, compressed.prefix, e.cfg.Obs.Corruption)
	require.NoError(t, err)
	legacy.minTime, legacy.maxTime = compressed.minTime, compressed.maxTime
	assert.True(t, e.usefulMerge([]*part{legacy}, []*part{compressed}), "legacy sizing metadata can be upgraded without reducing parts")
}
