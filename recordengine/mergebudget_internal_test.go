package recordengine

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/oteldb/storage/backend"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMergeDiskCapIndependentOfMemory(t *testing.T) {
	t.Parallel()

	small := New(Config{Schema: headTestSchema, MaxPartBytes: 64 << 20, MergeMemoryBytes: 1 << 20})
	large := New(Config{Schema: headTestSchema, MaxPartBytes: 64 << 20, MergeMemoryBytes: 1 << 30})
	assert.Equal(t, small.mergeCapBytes(context.Background()), large.mergeCapBytes(context.Background()), "resident memory must not size a disk part")
}

func TestMergeSelectionBoundsSourceCount(t *testing.T) {
	t.Parallel()

	parts := make([]*part, 100)
	for i := range parts {
		parts[i] = partOfSize(i, 1)
		parts[i].minTime, parts[i].maxTime = 100, 300
	}

	for _, tc := range []struct {
		name   string
		retain int64
		force  bool
	}{
		{"ordinary", 0, false},
		{"operator", 0, true},
		{"retention", 200, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := selectMergeParts(parts, tc.retain, 1<<30, tc.force)
			require.NotEmpty(t, got)
			assert.LessOrEqual(t, len(got), 16)
		})
	}
}

func TestCompressedLogsMergeBeyondDecodedCap(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	e := budgetLogEngine(t, backend.Memory(), 24)
	require.Len(t, e.parts, 24)
	for range 30 {
		_, err := e.MergeStep(ctx, MergeOptions{Background: true})
		require.NoError(t, err)
		if e.PartCount() == 1 {
			break
		}
	}
	require.Len(t, e.parts, 1, "compressed logs must grow beyond the old decoded-byte cap")
	assert.Greater(t, e.parts[0].rawBytes, int64(64<<20))
	assert.Less(t, e.parts[0].sizeBytes(), int64(1<<20))
	assert.Equal(t, int64(24*budgetLogRows), e.parts[0].rows())

	col, err := e.parts[0].reader.Column(ctx, "body")
	require.NoError(t, err)
	decoded, err := col.Bytes()
	require.NoError(t, err)
	require.Equal(t, 24*budgetLogRows, decoded.Len())
	for i := range decoded.Len() {
		require.Len(t, decoded.At(i), budgetLogBody)
		require.Equal(t, byte('a'+i%2), decoded.At(i)[0])
	}
}

func TestRecordRunScoring(t *testing.T) {
	t.Parallel()
	const mib = 1 << 20
	for _, tc := range []struct {
		name       string
		sizes      []int64
		cap        int64
		idle, want int
	}{
		{"balanced across old tiers", []int64{3 * mib, 4 * mib}, 64 * mib, 0, 2},
		{"geometric leftovers wait", []int64{mib, 16 * mib}, 64 * mib, 0, 0},
		{"geometric leftovers converge", []int64{mib, 16 * mib}, 64 * mib, mergeIdleRounds, 2},
		{"cheap spread merges immediately", []int64{1, 50_000}, 64 * mib, 0, 2},
		{"disk sum limits run", []int64{8 * mib, 8 * mib, 8 * mib}, 20 * mib, 0, 2},
		{"near cap is sealed", []int64{39 * mib, 39 * mib}, 64 * mib, mergeIdleRounds, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			parts := make([]*part, len(tc.sizes))
			for i, size := range tc.sizes {
				parts[i] = partOfSize(i, size)
				parts[i].rawBytes = 100 << 30
			}
			assert.Len(t, pickMergeRun(parts, tc.cap, tc.idle), tc.want, "logical bytes must not influence stored-byte selection")
		})
	}
}

func TestMergeBuffersIndependentOfDiskCap(t *testing.T) {
	t.Parallel()
	for _, budget := range []int64{1 << 20, 32 << 20, -1} {
		t.Run(fmt.Sprintf("memory=%d", budget), func(t *testing.T) {
			t.Parallel()
			e := New(Config{Schema: headTestSchema, MergeMemoryBytes: budget, MergeCeilingBytes: 1 << 20})
			window, run := e.mergeWindow(16), e.mergeRunBytes()
			e.cfg.MergeCeilingBytes = 16 << 30
			assert.Equal(t, window, e.mergeWindow(16))
			assert.Equal(t, run, e.mergeRunBytes())
			assert.LessOrEqual(t, window*16*3, e.mergeMemoryBudgetBytes()/2)
			assert.LessOrEqual(t, run, int64(1<<20))
		})
	}
}

func TestIdleWaiverSurvivesLateDeferral(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	e := budgetLogEngine(t, backend.Memory(), 2)
	e.cfg.MergeMemoryBytes = 1
	for i, p := range e.parts {
		m := p.reader.Manifest()
		m.Columns = slices.Clone(m.Columns)
		for k := range m.Columns {
			m.Columns[k].HasSizing = false
		}
		require.NoError(t, e.cfg.Backend.Write(ctx, p.prefix+"/manifest", m.Encode(nil)))
		reopened, err := openPart(ctx, e.cfg.Backend, e.cfg.Schema, p.prefix, e.cfg.Obs.Corruption)
		require.NoError(t, err)
		reopened.minTime, reopened.maxTime = p.minTime, p.maxTime
		e.parts[i] = reopened
	}
	calls := 0
	e.cfg.MergeAdmission = func(_ context.Context, _ int64, wait bool) (func(), bool, error) {
		require.False(t, wait)
		calls++
		if calls == 1 {
			return func() {}, true, nil
		}
		return nil, false, nil
	}
	e.idleMerges.Store(mergeIdleRounds)
	changed, err := e.MergeStep(ctx, MergeOptions{Background: true})
	require.NoError(t, err)
	assert.False(t, changed)
	require.True(t, e.MergeDeferred())
	require.Greater(t, calls, 1, "the source manifest must require a late grant top-up")
	assert.Equal(t, int64(mergeIdleRounds), e.idleMerges.Load(), "deferral must not disarm the idle waiver")
}
