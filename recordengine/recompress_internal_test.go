package recordengine

import (
	"context"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/compress"
	"github.com/oteldb/storage/signal"
)

func compressionEngine(t *testing.T, times ...[]int64) *Engine {
	t.Helper()
	e := New(Config{Schema: headTestSchema, Backend: backend.Memory(), Prefix: "t/compress",
		MergeMemoryBytes: -1, MergeCompression: compress.AlgorithmZSTD, MergeCompressionLevel: compress.LevelBest})
	series := signal.Series{}
	for _, ts := range times {
		b := &Batch{Stream: series.Hash(), Identity: func() signal.Series { return series }, Ts: ts,
			Ints: [][]int64{make([]int64, len(ts))}, Bytes: [][][]byte{make([][]byte, len(ts))}}
		for i := range ts {
			b.Ints[0][i] = int64(i)
			b.Bytes[0][i] = fmt.Appendf(nil, "log body row %d", i)
		}
		_, err := e.AppendBatch(b, AppendLimits{})
		require.NoError(t, err)
		require.NoError(t, e.Flush(context.Background()))
	}
	return e
}

func TestMergeCompressesRestingRecords(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		seal bool
		ts   []int64
	}{
		{"singleton", false, []int64{1, 2, 3}},
		{"sealed singleton", true, []int64{1, 2, 3}},
		{"constant columns", false, []int64{1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := compressionEngine(t, tc.ts)
			if tc.seal {
				e.mergeCap = e.parts[0].sizeBytes()
			}
			before := e.PartPrefixes()
			want := mergedRows(t, e.parts)
			assert.Equal(t, 1, e.MergeShape().Candidates)
			require.NoError(t, e.Merge(context.Background(), 0))
			assert.NotEqual(t, before, e.PartPrefixes(), "a resting part still needs compression")
			assert.Equal(t, want, mergedRows(t, e.parts))
			for _, p := range e.parts {
				for _, c := range p.reader.Manifest().Columns {
					if !c.Const {
						assert.Equal(t, compress.AlgorithmZSTD, c.Compress)
						assert.Equal(t, compress.LevelBest, c.Level)
					}
				}
			}
			after := e.PartPrefixes()
			assert.Zero(t, e.MergeShape().Candidates)
			require.NoError(t, e.Merge(context.Background(), 0))
			assert.Equal(t, after, e.PartPrefixes(), "compression must reach a fixed point")
		})
	}
}

func TestCompressionRewriteCountBound(t *testing.T) {
	t.Parallel()
	times := make([][]int64, maxTierParts+5)
	for i := range times {
		times[i] = []int64{1, 2 * hour}
	}
	e := compressionEngine(t, times...)
	assert.Equal(t, maxTierParts, e.MergeShape().Candidates)
	want := mergedRows(t, e.parts)
	require.NoError(t, e.Merge(context.Background(), 0))
	assert.Equal(t, want, mergedRows(t, e.parts))
	assert.Equal(t, 6, e.PartCount())
}

func TestCompressionRewriteByteBound(t *testing.T) {
	t.Parallel()
	e := compressionEngine(t, []int64{1, 2}, []int64{11, 12}, []int64{21, 22})
	e.mergeCap = e.parts[0].sizeBytes()
	sh := e.MergeShape()
	require.Equal(t, 3, sh.Sealed)
	assert.Equal(t, 1, sh.Candidates, "one sealed source fills the cumulative byte bound")
	before := e.PartPrefixes()
	want := mergedRows(t, e.parts)
	require.NoError(t, e.Merge(context.Background(), 0))
	assert.NotContains(t, e.PartPrefixes(), before[0])
	assert.Contains(t, e.PartPrefixes(), before[1])
	assert.Contains(t, e.PartPrefixes(), before[2])
	assert.Equal(t, want, mergedRows(t, e.parts))
	assert.Equal(t, 1, e.MergeShape().Candidates, "the remaining rewrite backlog drains over later cycles")
}

func TestCompressionRewriteAdmissionDeferral(t *testing.T) {
	t.Parallel()
	e := compressionEngine(t, []int64{1, 2})
	before := e.PartPrefixes()
	e.cfg.MergeAdmission = func(context.Context, int64, bool) (func(), bool, error) {
		return nil, false, nil
	}
	require.NoError(t, e.MergeWith(context.Background(), MergeOptions{Background: true}))
	assert.True(t, e.MergeDeferred())
	assert.Equal(t, before, e.PartPrefixes())
	assert.Equal(t, 1, e.MergeShape().Candidates)
	e.cfg.MergeAdmission = nil
	require.NoError(t, e.MergeWith(context.Background(), MergeOptions{Background: true}))
	assert.False(t, e.MergeDeferred())
	assert.Zero(t, e.MergeShape().Candidates)
}

func TestCompressionRewritePreservesOrdinaryPriority(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		times [][]int64
	}{
		{"size compaction", [][]int64{{1, 2}, {2*day + 1, 2*day + 2}, {2*day + 3, 2*day + 4}}},
		{"straddler batch", [][]int64{{1, 2*day + 1}, {2, 2*day + 2}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := compressionEngine(t, tc.times...)
			ordinary := selectMergeParts(e.parts, 0, 0, false)
			require.Len(t, ordinary, 2)
			assert.Equal(t, ordinary, e.selectMergeParts(e.parts, 0, 0, false))
		})
	}
}

func TestCompressionRewriteLevels(t *testing.T) {
	t.Parallel()
	e := compressionEngine(t, []int64{1, 2, 3})
	e.cfg.MergeCompressionLevel = compress.LevelDefault
	require.NoError(t, e.Merge(context.Background(), 0))
	assert.Zero(t, e.MergeShape().Candidates)

	e.cfg.MergeCompressionLevel = 3
	assert.Zero(t, e.MergeShape().Candidates, "explicit level 3 equals the default")
	e.cfg.MergeCompressionLevel = compress.LevelFast
	assert.Zero(t, e.MergeShape().Candidates, "default level is stronger than fast")
	e.cfg.MergeCompressionLevel = compress.LevelBest
	assert.Equal(t, 1, e.MergeShape().Candidates)
	require.NoError(t, e.Merge(context.Background(), 0))
	assert.Zero(t, e.MergeShape().Candidates)
	e.cfg.MergeCompressionLevel = compress.LevelDefault
	assert.Zero(t, e.MergeShape().Candidates, "stronger compression must not be downgraded")

	e.cfg.MergeCompression = compress.AlgorithmLZ4
	assert.Equal(t, 1, e.MergeShape().Candidates)
	require.NoError(t, e.Merge(context.Background(), 0))
	e.cfg.MergeCompressionLevel = compress.LevelBest
	assert.Zero(t, e.MergeShape().Candidates, "LZ4 ignores the level")
}

func TestCompressionRewriteDisabled(t *testing.T) {
	t.Parallel()
	e := compressionEngine(t, []int64{1, 2, 3})
	e.cfg.MergeCompression = compress.AlgorithmNone
	before := e.PartPrefixes()
	assert.Zero(t, e.MergeShape().Candidates)
	require.NoError(t, e.Merge(context.Background(), 0))
	assert.Equal(t, before, e.PartPrefixes())
}

func TestCompressionRewriteIsTimeLocalAndBounded(t *testing.T) {
	t.Parallel()
	e := compressionEngine(t, []int64{1, 2}, []int64{3*day + 1, 3*day + 2})
	before := e.PartPrefixes()
	assert.Equal(t, 1, e.MergeShape().Candidates)
	require.NoError(t, e.Merge(context.Background(), 0))
	assert.Contains(t, e.PartPrefixes(), before[1], "the other day's part must wait for its own rewrite")
	assert.NotContains(t, e.PartPrefixes(), before[0])
	assert.Equal(t, 1, e.MergeShape().Candidates)
	require.NoError(t, e.Merge(context.Background(), 0))
	assert.Zero(t, e.MergeShape().Candidates)
}

func TestCompressionRewriteIncompressibleFixedPoint(t *testing.T) {
	t.Parallel()
	e := compressionEngine(t, []int64{1, 2})
	r := rand.New(rand.NewPCG(1, 2))
	series := signal.Series{}
	b := &Batch{Stream: series.Hash(), Identity: func() signal.Series { return series }, Ts: []int64{3, 4},
		Ints: [][]int64{{0, 1}}, Bytes: [][][]byte{{make([]byte, 64), make([]byte, 64)}}}
	for _, row := range b.Bytes[0] {
		for i := range row {
			row[i] = byte(r.Uint32())
		}
	}
	_, err := e.AppendBatch(b, AppendLimits{})
	require.NoError(t, err)
	require.NoError(t, e.Flush(context.Background()))
	require.NoError(t, e.Merge(context.Background(), 0))
	before := e.PartPrefixes()
	require.NoError(t, e.Merge(context.Background(), 0))
	assert.Equal(t, before, e.PartPrefixes())
	assert.Zero(t, e.MergeShape().Candidates)
}
