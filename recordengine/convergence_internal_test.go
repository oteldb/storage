package recordengine

import (
	"crypto/sha256"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/compress"
)

func convergenceEngine(t *testing.T) *Engine {
	t.Helper()
	return wideDictEngine(t, backend.Memory(), Config{
		MaxPartBytes: 64 << 20, MergeCeilingBytes: 1 << 30, MergeMemoryBytes: 32 << 20,
		MergeCompression: compress.AlgorithmZSTD, MergeCompressionLevel: compress.LevelFast,
	}, 3, 4, 128<<10, 1)
}

func recordDigest(t *testing.T, e *Engine) (uint64, [4]uint64) {
	t.Helper()
	var rows uint64
	var digest [4]uint64
	var buf []byte
	for _, p := range e.parts {
		c, err := p.readCols(t.Context(), fullSel(e.cfg.Schema), nil, nil)
		require.NoError(t, err)
		for _, r := range p.ranges {
			for i := r.start; i < r.end; i++ {
				buf = binary.LittleEndian.AppendUint64(buf[:0], r.id.Hi)
				buf = binary.LittleEndian.AppendUint64(buf, r.id.Lo)
				buf = binary.LittleEndian.AppendUint64(buf, uint64(c.ts[i]))
				for _, col := range c.ints {
					buf = binary.LittleEndian.AppendUint64(buf, uint64(col[i]))
				}
				for _, col := range c.bytes {
					v := col.at(i)
					buf = binary.LittleEndian.AppendUint64(buf, uint64(len(v)))
					buf = append(buf, v...)
				}
				sum := sha256.Sum256(buf)
				for k := range digest {
					digest[k] += binary.LittleEndian.Uint64(sum[k*8:])
				}
				rows++
			}
		}
	}
	return rows, digest
}

func TestResidentLimitedCompactionConverges(t *testing.T) {
	t.Parallel()
	e := convergenceEngine(t)
	rows, want := recordDigest(t, e)
	for range 24 {
		before := e.PartPrefixes()
		index, err := e.cfg.Backend.Read(t.Context(), e.indexKey())
		require.NoError(t, err)
		changed, err := e.MergeStep(t.Context(), MergeOptions{})
		require.NoError(t, err)
		if !changed {
			assert.Equal(t, before, e.PartPrefixes(), "a rejected rewrite must leave the committed inputs intact")
			assert.Empty(t, unindexedParts(t, e), "discarded output must not leave backend objects behind")
			after, err := e.cfg.Backend.Read(t.Context(), e.indexKey())
			require.NoError(t, err)
			assert.Equal(t, index, after, "rejection must not publish an index or block claim")
		}
		if e.MergeShape().Candidates == 0 {
			break
		}
	}
	require.Zero(t, e.MergeShape().Candidates, "unchanged resident-limited runs must reach a fixed point")
	require.Positive(t, e.MergeShape().NonReducingMerges)
	require.LessOrEqual(t, e.PartCount(), 3, "ordinary compaction must not multiply already-compressed parts")
	gotRows, got := recordDigest(t, e)
	assert.Equal(t, rows, gotRows)
	assert.Equal(t, want, got)
	before := e.PartPrefixes()
	changed, err := e.MergeStep(t.Context(), MergeOptions{Force: true})
	require.NoError(t, err)
	assert.False(t, changed, "force waives scoring, not known output feasibility")
	assert.Equal(t, before, e.PartPrefixes())
	e.cfg.MergeMemoryBytes = 128 << 20
	for step := 0; step < 12 && e.PartCount() > 1; step++ {
		_, err := e.MergeStep(t.Context(), MergeOptions{Force: true})
		require.NoError(t, err)
	}
	require.Equal(t, 1, e.PartCount(), "more resident allowance must invalidate the old rejection")
}
