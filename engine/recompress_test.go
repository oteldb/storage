package engine_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/block"
	"github.com/oteldb/storage/encoding/compress"
	"github.com/oteldb/storage/engine"
)

// valueColumnProfile opens the single metric part under the prefix and returns its value column's
// block-compression algorithm and level — the observable result of the compression ladder.
func valueColumnProfile(t *testing.T, b backend.Backend) (compress.Algorithm, compress.Level) {
	t.Helper()
	ctx := context.Background()

	keys, err := b.List(ctx, "default/metrics")
	require.NoError(t, err)

	for _, k := range keys {
		if !strings.HasSuffix(k, "/manifest") {
			continue
		}

		r, err := block.OpenPart(ctx, b, strings.TrimSuffix(k, "/manifest"))
		require.NoError(t, err)

		for _, c := range r.Manifest().Columns {
			if c.Name == "value" {
				return c.Compress, c.Level
			}
		}
	}

	t.Fatal("no value column found")

	return compress.AlgorithmNone, 0
}

// valueColumnAlg is [valueColumnProfile] without the level.
func valueColumnAlg(t *testing.T, b backend.Backend) compress.Algorithm {
	t.Helper()

	alg, _ := valueColumnProfile(t, b)

	return alg
}

func partKeys(t *testing.T, b backend.Backend) []string {
	t.Helper()
	keys, err := b.List(context.Background(), "default/metrics")
	require.NoError(t, err)

	return keys
}

func TestMergeRecompressesColdData(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	b := backend.Memory()
	e := engine.New(engine.Config{Backend: b, Prefix: "default/metrics"})
	s := mkSeries("job", "api")

	// Two parts of old (cold) data.
	mustAppend(t, e, s, 100, 1)
	require.NoError(t, e.Flush(ctx))
	mustAppend(t, e, s, 200, 2)
	require.NoError(t, e.Flush(ctx))
	assert.Equal(t, compress.AlgorithmNone, valueColumnAlg(t, b), "flush writes codec-only framing")

	// Merge with recompression: every sample (ts ≤ 200) is older than the cutoff, so the merged
	// part is fully cold and rewritten with zstd.
	cold := &engine.RecompressSpec{Before: 1000, Algorithm: compress.AlgorithmZSTD, Level: compress.LevelBest}
	require.NoError(t, e.MergeWith(ctx, engine.MergeOptions{Recompress: cold}))
	assert.Equal(t, 1, e.PartCount())
	assert.Equal(t, compress.AlgorithmZSTD, valueColumnAlg(t, b), "cold merged part recompressed")

	// Fixed point: re-merging the lone, already-cold-compressed part is a no-op (no backend churn).
	before := partKeys(t, b)
	require.NoError(t, e.MergeWith(ctx, engine.MergeOptions{Recompress: cold}))
	assert.Equal(t, before, partKeys(t, b), "already-recompressed part is not rewritten")
}

// TestMergeLaddersWarmData pins the ladder: a merge compresses its output even when no part is cold,
// at the cheap level a small part earns — not the archival level the cold tier would apply.
func TestMergeLaddersWarmData(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	b := backend.Memory()
	e := engine.New(engine.Config{Backend: b, Prefix: "default/metrics"})
	s := mkSeries("job", "api")

	// Two parts of recent (warm) data, newer than the cutoff.
	mustAppend(t, e, s, 5000, 1)
	require.NoError(t, e.Flush(ctx))
	mustAppend(t, e, s, 6000, 2)
	require.NoError(t, e.Flush(ctx))

	cold := &engine.RecompressSpec{Before: 1000, Algorithm: compress.AlgorithmZSTD, Level: compress.LevelBest}
	require.NoError(t, e.MergeWith(ctx, engine.MergeOptions{Recompress: cold}))
	assert.Equal(t, 1, e.PartCount())
	alg, level := valueColumnProfile(t, b)
	assert.Equal(t, compress.AlgorithmZSTD, alg, "a merge always compresses its output")
	assert.Equal(t, compress.Level(1), level, "a small warm part takes the cheapest ladder level")
}

// TestMergeFixedPointAtLadderLevel checks that the cold tier's fixed point survives the ladder: a
// part written at the ladder level is upgraded once to the cold level and then left alone.
func TestMergeFixedPointAtLadderLevel(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	b := backend.Memory()
	e := engine.New(engine.Config{Backend: b, Prefix: "default/metrics"})
	s := mkSeries("job", "api")

	mustAppend(t, e, s, 5000, 1)
	require.NoError(t, e.Flush(ctx))
	mustAppend(t, e, s, 6000, 2)
	require.NoError(t, e.Flush(ctx))

	// Warm merge: the ladder level, below the cold tier's.
	require.NoError(t, e.MergeWith(ctx, engine.MergeOptions{}))

	alg, level := valueColumnProfile(t, b)
	require.Equal(t, compress.AlgorithmZSTD, alg)
	require.Equal(t, compress.Level(1), level)

	// The same part, now cold: it is below the cold level, so it is rewritten exactly once.
	cold := &engine.RecompressSpec{Before: 10000, Algorithm: compress.AlgorithmZSTD, Level: 9}
	require.NoError(t, e.MergeWith(ctx, engine.MergeOptions{Recompress: cold}))

	alg, level = valueColumnProfile(t, b)
	assert.Equal(t, compress.AlgorithmZSTD, alg)
	assert.Equal(t, compress.Level(9), level, "cold part upgraded to the age tier's level")

	before := partKeys(t, b)
	require.NoError(t, e.MergeWith(ctx, engine.MergeOptions{Recompress: cold}))
	assert.Equal(t, before, partKeys(t, b), "a part already at the cold level is not rewritten")
}
