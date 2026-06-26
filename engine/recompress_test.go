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

// valueColumnAlg opens the single metric part under the prefix and returns its value column's
// block-compression algorithm — the observable result of recompression.
func valueColumnAlg(t *testing.T, b backend.Backend) compress.Algorithm {
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
				return c.Compress
			}
		}
	}

	t.Fatal("no value column found")

	return compress.AlgorithmNone
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

func TestMergeDoesNotRecompressWarmData(t *testing.T) {
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
	assert.Equal(t, compress.AlgorithmNone, valueColumnAlg(t, b), "warm part keeps default framing")
}
