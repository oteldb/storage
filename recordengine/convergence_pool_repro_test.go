package recordengine

import (
	"context"
	"encoding/binary"
	"runtime"
	"runtime/debug"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/internal/reproduce"
	"github.com/oteldb/storage/pool"
)

//nolint:paralleltest // Controls process-wide GC and the shared dictionary-map pool.
func TestConvergenceAfterLargePooledMaps(t *testing.T) {
	reproduce.Unfixed(t, 789, "retained dictionary-map capacity leaves non-reducing compression candidates selectable")
	runtime.GC()
	runtime.GC()
	previous := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(previous)

	e := convergenceEngine(t)
	t.Cleanup(func() { require.NoError(t, e.Close(context.WithoutCancel(t.Context()))) })
	rows, digest := recordDigest(t, e)
	maps := make([]*pool.ByteIntMap, 24)
	for i := range maps {
		m := pool.NewByteIntMap()
		for k := range 16384 {
			m.Put(binary.LittleEndian.AppendUint64(nil, uint64(k)), k)
		}
		maps[i] = m
	}
	for _, m := range maps {
		m.PutBack()
	}
	for range 24 {
		_, err := e.MergeStep(t.Context(), MergeOptions{})
		require.NoError(t, err)
		if e.MergeShape().Candidates == 0 {
			break
		}
	}
	gotRows, gotDigest := recordDigest(t, e)
	assert.Equal(t, rows, gotRows)
	assert.Equal(t, digest, gotDigest)
	require.Zero(t, e.MergeShape().Candidates, "unchanged resident-limited runs must reach a fixed point")
}
