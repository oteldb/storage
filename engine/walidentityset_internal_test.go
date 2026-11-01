package engine

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/index/series"
)

func TestWALIdentitySetGenerations(t *testing.T) {
	t.Parallel()
	first, second := series.New(), series.New()
	var markers walIdentitySet
	markers.prepare(first, 1, 130)
	markers.mark(0)
	markers.mark(64)
	markers.prepare(first, 1, 70000)
	require.True(t, markers.contains(0))
	require.True(t, markers.contains(64))
	require.False(t, markers.contains(69999))
	markers.mark(69999)
	markers.prepare(first, 2, 70000)
	require.False(t, markers.contains(0))
	require.False(t, markers.contains(69999))
	markers.mark(64)
	markers.prepare(second, 2, 70000)
	require.False(t, markers.contains(64), "a dense ref in a replacement registry is a different identity")
	require.False(t, markers.contains(1<<30))
	require.Panics(t, func() { markers.mark(1 << 30) })
}

func TestWALIdentitySetConcurrentWordUpdates(t *testing.T) {
	t.Parallel()
	var markers walIdentitySet
	markers.prepare(series.New(), 1, 128)
	var wg sync.WaitGroup
	for bit := range 128 {
		wg.Go(func() { markers.mark(uint32(bit)) })
	}
	wg.Wait()
	for bit := range 128 {
		require.True(t, markers.contains(uint32(bit)), "concurrent disjoint IDs can share a word")
	}
}

func TestWALIdentitySetFootprint(t *testing.T) {
	t.Parallel()
	var markers walIdentitySet
	markers.prepare(series.New(), 1, 14_490_000)
	require.Less(t, markers.bytes(), int64(2<<20), "dense relog state fits below two MiB at 10k-node cardinality")
}
