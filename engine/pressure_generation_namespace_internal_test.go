package engine

import (
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"
)

func TestGenerationPredecessorNamespace(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		e, _, _, resume, _ := blockedGenerationPair(t, nil)
		a, b := e.pressureGenerations[0], e.pressureGenerations[1]
		lease := a.mergeNames
		first, ok := lease.Next()
		require.True(t, ok)
		second, ok := lease.Next()
		require.True(t, ok)
		later, ok := a.names.Next()
		require.True(t, ok)
		nextMerge, ok := b.mergeNames.Next()
		require.True(t, ok)
		prefix := e.cfg.Prefix + "/"
		require.Less(t, first.String(), second.String(), "each merge can consume a fresh floor after earlier outputs")
		require.Less(t, prefix+second.String(), a.prefixFloor)
		require.Less(t, later.String(), nextMerge.String())
		require.Less(t, prefix+nextMerge.String(), b.prefixFloor)
		ceiling := a.prefixFloor
		resume()
		synctest.Wait()
		require.NoError(t, e.Flush(t.Context()))
		require.Nil(t, a.head)
		require.Nil(t, b.head)
		require.Empty(t, e.pressureGenerations)
		afterRetirement, ok := lease.Next()
		require.True(t, ok)
		require.Greater(t, afterRetirement.String(), second.String())
		require.Less(t, prefix+afterRetirement.String(), ceiling, "a leased namespace survives without retaining a head")
		require.NoError(t, e.Close(t.Context()))
	})
}
