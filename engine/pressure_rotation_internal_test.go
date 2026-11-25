package engine

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIdleGenerationWakeKeepsWriterRevision(t *testing.T) {
	t.Parallel()
	e, _ := newFaultWALEngine(t, 10)
	t.Cleanup(func() { require.NoError(t, e.Close(t.Context())) })
	e.pressureGenerations = []*pressureGeneration{{building: true}}
	rev, ok := e.mu.Revision()
	require.True(t, ok)

	started, froze, err := e.startGenerationBuilds(t.Context(), false, true, nil)
	require.NoError(t, err)
	require.Zero(t, started)
	require.False(t, froze)
	got, ok := e.mu.Revision()
	require.True(t, ok)
	e.pressureGenerations = nil
	require.Equal(t, rev, got, "an idle wake should not enter exclusive writer ownership")
}
