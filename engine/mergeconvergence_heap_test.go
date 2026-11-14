//go:build !race

package engine

import (
	"testing"

	"github.com/stretchr/testify/require"

	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/internal/heaptest"
)

//nolint:paralleltest // Samples process-wide live heap during streamed column writes.
func TestMostlyDisjointMergeFitsGrant(t *testing.T) {
	heaptest.SkipUnsupported(t)
	files, err := backendfile.New(t.TempDir())
	require.NoError(t, err)
	sampler := &heaptest.FileSampler{File: files, KeyContains: "/c/", Writes: true}
	e, _ := mostlyDisjointMergeFixture(t, sampler)
	peak := heaptest.Resident(t, sampler, func() {
		require.NoError(t, e.MergeWith(t.Context(), MergeOptions{Force: true}))
	})
	t.Logf("peak_live_bytes=%d grant=%d outputs=%d", peak, disjointMergeGrant, e.PartCount())
	require.LessOrEqual(t, peak, uint64(disjointMergeGrant))
	require.Less(t, e.PartCount(), disjointMergeInputs)
}
