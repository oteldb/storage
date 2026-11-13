package engine

import (
	"context"
	"math"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/encoding/compress"
	"github.com/oteldb/storage/internal/buildbudget"
)

func TestMergeMemorySelectionDoesNotSkipAffordableFanIn(t *testing.T) {
	t.Parallel()
	e, _ := inputMemoryCorpus(t, 32768, 16, 128<<20, false)
	opts := MergeOptions{Force: true, Recompress: &RecompressSpec{Before: math.MaxInt64, Algorithm: compress.AlgorithmNone}}
	readable := slices.Clone(e.parts)
	cohorts := mergeCohorts(e.parts, readable)
	want := cohortRunPlanLimited(cohorts, readable, opts, 0, mergeIdleRounds, 0, 7)
	bound, err := e.mergeInputBound(t.Context(), want.parts, false)
	require.NoError(t, err)
	metadata, err := e.mergeMetadataWorkspace(t.Context(), want.parts)
	require.NoError(t, err)
	input := buildbudget.Add(bound.bytes(), buildbudget.Add(metadata, mergeSeriesScratchBound(want.parts, want.tiers)))
	limit := max(2*input, input+e.minimumMergeOutput(want.parts, 0, opts, want.tiers))
	selection, err := e.selectBoundedMergeWithAllowance(t.Context(), cohorts, readable, opts, 0, mergeIdleRounds, 0, limit)
	require.NoError(t, err)
	require.Len(t, selection.parts, 7, "a seven-input grant must not fall back to four")
	require.Equal(t, want.parts, selection.parts)
	require.Greater(t, selection.plan.Attempts, 1)
	require.Equal(t, 16, selection.plan.Candidates)
	require.LessOrEqual(t, selection.inputBytes, limit/2)
}

func TestMergePlanAdmissionAndOutputs(t *testing.T) {
	t.Parallel()
	e, _ := inputMemoryCorpus(t, 16, 24, 128<<20, false)
	defer func() { require.NoError(t, e.Close(t.Context())) }()
	busy := true
	e.cfg.MergeAdmission = func(_ context.Context, _ int64, _ bool) (func(), bool, error) {
		return func() {}, !busy, nil
	}
	opts := MergeOptions{Background: true, Force: true}
	require.NoError(t, e.MergeWith(t.Context(), opts))
	plan := e.LastMergePlan()
	require.Equal(t, "pool_busy", plan.Outcome)
	require.Equal(t, 24, plan.Selected)
	busy = false
	require.NoError(t, e.MergeWith(t.Context(), opts))
	plan = e.LastMergePlan()
	require.Equal(t, "completed", plan.Outcome)
	require.Equal(t, 24, plan.Selected)
	require.Equal(t, 1, plan.OutputParts)
	require.Equal(t, 1, e.PartCount())
}
