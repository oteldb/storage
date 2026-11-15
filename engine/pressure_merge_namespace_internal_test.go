package engine

import (
	"context"
	"strings"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/faultbackend"
	"github.com/oteldb/storage/internal/partid"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

func TestPressureMergesDuringPendingGeneration(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		e, _ := newFaultWALEngine(t, 10)
		memory := faultbackend.Wrap(backend.Memory())
		e.cfg.Backend, e.cfg.Prefix = memory, "pending-merge-service"
		e.cfg.DiskSeries, e.cfg.AsyncPressureFlush, e.cfg.PressureGenerations = true, true, 2
		const headLimit = 8 << 20
		e.cfg.ResidentHeadBytes, e.cfg.MergeMemoryBytes = headLimit, 128<<20
		e.cfg.FlushAdmission = func(context.Context, int64) (func(), error) { return func() {}, nil }
		e.head.disk = newDiskHeadState()
		defer func() { require.NoError(t, e.Close(t.Context())) }()
		resumes := make([]func(), 0, 3)
		defer func() {
			for _, resume := range resumes {
				resume()
			}
		}()
		s := benchSeries(0)
		value := float64(0)
		appendValue := func() {
			value++
			_, err := e.Append(s, 100, value)
			require.NoError(t, err)
		}
		for range maxMergeParts + 5 {
			appendValue()
			require.NoError(t, e.Flush(t.Context()))
		}
		for cycle := range 3 {
			if cycle > 0 {
				for range 5 {
					appendValue()
					require.NoError(t, e.Flush(t.Context()))
				}
			}
			gate := &generationGateBackend{Backend: memory, first: make(chan struct{}), resume: make(chan struct{})}
			resume := sync.OnceFunc(func() { close(gate.resume) })
			resumes = append(resumes, resume)
			e.cfg.Backend = gate
			appendValue()
			done := make(chan error, 1)
			go func() { done <- e.Flush(t.Context()) }()
			<-gate.first
			e.mu.RLock()
			ceiling := e.pressureGenerations[0].prefixFloor
			e.mu.RUnlock()
			failedPrefix := ""
			if cycle == 0 {
				memory.Add(faultbackend.Rule{Kind: faultbackend.Write, Err: context.DeadlineExceeded,
					Match: func(op faultbackend.Op) bool {
						if prefix, ok := strings.CutSuffix(op.Key, "/identity"); ok {
							failedPrefix = prefix
							return true
						}
						return false
					}})
				before := e.PartCount()
				changed, err := e.MergeStep(t.Context(), MergeOptions{Force: true})
				require.ErrorIs(t, err, context.DeadlineExceeded)
				require.False(t, changed)
				require.Equal(t, before, e.PartCount())
				require.NotEmpty(t, failedPrefix)
				require.Nil(t, e.mergeNames, "failed output attempts release their namespace lease")
				memory.Reset()
			}
			for attempt := 0; e.PartCount() > 1 && attempt < 4; attempt++ {
				before := e.PartCount()
				changed, err := e.MergeStep(t.Context(), MergeOptions{Force: true})
				require.NoError(t, err)
				require.True(t, changed, "the pending generation must not starve compaction")
				require.Less(t, e.PartCount(), before)
				require.Nil(t, e.mergeNames)
				for _, p := range e.parts {
					require.Less(t, p.prefix, ceiling, "rewritten older rows stay older than the pending generation")
					require.NotEqual(t, failedPrefix, p.prefix, "failed output names must never be reused")
				}
			}
			require.Equal(t, 1, e.PartCount())
			require.Len(t, e.pressureGenerations, 1, "merging must not drain or pause the generation")
			_, err := e.Append(benchSeries(1), 200, float64(cycle))
			require.NoError(t, err, "active writes retain their original head budget")
			require.EqualValues(t, headLimit, e.cfg.ResidentHeadBytes)
			resume()
			require.NoError(t, <-done)
			e.cfg.Backend = memory
			require.NoError(t, e.LoadParts(t.Context()))
			it, err := e.Fetch(t.Context(), fetch.Request{Start: 100, End: 100})
			require.NoError(t, err)
			batches, err := fetch.Drain(t.Context(), it)
			require.NoError(t, err)
			require.Len(t, batches, 1)
			require.Equal(t, []float64{value}, batches[0].Values)
		}
	})
}

func TestMergeNamespaceBounds(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		id   partid.ID
		want partid.ID
		ok   bool
	}{
		{name: "next millisecond", id: partid.ID{5: 42, 6: 255, 15: 255}, want: partid.ID{5: 43}, ok: true},
		{name: "carry", id: partid.ID{4: 2, 5: 255, 6: 7}, want: partid.ID{4: 3}, ok: true},
		{name: "exhausted", id: partid.ID{0: 255, 1: 255, 2: 255, 3: 255, 4: 255, 5: 255}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, ok := mergeNamespaceCeiling(tc.id)
			require.Equal(t, tc.ok, ok)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestMergeNamespaceKeepsPublishedGapGuard(t *testing.T) {
	t.Parallel()
	e, _ := newFaultWALEngine(t, 10)
	e.cfg.DiskSeries, e.cfg.AsyncPressureFlush, e.cfg.PressureGenerations = true, true, 2
	e.cfg.Prefix = "namespace-gap"
	t.Cleanup(func() { require.NoError(t, e.CloseWAL()) })
	id := benchSeries(0).Hash()
	for range 3 {
		e.parts = append(e.parts, &part{prefix: e.newPartPrefix(), minTime: 100, maxTime: 100,
			index: partIndex{ids: []signal.SeriesID{id}}})
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	var err error
	e.mergeNames, err = e.reserveMergeNamesLocked()
	require.NoError(t, err)
	defer func() { e.mergeNames = nil }()
	require.Less(t, e.parts[2].prefix, e.mergeNames.floor)
	selected := []*part{e.parts[0], e.parts[2]}
	require.ErrorIs(t, e.pressureMergeOrderLocked(selected, nil), errPressureMergeOrder,
		"A,C cannot leapfrog an excluded published B even inside a reserved namespace")
	output, err := e.newMergePartPrefix()
	require.NoError(t, err)
	require.ErrorIs(t, e.pressureMergeOrderLocked(selected, []*part{{prefix: output, minTime: 100, maxTime: 100}}), errPressureMergeOrder)
	require.NoError(t, e.pressureMergeOrderLocked(e.parts[1:], nil))
	unknown := &part{prefix: e.mergeNames.ceiling, minTime: 100, maxTime: 100}
	require.ErrorIs(t, e.pressureMergeOrderLocked([]*part{unknown}, nil), errPressureMergeOrder,
		"a source newer than the attempt floor cannot be assigned an older output")
	e.pressureGenerations = []*pressureGeneration{{prefixFloor: e.newPartPrefix(),
		head: &head{bufferedKnown: true, bufferedMin: 100, newest: 100}}}
	defer func() { e.pressureGenerations = nil }()
	raw := &part{prefix: output, minTime: 100, maxTime: 100}
	require.NoError(t, e.pressureMergeOrderLocked(e.parts, []*part{raw}))
	raw.rollup = []DownsampleTier{{Before: 200, Interval: 10, Agg: signal.AggSum}}
	require.ErrorIs(t, e.pressureMergeOrderLocked(e.parts, []*part{raw}), errPressureMergeOrder,
		"a pending duplicate must participate before old raw values become a representative")
}

func TestPressureMergeNamespacePreservesRollupClosure(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		e, _ := newFaultWALEngine(t, 10)
		memory := backend.Memory()
		e.cfg.Backend, e.cfg.Prefix = memory, "pending-rollup"
		e.cfg.DiskSeries, e.cfg.AsyncPressureFlush, e.cfg.PressureGenerations = true, true, 2
		e.cfg.ResidentHeadBytes = 8 << 20
		e.cfg.FlushAdmission = func(context.Context, int64) (func(), error) { return func() {}, nil }
		e.head.disk = newDiskHeadState()
		defer func() { require.NoError(t, e.Close(t.Context())) }()
		s := benchSeries(0)
		for range 2 {
			_, err := e.Append(s, 100, 1)
			require.NoError(t, err)
			require.NoError(t, e.Flush(t.Context()))
		}
		gate := &generationGateBackend{Backend: memory, first: make(chan struct{}), resume: make(chan struct{})}
		resume := sync.OnceFunc(func() { close(gate.resume) })
		defer resume()
		e.cfg.Backend = gate
		_, err := e.Append(s, 100, 2)
		require.NoError(t, err)
		done := make(chan error, 1)
		go func() { done <- e.Flush(t.Context()) }()
		<-gate.first
		opts := MergeOptions{Force: true, Downsample: []DownsampleTier{{Before: 200, Interval: 10, Agg: signal.AggSum}}}
		require.NoError(t, e.MergeWith(t.Context(), opts))
		require.Equal(t, 1, e.PartCount())
		require.Empty(t, e.parts[0].rollup, "pending raw bounds clamp the requested rollup while plain compaction progresses")
		require.Nil(t, e.mergeNames)
		resume()
		require.NoError(t, <-done)
		require.NoError(t, e.MergeWith(t.Context(), opts))
		require.NoError(t, e.LoadParts(t.Context()))
		it, err := e.Fetch(t.Context(), fetch.Request{Start: 100, End: 100})
		require.NoError(t, err)
		batches, err := fetch.Drain(t.Context(), it)
		require.NoError(t, err)
		require.Len(t, batches, 1)
		require.Equal(t, []float64{2}, batches[0].Values, "the duplicate must resolve before Sum produces a representative")
	})
}
