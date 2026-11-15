package engine

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/internal/reproduce"
	"github.com/oteldb/storage/query/fetch"
)

func TestPressureMergesDuringPendingGeneration(t *testing.T) {
	reproduce.Unfixed(t, 661, "overlapping pending generations prevent every otherwise safe chronological merge")
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		e, _ := newFaultWALEngine(t, 10)
		memory := backend.Memory()
		e.cfg.Backend, e.cfg.Prefix = memory, "pending-merge-service"
		e.cfg.DiskSeries, e.cfg.AsyncPressureFlush, e.cfg.PressureGenerations = true, true, 2
		const headLimit = 8 << 20
		e.cfg.ResidentHeadBytes, e.cfg.MergeMemoryBytes = headLimit, 128<<20
		e.cfg.FlushAdmission = func(context.Context, int64) (func(), error) { return func() {}, nil }
		e.head.disk = newDiskHeadState()
		t.Cleanup(func() { require.NoError(t, e.Close(t.Context())) })
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
			defer resume()
			e.cfg.Backend = gate
			appendValue()
			done := make(chan error, 1)
			go func() { done <- e.Flush(t.Context()) }()
			<-gate.first
			e.mu.RLock()
			ceiling := e.pressureGenerations[0].prefixFloor
			e.mu.RUnlock()
			for attempt := 0; e.PartCount() > 1 && attempt < 4; attempt++ {
				before := e.PartCount()
				changed, err := e.MergeStep(t.Context(), MergeOptions{Force: true})
				require.NoError(t, err)
				require.True(t, changed, "the pending generation must not starve compaction")
				require.Less(t, e.PartCount(), before)
				for _, p := range e.parts {
					require.Less(t, p.prefix, ceiling, "rewritten older rows stay older than the pending generation")
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
