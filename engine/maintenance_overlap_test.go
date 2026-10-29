package engine_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/faultbackend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/internal/memlimit"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

func TestFlushDuringMergeWithBudget(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		merge int64
	}{
		{name: "overlap", merge: 32 << 20},
		{name: "exhausted-budget", merge: 128 << 20},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				be := faultbackend.Wrap(backend.Memory())
				pool := memlimit.NewPool(128 << 20)
				e := engine.New(engine.Config{
					Backend: be, Prefix: "metrics", MergeMemoryBytes: tc.merge,
					FlushAdmission: pool.Acquire,
					MergeAdmission: func(ctx context.Context, bytes int64, _ bool) (func(), bool, error) {
						release, err := pool.Acquire(ctx, bytes)
						return release, err == nil, err
					},
				})
				s := mkSeries("job", "api")
				for round := range 2 {
					for row := range 16 {
						_, err := e.Append(s, int64(round*16+row+1), float64(row))
						require.NoError(t, err)
					}
					require.NoError(t, e.Flush(t.Context()))
				}
				gate := faultbackend.NewGate()
				release := sync.OnceFunc(gate.Release)
				defer release()
				be.Add(gate.Rule(faultbackend.Read, func(op faultbackend.Op) bool { return strings.Contains(op.Key, "/c/") }))
				merged := make(chan error, 1)
				go func() { merged <- e.MergeWith(t.Context(), engine.MergeOptions{Force: true}) }()
				gate.Await(t)
				be.Reset()
				_, err := e.Append(s, 100, 100)
				require.NoError(t, err)
				flushed := make(chan error, 1)
				go func() { flushed <- e.Flush(t.Context()) }()
				synctest.Wait()
				if tc.merge == pool.Total() {
					assert.Equal(t, 1, pool.Waiting())
					assert.Equal(t, 2, e.PartCount())
				} else {
					require.NoError(t, <-flushed)
					assert.Equal(t, 3, e.PartCount(), "pressure flush finishes before the paused merge")
				}
				release()
				require.NoError(t, <-merged)
				if tc.merge == pool.Total() {
					require.NoError(t, <-flushed)
				}
				got := fetchAll(t, e, fetch.Request{Start: 0, End: 1000})
				require.Len(t, got, 1)
				assert.Len(t, got[0].Timestamps, 33)
				assert.Equal(t, 2, e.PartCount(), "merge preserves the concurrently flushed part")
			})
		})
	}
}

func TestDetachedSamplesRemainCharged(t *testing.T) {
	t.Parallel()
	be := faultbackend.Wrap(backend.Memory())
	e := engine.New(engine.Config{Backend: be, Prefix: "metrics"})
	s := mkSeries("job", "api")
	_, err := e.Append(s, 1, 1)
	require.NoError(t, err)
	gate := faultbackend.NewGate()
	release := sync.OnceFunc(gate.Release)
	t.Cleanup(release)
	be.Add(gate.Rule(faultbackend.Write, nil))
	done := make(chan error, 1)
	go func() { done <- e.Flush(t.Context()) }()
	gate.Await(t)
	assert.EqualValues(t, engine.SampleBytes, e.HeadBytes())
	res, err := e.AppendBatch([]signal.SeriesID{s.Hash()}, []int64{2}, []float64{2}, nil,
		func(int) signal.Series { return s }, engine.AppendLimits{MaxInFlightBytes: engine.SampleBytes})
	require.NoError(t, err)
	assert.Equal(t, 1, res.RejectedBytes)
	release()
	require.NoError(t, <-done)
	assert.Zero(t, e.HeadBytes())
}
