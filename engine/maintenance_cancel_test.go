package engine_test

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/faultbackend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
)

func TestCanceledQueuedFlushLeavesHeadUntouched(t *testing.T) {
	t.Parallel()
	be := faultbackend.Wrap(backend.Memory())
	e := engine.New(engine.Config{Backend: be, Prefix: "metrics",
		FlushAdmission: func(context.Context, int64) (func(), error) { return func() {}, nil }})
	s := mkSeries("job", "api")
	for i := range 16 {
		_, err := e.Append(s, int64(i+1), float64(i+1))
		require.NoError(t, err)
	}
	gate := faultbackend.NewGate()
	release := sync.OnceFunc(gate.Release)
	t.Cleanup(release)
	be.Add(gate.Rule(faultbackend.Write, func(op faultbackend.Op) bool { return strings.Contains(op.Key, "/c/") }))
	first := make(chan error, 1)
	go func() { first <- e.Flush(t.Context()) }()
	gate.Await(t)
	_, err := e.Append(s, 100, 100)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	second := make(chan error, 1)
	go func() { second <- e.Flush(ctx) }()
	cancel()
	release()
	require.NoError(t, <-first)
	require.ErrorIs(t, <-second, context.Canceled)
	assert.Equal(t, 1, e.PartCount())
	rows := fetchAll(t, e, fetch.Request{Start: 0, End: 200})
	require.Len(t, rows, 1)
	want := make([]int64, 17)
	for i := range 16 {
		want[i] = int64(i + 1)
	}
	want[16] = 100
	assert.Equal(t, want, rows[0].Timestamps)
	require.NoError(t, e.Flush(t.Context()))
	assert.Equal(t, 2, e.PartCount())
}

func TestCanceledMaintenancePreservesEmptyNoop(t *testing.T) {
	t.Parallel()
	for _, data := range []bool{false, true} {
		name := "empty"
		if data {
			name = "with-data"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var admissions int
			admit := func(context.Context, int64) (func(), error) { admissions++; return func() {}, nil }
			e := engine.New(engine.Config{Backend: backend.Memory(), Prefix: "metrics", FlushAdmission: admit})
			if data {
				_, err := e.Append(mkSeries("job", "api"), 1, 1)
				require.NoError(t, err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			err := e.Flush(ctx)
			if data {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.NoError(t, err)
			}
			assert.Zero(t, admissions)
			assert.Zero(t, e.PartCount())
			require.ErrorIs(t, e.MergeWith(ctx, engine.MergeOptions{Force: true}), context.Canceled)
		})
	}
}
