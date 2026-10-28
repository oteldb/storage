package recordengine_test

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/faultbackend"
	"github.com/oteldb/storage/recordengine"
)

func TestCanceledQueuedFlushLeavesHeadUntouched(t *testing.T) {
	t.Parallel()
	be := faultbackend.Wrap(backend.Memory())
	e := recordengine.New(recordengine.Config{Schema: testSchema, Backend: be, Prefix: "records",
		FlushAdmission: func(context.Context, int64) (func(), error) { return func() {}, nil }})
	var want []string
	for i := range 16 {
		body := strconv.Itoa(i)
		_, err := e.AppendBatch(mkBatch("api", rrec{ts: int64(i + 1), body: body}), recordengine.AppendLimits{})
		require.NoError(t, err)
		want = append(want, body)
	}
	gate := faultbackend.NewGate()
	release := sync.OnceFunc(gate.Release)
	t.Cleanup(release)
	be.Add(gate.Rule(faultbackend.Write, func(op faultbackend.Op) bool { return strings.Contains(op.Key, "/c/") }))
	first := make(chan error, 1)
	go func() { first <- e.Flush(t.Context()) }()
	gate.Await(t)
	_, err := e.AppendBatch(mkBatch("api", rrec{ts: 100, body: "last"}), recordengine.AppendLimits{})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	second := make(chan error, 1)
	go func() { second <- e.Flush(ctx) }()
	cancel()
	release()
	require.NoError(t, <-first)
	require.ErrorIs(t, <-second, context.Canceled)
	assert.Equal(t, 1, e.PartCount())
	assert.Equal(t, append(want, "last"), streamBodies(t, e))
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
			e := recordengine.New(recordengine.Config{Schema: testSchema, Backend: backend.Memory(), Prefix: "records", FlushAdmission: admit})
			if data {
				_, err := e.AppendBatch(mkBatch("api", rrec{ts: 1, body: "first"}), recordengine.AppendLimits{})
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
			require.ErrorIs(t, e.MergeWith(ctx, recordengine.MergeOptions{Force: true}), context.Canceled)
		})
	}
}
