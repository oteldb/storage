package faultbackend_test

import (
	"context"
	"sync"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
	"github.com/oteldb/storage/backend/faultbackend"
)

// TestConformance guards the wrapper's transparency: with no rules installed it must behave
// exactly like the backend it wraps, or a test that injects nothing is already testing something
// other than production.
func TestConformance(t *testing.T) {
	t.Parallel()
	backendtest.Run(t, func(t *testing.T) backend.Backend {
		t.Helper()

		return faultbackend.Wrap(backend.Memory())
	})
}

var errInjected = errors.New("injected")

func TestRuleFailsMatchingOperation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := faultbackend.Wrap(backend.Memory())
	be.Add(faultbackend.Rule{
		Kind:  faultbackend.Write,
		Match: func(op faultbackend.Op) bool { return op.Key == "blocked" },
		Err:   errInjected,
	})

	require.ErrorIs(t, be.Write(ctx, "blocked", []byte("x")), errInjected)
	require.NoError(t, be.Write(ctx, "allowed", []byte("x")))

	_, err := be.Read(ctx, "blocked")
	require.ErrorIs(t, err, backend.ErrNotExist, "the failed write must not have reached the store")
}

func TestRuleTimesLimitsFailures(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := faultbackend.Wrap(backend.Memory())
	be.Add(faultbackend.Rule{Kind: faultbackend.Write, Err: errInjected, Times: 2})

	require.ErrorIs(t, be.Write(ctx, "k", []byte("1")), errInjected)
	require.ErrorIs(t, be.Write(ctx, "k", []byte("2")), errInjected)
	require.NoError(t, be.Write(ctx, "k", []byte("3")))
}

func TestFirstMatchingRuleWins(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := faultbackend.Wrap(backend.Memory())
	second := errors.New("second")
	be.Add(faultbackend.Rule{Kind: faultbackend.Read, Err: errInjected})
	be.Add(faultbackend.Rule{Kind: faultbackend.Read, Err: second})

	_, err := be.Read(ctx, "k")
	require.ErrorIs(t, err, errInjected)
}

func TestRulesApplyToEveryOperation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := faultbackend.Wrap(backend.Memory())
	for _, kind := range []faultbackend.Kind{
		faultbackend.Read, faultbackend.Write, faultbackend.PutIfAbsent,
		faultbackend.List, faultbackend.Delete,
	} {
		be.Add(faultbackend.Rule{Kind: kind, Err: errInjected})
	}

	require.ErrorIs(t, be.Write(ctx, "k", nil), errInjected)
	_, err := be.Read(ctx, "k")
	require.ErrorIs(t, err, errInjected)
	_, err = be.PutIfAbsent(ctx, "k", nil)
	require.ErrorIs(t, err, errInjected)
	_, err = be.List(ctx, "")
	require.ErrorIs(t, err, errInjected)
	require.ErrorIs(t, be.Delete(ctx, "k"), errInjected)
}

func TestOpsRecordsEveryOperation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := faultbackend.Wrap(backend.Memory())
	require.NoError(t, be.Write(ctx, "k", []byte("v")))
	_, err := be.Read(ctx, "k")
	require.NoError(t, err)
	require.NoError(t, be.Delete(ctx, "k"))

	assert.Equal(t, []faultbackend.Op{
		{Kind: faultbackend.Write, Key: "k"},
		{Kind: faultbackend.Read, Key: "k"},
		{Kind: faultbackend.Delete, Key: "k"},
	}, be.Ops())
	assert.Equal(t, 1, be.Count(func(op faultbackend.Op) bool { return op.Kind == faultbackend.Write }))
}

func TestResetKeepsTheLog(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := faultbackend.Wrap(backend.Memory())
	be.Add(faultbackend.Rule{Kind: faultbackend.Write, Err: errInjected})
	require.ErrorIs(t, be.Write(ctx, "k", nil), errInjected)

	be.Reset()
	require.NoError(t, be.Write(ctx, "k", nil))
	assert.Len(t, be.Ops(), 2)
}

// TestGateSuspendsUntilReleased is the harness' own interleaving test: the gated write must not
// have reached the store while another goroutine runs, and must land once released.
func TestGateSuspendsUntilReleased(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := faultbackend.Wrap(backend.Memory())
	gate := faultbackend.NewGate()
	be.Add(gate.Rule(faultbackend.Write, func(op faultbackend.Op) bool { return op.Key == "gated" }))

	var wg sync.WaitGroup
	wg.Go(func() {
		assert.NoError(t, be.Write(ctx, "gated", []byte("late")))
	})

	assert.Equal(t, faultbackend.Op{Kind: faultbackend.Write, Key: "gated"}, gate.Await(t))

	_, err := be.Read(ctx, "gated")
	require.ErrorIs(t, err, backend.ErrNotExist, "a gated write must not reach the store before release")

	gate.Release()
	wg.Wait()

	got, err := be.Read(ctx, "gated")
	require.NoError(t, err)
	assert.Equal(t, []byte("late"), got)
}

// TestGateDoesNotBlockOtherTraffic guards the property the reproducers depend on: the goroutine
// held at the gate must not stop the goroutine it is waiting for from using the same backend.
func TestGateDoesNotBlockOtherTraffic(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := faultbackend.Wrap(backend.Memory())
	gate := faultbackend.NewGate()
	be.Add(gate.Rule(faultbackend.Write, func(op faultbackend.Op) bool { return op.Key == "gated" }))

	var wg sync.WaitGroup
	wg.Go(func() {
		assert.NoError(t, be.Write(ctx, "gated", nil))
	})

	gate.Await(t)
	require.NoError(t, be.Write(ctx, "other", []byte("v")))

	gate.Release()
	wg.Wait()
}

func TestKindString(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "put-if-absent", faultbackend.PutIfAbsent.String())
	assert.Equal(t, "unknown", faultbackend.Kind(99).String())
}
