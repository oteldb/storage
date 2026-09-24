package faultbackend_test

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

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
		{Kind: faultbackend.Write, Key: "k", Bytes: 1},
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

	assert.Equal(t, faultbackend.Op{Kind: faultbackend.Write, Key: "gated", Bytes: 4}, gate.Await(t))

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

// TestReplaceRewritesReadBytes covers the corruption seam: a store that hands back bytes other
// than those written, without reporting an error.
func TestReplaceRewritesReadBytes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := faultbackend.Wrap(backend.Memory())
	be.Add(faultbackend.Rule{
		Kind:    faultbackend.Read,
		Match:   func(op faultbackend.Op) bool { return op.Key == "rotten" },
		Replace: func(_ faultbackend.Op, data []byte) []byte { return append(data, '!') },
	})

	require.NoError(t, be.Write(ctx, "rotten", []byte("v")))
	require.NoError(t, be.Write(ctx, "sound", []byte("v")))

	got, err := be.Read(ctx, "rotten")
	require.NoError(t, err, "corruption is silent: the read still succeeds")
	assert.Equal(t, []byte("v!"), got)

	got, err = be.Read(ctx, "sound")
	require.NoError(t, err)
	assert.Equal(t, []byte("v"), got)
}

// TestReplaceLeavesAFailedReadAlone guards the ordering: a rule carrying both an error and a
// replacement must report the error, not a rewritten value of nothing.
func TestReplaceLeavesAFailedReadAlone(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := faultbackend.Wrap(backend.Memory())
	require.NoError(t, be.Write(ctx, "k", []byte("v")))
	be.Add(faultbackend.Rule{
		Kind:    faultbackend.Read,
		Err:     errInjected,
		Replace: func(_ faultbackend.Op, data []byte) []byte { return append(data, '!') },
	})

	_, err := be.Read(ctx, "k")
	require.ErrorIs(t, err, errInjected)
}

func TestOpBytesRecordsStoredLength(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := faultbackend.Wrap(backend.Memory())
	require.NoError(t, be.Write(ctx, "w", []byte("abc")))
	_, err := be.PutIfAbsent(ctx, "p", []byte("de"))
	require.NoError(t, err)
	_, _, err = be.CompareAndSwap(ctx, "c", backend.VersionAbsent, []byte("f"))
	require.NoError(t, err)
	_, err = be.Read(ctx, "w")
	require.NoError(t, err)

	assert.Equal(t, []faultbackend.Op{
		{Kind: faultbackend.Write, Key: "w", Bytes: 3},
		{Kind: faultbackend.PutIfAbsent, Key: "p", Bytes: 2},
		{Kind: faultbackend.CompareAndSwap, Key: "c", Bytes: 1},
		{Kind: faultbackend.Read, Key: "w"},
	}, be.Ops())
	assert.Equal(t, 6, be.Bytes(func(faultbackend.Op) bool { return true }))
	assert.Equal(t, 3, be.Bytes(func(op faultbackend.Op) bool { return op.Kind == faultbackend.Write }))
}

func TestLoseReportsALostRaceWithoutError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := faultbackend.Wrap(backend.Memory())
	be.Add(faultbackend.Rule{Kind: faultbackend.PutIfAbsent, Lose: true})
	be.Add(faultbackend.Rule{Kind: faultbackend.CompareAndSwap, Lose: true})

	ok, err := be.PutIfAbsent(ctx, "p", []byte("v"))
	require.NoError(t, err)
	assert.False(t, ok)

	version, ok, err := be.CompareAndSwap(ctx, "c", backend.VersionAbsent, []byte("v"))
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Equal(t, backend.VersionAbsent, version)

	for _, key := range []string{"p", "c"} {
		_, err := be.Read(ctx, key)
		require.ErrorIs(t, err, backend.ErrNotExist, "a lost race stores nothing")
	}
}

func TestErrTakesPrecedenceOverLose(t *testing.T) {
	t.Parallel()

	be := faultbackend.Wrap(backend.Memory())
	be.Add(faultbackend.Rule{Kind: faultbackend.CompareAndSwap, Lose: true, Err: errInjected})

	_, _, err := be.CompareAndSwap(context.Background(), "c", backend.VersionAbsent, nil)
	require.ErrorIs(t, err, errInjected)
}

// TestAfterSeesOnlyLandedValues guards what an invariant checker relies on: it is shown every value
// that landed, and nothing that did not.
func TestAfterSeesOnlyLandedValues(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := faultbackend.Wrap(backend.Memory())

	var seen []string
	be.Add(faultbackend.Rule{
		Kind:  faultbackend.CompareAndSwap,
		After: func(op faultbackend.Op, data []byte) { seen = append(seen, op.Key+"="+string(data)) },
	})

	v1, ok, err := be.CompareAndSwap(ctx, "k", backend.VersionAbsent, []byte("1"))
	require.NoError(t, err)
	require.True(t, ok)

	_, ok, err = be.CompareAndSwap(ctx, "k", backend.VersionAbsent, []byte("stale"))
	require.NoError(t, err)
	require.False(t, ok, "a genuinely lost race")

	_, ok, err = be.CompareAndSwap(ctx, "k", v1, []byte("2"))
	require.NoError(t, err)
	require.True(t, ok)

	assert.Equal(t, []string{"k=1", "k=2"}, seen)
}

func TestAfterSkipsFailedOperations(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := faultbackend.Wrap(backend.Memory())

	var calls int
	after := func(faultbackend.Op, []byte) { calls++ }
	be.Add(faultbackend.Rule{Kind: faultbackend.Write, Err: errInjected, After: after})
	be.Add(faultbackend.Rule{Kind: faultbackend.PutIfAbsent, Lose: true, After: after})
	be.Add(faultbackend.Rule{Kind: faultbackend.Read, After: after})

	require.ErrorIs(t, be.Write(ctx, "k", nil), errInjected)
	_, err := be.PutIfAbsent(ctx, "k", nil)
	require.NoError(t, err)
	_, err = be.Read(ctx, "k")
	require.ErrorIs(t, err, backend.ErrNotExist)

	assert.Zero(t, calls)
}

func TestAfterRunsForEveryKind(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := faultbackend.Wrap(backend.Memory())

	var seen []faultbackend.Kind
	for _, kind := range []faultbackend.Kind{
		faultbackend.Read, faultbackend.Write, faultbackend.PutIfAbsent, faultbackend.CompareAndSwap,
		faultbackend.ReadVersioned, faultbackend.List, faultbackend.Delete,
	} {
		be.Add(faultbackend.Rule{Kind: kind, After: func(op faultbackend.Op, _ []byte) { seen = append(seen, op.Kind) }})
	}

	require.NoError(t, be.Write(ctx, "w", []byte("v")))
	_, err := be.Read(ctx, "w")
	require.NoError(t, err)
	_, err = be.PutIfAbsent(ctx, "p", nil)
	require.NoError(t, err)
	_, _, err = be.CompareAndSwap(ctx, "c", backend.VersionAbsent, nil)
	require.NoError(t, err)
	_, _, err = be.ReadVersioned(ctx, "w")
	require.NoError(t, err)
	_, err = be.List(ctx, "")
	require.NoError(t, err)
	require.NoError(t, be.Delete(ctx, "w"))

	assert.Equal(t, []faultbackend.Kind{
		faultbackend.Write, faultbackend.Read, faultbackend.PutIfAbsent, faultbackend.CompareAndSwap,
		faultbackend.ReadVersioned, faultbackend.List, faultbackend.Delete,
	}, seen)
}

func TestAfterSeesReplacedRead(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := faultbackend.Wrap(backend.Memory())
	require.NoError(t, be.Write(ctx, "k", []byte("v")))

	var got []byte
	be.Add(faultbackend.Rule{
		Kind:    faultbackend.Read,
		Replace: func(_ faultbackend.Op, data []byte) []byte { return append(data, '!') },
		After:   func(_ faultbackend.Op, data []byte) { got = data },
	})

	_, err := be.Read(ctx, "k")
	require.NoError(t, err)
	assert.Equal(t, []byte("v!"), got, "After sees what the caller sees")
}

// TestGateRuleAllHoldsEveryMatch covers the hold-all mode: every matching operation waits for the
// release, not only the first.
func TestGateRuleAllHoldsEveryMatch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := faultbackend.Wrap(backend.Memory())
	gate := faultbackend.NewGate()
	be.Add(gate.RuleAll(faultbackend.Write, nil))

	const writers = 3

	var wg sync.WaitGroup
	for i := range writers {
		wg.Go(func() { assert.NoError(t, be.Write(ctx, strconv.Itoa(i), []byte("v"))) })
	}

	gate.Await(t)

	// An operation is recorded before its hold, so once all are recorded all are held.
	require.Eventually(t, func() bool {
		return be.Count(func(op faultbackend.Op) bool { return op.Kind == faultbackend.Write }) == writers
	}, 10*time.Second, time.Millisecond)

	keys, err := be.List(ctx, "")
	require.NoError(t, err)
	assert.Empty(t, keys, "no held write reaches the store before release")

	gate.Release()
	wg.Wait()

	keys, err = be.List(ctx, "")
	require.NoError(t, err)
	assert.Len(t, keys, writers)
}

func TestGateRuleHoldsOnlyTheFirst(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := faultbackend.Wrap(backend.Memory())
	gate := faultbackend.NewGate()
	be.Add(gate.Rule(faultbackend.Write, nil))

	var wg sync.WaitGroup
	wg.Go(func() { assert.NoError(t, be.Write(ctx, "held", nil)) })

	gate.Await(t)
	require.NoError(t, be.Write(ctx, "free", nil), "a later match is not held")

	gate.Release()
	wg.Wait()
}
