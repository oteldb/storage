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
	"github.com/oteldb/storage/wal"
)

// TestMidFlushAppendSurvivesCrash is #467: a sample appended while a flush writes its part off-lock
// is in no part, so the flush's WAL checkpoint must not discard the segment holding it. The
// interleaving is stated with a gate rather than raced for.
func TestMidFlushAppendSurvivesCrash(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := faultbackend.Wrap(backend.Memory())
	walDir := t.TempDir()

	sw, err := wal.Create(walDir, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sw.Close() })

	e := engine.New(engine.Config{WAL: sw, Backend: be, Prefix: "t/m"})
	s := mkSeries("job", "api")
	mustAppend(t, e, s, 100, 1)

	gate := faultbackend.NewGate()
	be.Add(gate.Rule(faultbackend.Write, func(op faultbackend.Op) bool {
		return strings.HasPrefix(op.Key, "t/m/")
	}))

	var (
		wg       sync.WaitGroup
		flushErr error
	)

	wg.Go(func() { flushErr = e.Flush(ctx) })

	gate.Await(t) // the head is detached and the part objects are being written
	be.Reset()

	mustAppend(t, e, s, 200, 2) // acknowledged mid-flush

	gate.Release()
	wg.Wait()
	require.NoError(t, flushErr)
	require.NoError(t, sw.Sync())
	require.NoError(t, sw.Close()) // crash: the head is gone, the backend and WAL dir survive

	sw2, err := wal.Create(walDir, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sw2.Close() })

	restored := engine.New(engine.Config{WAL: sw2, Backend: be, Prefix: "t/m"})
	require.NoError(t, restored.LoadParts(ctx))
	require.NoError(t, restored.Replay(walDir))

	got := fetchAll(t, restored, fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("job", "api")}})
	require.Len(t, got, 1)
	assert.Equal(t, []int64{100, 200}, got[0].Timestamps,
		"the flushed sample comes from the part and the mid-flush one from the kept segment, each once")
}
