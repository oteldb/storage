package recordengine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/compress"
)

// TestAdmissionReservesWhatTheSourcesNeed: a merge whose sources, encoder and writers' floor need more
// than its share asks admission for that, not the share, so it never holds more than it reserved.
func TestAdmissionReservesWhatTheSourcesNeed(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	var asked []int64

	admit := func(_ context.Context, bytes int64, _ bool) (func(), bool, error) {
		asked = append(asked, bytes)

		return func() {}, true, nil
	}

	const share = 1 << 20

	e := wideDictEngine(t, backend.Memory(), Config{MergeMemoryBytes: share, MergeAdmission: admit}, 3, 4, 8<<10, 2)
	need, ok := e.mergeNeed(ctx, e.parts, e.mergeCapBytes())
	require.True(t, ok)
	require.Greater(t, need, e.mergeMemoryBudgetBytes(), "the sources must need more than the share for the test to mean anything")

	require.NoError(t, e.MergeWith(ctx, MergeOptions{Force: true}))
	require.NotEmpty(t, asked)
	assert.Greater(t, asked[0], e.mergeMemoryBudgetBytes(), "the merge must reserve what it needs")
}

// TestOpenGrantedCoversAShortGrant: a grant the opened sources outgrow — one reserved without a
// manifest bound — is topped up without waiting, declined for a merge that may not wait, and
// otherwise handed back and waited for whole.
func TestOpenGrantedCoversAShortGrant(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := wideDictEngine(t, backend.Memory(), Config{MergeMemoryBytes: -1}, 3, 4, 8<<10, 2)
	comp := compress.NewCompressor(compress.AlgorithmZSTD, compress.LevelDefault)

	for _, tc := range []struct {
		name         string
		busy, wait   bool
		declined     bool
		wantWaitLast bool
	}{
		{name: "free", busy: false, wait: false},
		{name: "busy background", busy: true, wait: false, declined: true},
		{name: "busy waiting", busy: true, wait: true, wantWaitLast: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var waits []bool

			admit := func(_ context.Context, _ int64, wait bool) (func(), bool, error) {
				waits = append(waits, wait)
				if tc.busy && !wait {
					return nil, false, nil
				}

				return func() {}, true, nil
			}

			g := &mergeGrant{bytes: 1, release: func() {}, wait: tc.wait, admit: admit}

			sources, limit, _, err := e.openGranted(ctx, e.parts, g, comp, 0)
			if tc.declined {
				require.ErrorIs(t, err, errMergeDeclined)

				return
			}

			require.NoError(t, err)
			require.Len(t, sources, len(e.parts))

			_, reserve, need := mergeWriterBudget(g.bytes, sources, comp, 0)
			assert.GreaterOrEqual(t, g.bytes, need, "the grant must cover what the opened sources need")
			assert.GreaterOrEqual(t, limit, 2*reserve)
			assert.Equal(t, tc.wantWaitLast, waits[len(waits)-1])
		})
	}
}

func TestMergeGrantTopAndRegrant(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type call struct {
		bytes int64
		wait  bool
	}

	var (
		calls    []call
		released []int64
		refuse   bool
	)

	admit := func(_ context.Context, bytes int64, wait bool) (func(), bool, error) {
		calls = append(calls, call{bytes, wait})
		if refuse && !wait {
			return nil, false, nil
		}

		return func() { released = append(released, bytes) }, true, nil
	}

	g := &mergeGrant{bytes: 10, release: func() { released = append(released, 10) }, wait: true, admit: admit}

	ok, err := g.top(ctx, 5)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, int64(15), g.bytes)
	assert.Equal(t, []call{{5, false}}, calls, "a top-up never waits")

	refuse = true

	ok, err = g.top(ctx, 5)
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Equal(t, int64(15), g.bytes)

	require.NoError(t, g.regrant(ctx, 30))
	assert.ElementsMatch(t, []int64{10, 5}, released, "the grant is handed back before queueing for the whole")
	assert.Equal(t, call{30, true}, calls[len(calls)-1])
	assert.Equal(t, int64(30), g.bytes)

	g.done()
	g.done()
	assert.ElementsMatch(t, []int64{10, 5, 30}, released, "done hands back once")

	unbounded := &mergeGrant{bytes: 10}
	ok, err = unbounded.top(ctx, 5)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, int64(15), unbounded.bytes, "without admission the grant only records what the merge holds")
}
