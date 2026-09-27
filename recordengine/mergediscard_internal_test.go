package recordengine

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/backend/faultbackend"
	"github.com/oteldb/storage/internal/memlimit"
	"github.com/oteldb/storage/internal/partid"
	"github.com/oteldb/storage/internal/watermark"
)

var errInjected = errors.New("injected")

// compactParts is [Engine.compact] with the abandoned outputs deleted, as [Engine.merge] does.
func (e *Engine) compactParts(ctx context.Context, src []*part, start, capBytes int64, grant *mergeGrant) ([]*part, error) {
	out, abandoned, err := e.compact(ctx, src, start, capBytes, grant)
	e.discardOutputs(ctx, abandoned)

	return out, err
}

// unindexedParts returns the part prefixes under the engine's prefix that its part set does not name.
func unindexedParts(t *testing.T, e *Engine) map[string]struct{} {
	t.Helper()

	root := e.cfg.Prefix + "/"
	keys, err := e.cfg.Backend.List(context.Background(), root)
	require.NoError(t, err)

	live := make(map[string]struct{})
	for _, p := range e.PartPrefixes() {
		live[p] = struct{}{}
	}

	out := make(map[string]struct{})

	for _, k := range keys {
		dir, _, ok := strings.Cut(strings.TrimPrefix(k, root), "/")
		if !ok {
			continue
		}

		if _, err := partid.Parse(dir); err != nil {
			continue
		}

		if _, ok := live[root+dir]; !ok {
			out[root+dir] = struct{}{}
		}
	}

	return out
}

// TestFailedMergeDeletesItsOutputs: a merge that fails once it has written parts deletes every one of
// them, so a merge failing the same way each cycle leaves nothing behind. Only a commit that may have
// landed keeps them, since the index may name them.
func TestFailedMergeDeletesItsOutputs(t *testing.T) {
	t.Parallel()

	const failAt = 3

	// outputOp matches an operation on the failAt-th output part the run touches with it.
	outputOp := func(e *Engine, sources map[string]struct{}, suffix func(prefix string) string) func(faultbackend.Op) bool {
		var seen int

		root := e.cfg.Prefix + "/"

		return func(op faultbackend.Op) bool {
			dir, _, ok := strings.Cut(strings.TrimPrefix(op.Key, root), "/")
			if !ok {
				return false
			}

			prefix := root + dir
			if _, src := sources[prefix]; src || op.Key != suffix(prefix) {
				return false
			}

			seen++

			return seen == failAt
		}
	}

	for _, tc := range []struct {
		name string
		rule func(e *Engine, sources map[string]struct{}) faultbackend.Rule
		kept bool
	}{
		{"open", func(e *Engine, sources map[string]struct{}) faultbackend.Rule {
			return faultbackend.Rule{Kind: faultbackend.Read, Err: errInjected,
				Match: outputOp(e, sources, func(p string) string { return p + "/manifest" })}
		}, false},
		{"finish", func(e *Engine, sources map[string]struct{}) faultbackend.Rule {
			return faultbackend.Rule{Kind: faultbackend.Write, Err: errInjected,
				Match: outputOp(e, sources, watermark.Key)}
		}, false},
		{"commit conflict", func(e *Engine, _ map[string]struct{}) faultbackend.Rule {
			return faultbackend.Rule{Kind: faultbackend.CompareAndSwap, Lose: true,
				Match: func(op faultbackend.Op) bool { return op.Key == e.indexKey() }}
		}, false},
		{"commit unknown", func(e *Engine, _ map[string]struct{}) faultbackend.Rule {
			return faultbackend.Rule{Kind: faultbackend.CompareAndSwap, Err: errInjected,
				Match: func(op faultbackend.Op) bool { return op.Key == e.indexKey() }}
		}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			be := faultbackend.Wrap(backend.Memory())
			e := wideDictEngine(t, be, Config{MergeMemoryBytes: -1}, 3, 4, 8<<10, 4)

			before := e.PartPrefixes()
			sources := make(map[string]struct{}, len(before))

			for _, p := range before {
				sources[p] = struct{}{}
			}

			require.Empty(t, unindexedParts(t, e))

			var leftover int

			for range 3 {
				be.Reset()
				be.Add(tc.rule(e, sources))

				require.Error(t, e.MergeWith(ctx, MergeOptions{Force: true}))
				assert.Equal(t, before, e.PartPrefixes(), "a failed merge keeps its sources")

				n := len(unindexedParts(t, e))
				if tc.kept {
					assert.Greater(t, n, leftover, "a commit that may have landed keeps what it wrote")
				} else {
					assert.Zero(t, n, "a failed merge leaves no part behind")
				}

				leftover = n
			}

			be.Reset()
			require.NoError(t, e.MergeWith(ctx, MergeOptions{Force: true}))

			after := e.PartPrefixes()
			require.GreaterOrEqual(t, len(after), failAt, "the merge writes enough parts to fail past the first")

			for _, p := range before {
				assert.NotContains(t, after, p)
			}
		})
	}
}

// stalledDeletes is a backend whose Delete, once stalled, answers only when its context ends.
type stalledDeletes struct {
	backend.Backend

	stalled  atomic.Bool
	onDelete func()
}

func (b *stalledDeletes) Delete(ctx context.Context, key string) error {
	if !b.stalled.Load() {
		return b.Backend.Delete(ctx, key)
	}

	b.onDelete()
	<-ctx.Done()

	return ctx.Err()
}

// TestFailedMergeCleanupIsBounded: a failed merge whose output deletes never answer returns after
// [discardTimeout], and holds neither flushMu nor its grant while it waits.
func TestFailedMergeCleanupIsBounded(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		const budget = 1 << 30

		ctx := context.Background()
		pool := memlimit.NewPool(budget)
		faults := faultbackend.Wrap(backend.Memory())
		be := &stalledDeletes{Backend: faults}
		e := wideDictEngine(t, be, Config{MergeMemoryBytes: budget, MergeAdmission: poolAdmission(pool)}, 3, 4, 8<<10, 4)

		var deletes, flushFree, grantFree int

		be.onDelete = func() {
			deletes++

			if e.flushMu.TryLock() {
				flushFree++

				e.flushMu.Unlock()
			}

			if release, ok := pool.TryAcquire(budget); ok {
				grantFree++

				release()
			}
		}

		faults.Add(faultbackend.Rule{Kind: faultbackend.CompareAndSwap, Lose: true,
			Match: func(op faultbackend.Op) bool { return op.Key == e.indexKey() }})
		be.stalled.Store(true)

		start := time.Now()
		err := e.MergeWith(ctx, MergeOptions{Force: true})

		require.ErrorIs(t, err, bucketindex.ErrConflict)
		assert.Equal(t, discardTimeout, time.Since(start), "the cleanup gives up at its deadline")
		assert.Equal(t, 1, deletes, "the cleanup stops at its deadline rather than trying every part")
		assert.Equal(t, 1, flushFree, "flushMu is released before the cleanup")
		assert.Equal(t, 1, grantFree, "the grant is released before the cleanup")
	})
}
