package enginetest

import (
	"context"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/backend/faultbackend"
	"github.com/oteldb/storage/internal/obs/obstest"
)

// staleLoad is the shape #685 needs: e holds only part A, while the committed index names A, B and C
// because another writer committed B and C after e last loaded. B's manifest is the one a load of e
// fails on.
type staleLoad struct {
	be   *faultbackend.Backend
	e    Engine
	b, c string
}

// breakPart makes the part whose manifest is at key unopenable.
type breakPart func(ctx context.Context, be *faultbackend.Backend, manifest string)

// newStaleLoad builds a [staleLoad]. breakB runs once B is committed and before C is, so it can make
// B unopenable for e the way the test needs (corrupt it, or fail its next read).
func (k Kind) newStaleLoad(t *testing.T, cfg Config, breakB breakPart) staleLoad {
	t.Helper()

	ctx := context.Background()
	be := faultbackend.Wrap(backend.Memory())
	cfg.Backend = be

	w := k.open(t, be)
	w.Append(t, api(100, 1))
	require.NoError(t, w.Flush(ctx))

	e := k.Open(t, cfg)
	require.NoError(t, e.LoadParts(ctx))

	b := k.flushNew(t, w, be, api(200, 2))
	breakB(ctx, be, k.manifestOf(t, be, b))

	c := k.flushNew(t, w, be, web(300, 3))

	return staleLoad{be: be, e: e, b: k.Prefix + "/" + b, c: k.Prefix + "/" + c}
}

func (k Kind) manifestOf(t *testing.T, be backend.Backend, id string) string {
	t.Helper()

	keys := k.partKeys(context.Background(), t, be, id)
	i := slices.IndexFunc(keys, func(key string) bool { return strings.HasSuffix(key, "/manifest") })
	require.GreaterOrEqual(t, i, 0)

	return keys[i]
}

// flushNew flushes rows through w and returns the id of the one part the flush added.
func (k Kind) flushNew(t *testing.T, w Engine, be backend.Backend, rows ...Row) string {
	t.Helper()

	ctx := context.Background()
	before := k.partDirs(ctx, t, be)

	w.Append(t, rows...)
	require.NoError(t, w.Flush(ctx))

	var added []string

	for _, id := range k.partDirs(ctx, t, be) {
		if !slices.Contains(before, id) {
			added = append(added, id)
		}
	}

	require.Len(t, added, 1)

	return added[0]
}

func corruptObject(t *testing.T) breakPart {
	t.Helper()

	return func(ctx context.Context, be *faultbackend.Backend, key string) {
		require.NoError(t, be.Write(ctx, key, []byte("not a manifest")))
	}
}

func failNextRead(_ context.Context, be *faultbackend.Backend, key string) {
	be.Add(faultbackend.Rule{
		Kind: faultbackend.Read, Match: func(op faultbackend.Op) bool { return op.Key == key },
		Err: errReadRejected, Times: 1,
	})
}

// failedLoadKeepsUnopenedParts is #685: a load that fails on a part it cannot open must not leave the
// engine conditioned on the new index while it still holds the old part set, or its next commit
// passes the compare-and-swap and drops every entry it never opened.
func failedLoadKeepsUnopenedParts(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	s := k.newStaleLoad(t, Config{}, corruptObject(t))

	require.Error(t, s.e.LoadParts(ctx), "B's manifest is corrupt")

	s.e.Append(t, api(400, 4))
	require.ErrorIs(t, s.e.Flush(ctx), bucketindex.ErrFenced)

	names := prefixes(k.loadIndex(t, s.be).Entries)
	require.Contains(t, names, s.b, "the part the load could not open is still committed")
	require.Contains(t, names, s.c, "and so is the one another writer committed")

	assert.Equal(t, 1, s.e.HeadRows(), "the refused flush keeps its rows in the head")
	assert.Equal(t, []Row{api(100, 1), api(400, 4)}, rows(t, s.e, apiStream),
		"reads serve the part set that last loaded, and the head")
}

// failedLoadChangesNothing: a load is all-or-nothing, so one that fails leaves every field it would
// have replaced as it was, the adopted foreign entries included.
func failedLoadChangesNothing(t *testing.T, k Kind) {
	t.Helper()

	for _, tc := range []struct {
		name  string
		setup func(t *testing.T) Engine
	}{
		{"unopenable part", func(t *testing.T) Engine {
			t.Helper()

			ctx := context.Background()
			s := k.newStaleLoad(t, Config{}, corruptObject(t))
			// A flush on the stale view rebases, which is what gives the engine foreign entries.
			s.e.Append(t, api(400, 4))
			require.NoError(t, s.e.Flush(ctx))

			// And another writer's commit after it moves the index past the version e holds.
			w := k.open(t, s.be)
			w.Append(t, web(500, 5))
			require.NoError(t, w.Flush(ctx))

			return s.e
		}},
		{"unreadable index", func(t *testing.T) Engine {
			t.Helper()

			be := faultbackend.Wrap(backend.Memory())
			e := k.open(t, be)
			k.twoParts(t, e, be)

			be.Add(faultbackend.Rule{
				Kind: faultbackend.ReadVersioned, Err: errReadRejected,
				Match: func(op faultbackend.Op) bool { return op.Key == k.indexKey() },
			})

			return e
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := tc.setup(t)
			before := e.LoadState()

			require.Error(t, e.LoadParts(context.Background()))
			assert.Equal(t, before, e.LoadState())
			assert.True(t, e.Stats().IndexFenced)
		})
	}
}

// fenceLiftsOnReload: a load that failed transiently fences the engine only until the retry that
// succeeds, and the flush it refused meanwhile then commits alongside everything the index named.
func fenceLiftsOnReload(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	s := k.newStaleLoad(t, Config{}, failNextRead)

	require.ErrorIs(t, s.e.LoadParts(ctx), errReadRejected)
	require.True(t, s.e.Stats().IndexFenced)

	s.e.Append(t, api(400, 4))
	require.ErrorIs(t, s.e.Flush(ctx), bucketindex.ErrFenced)

	require.NoError(t, s.e.ReloadFenced(ctx))
	require.False(t, s.e.Stats().IndexFenced)
	require.NoError(t, s.e.Flush(ctx))

	names := prefixes(k.loadIndex(t, s.be).Entries)
	assert.Len(t, names, 4, "A, B, C and the flush the fence held back")
	assert.Contains(t, names, s.b)
	assert.Contains(t, names, s.c)
	assert.Equal(t, []Row{api(100, 1), api(200, 2), api(400, 4)}, rows(t, s.e, apiStream))
	assert.Equal(t, []Row{web(300, 3)}, rows(t, s.e, "web"))

	require.NoError(t, s.e.ReloadFenced(ctx), "an engine that is not fenced has nothing to retry")
}

// persistentFenceStaysFenced: a part that stays unopenable keeps the engine fenced across every
// retry, commits nothing, and says so on each attempt.
func persistentFenceStaysFenced(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	o, m := obstest.New(t)
	s := k.newStaleLoad(t, Config{Obs: o}, corruptObject(t))

	require.Error(t, s.e.LoadParts(ctx))

	commits := s.be.Count(k.indexCommit)
	s.e.Append(t, api(400, 4))

	const retries = 3
	for range retries {
		require.Error(t, s.e.ReloadFenced(ctx))
		require.True(t, s.e.Stats().IndexFenced)
		require.ErrorIs(t, s.e.Flush(ctx), bucketindex.ErrFenced)
	}

	assert.Equal(t, commits, s.be.Count(k.indexCommit), "a fenced engine commits nothing")
	assert.EqualValues(t, 1+retries, m.Counter("storage.index.fenced_loads"), "every failed load is counted")
	assert.Equal(t, 1, s.e.HeadRows())
}

// everyCommitterIsFenced: each path that commits an index refuses while the engine is fenced, and
// the ones with work to do before the commit refuse before doing it.
func everyCommitterIsFenced(t *testing.T, k Kind) {
	t.Helper()

	for _, tc := range []struct {
		name string
		run  func(ctx context.Context, t *testing.T, e Engine) error
	}{
		{"flush", func(ctx context.Context, t *testing.T, e Engine) error {
			t.Helper()
			e.Append(t, api(400, 4))

			return e.Flush(ctx)
		}},
		{"merge", func(ctx context.Context, _ *testing.T, e Engine) error { return e.Merge(ctx, 0) }},
		{"retention", func(ctx context.Context, _ *testing.T, e Engine) error { return e.Merge(ctx, 1<<62) }},
		{"admin compaction", func(ctx context.Context, _ *testing.T, e Engine) error { return e.ForceMerge(ctx) }},
		{"repair", func(ctx context.Context, _ *testing.T, e Engine) error {
			e.AdoptWants([]bucketindex.Want{{Prefix: k.Prefix + "/0000009998"}})

			return e.Merge(ctx, 0)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			f := AnswerAlways(bucketindex.WantAbsent, nil)
			s := k.newStaleLoad(t, Config{Repair: f}, corruptObject(t))
			require.Error(t, s.e.LoadParts(ctx))

			before := k.loadIndex(t, s.be)
			commits := s.be.Count(k.indexCommit)

			require.ErrorIs(t, tc.run(ctx, t, s.e), bucketindex.ErrFenced)

			assert.Equal(t, commits, s.be.Count(k.indexCommit))
			assert.Equal(t, before.Generation, k.loadIndex(t, s.be).Generation)
			assert.Zero(t, f.Calls(), "repair fetches nothing it could not commit")
		})
	}
}

// fencedMidway pins the fence inside the commit itself: a load that fails while a flush or merge is
// off the lock writing its part must still stop that commit. It leaves e holding two parts while the
// index also names a corrupt one of another writer's, runs op until its first part write, fails a
// load, and returns op's result.
func (k Kind) fencedMidway(t *testing.T, op func(ctx context.Context, e Engine) error) (*faultbackend.Backend, Engine, error) {
	t.Helper()

	ctx := context.Background()
	be := faultbackend.Wrap(backend.Memory())
	e := k.open(t, be)
	k.twoParts(t, e, be)

	w := k.open(t, be)
	corruptObject(t)(ctx, be, k.manifestOf(t, be, k.flushNew(t, w, be, web(300, 3))))

	gate := faultbackend.NewGate()
	be.Add(gate.Rule(faultbackend.Write, func(op faultbackend.Op) bool {
		return strings.HasPrefix(op.Key, k.Prefix+"/")
	}))

	var (
		wg    sync.WaitGroup
		opErr error
	)

	wg.Go(func() { opErr = op(ctx, e) })

	gate.Await(t)
	require.Error(t, e.LoadParts(ctx))
	gate.Release()
	wg.Wait()

	return be, e, opErr
}

func mergeFencedMidwayRollsBack(t *testing.T, k Kind) {
	t.Helper()

	be, e, err := k.fencedMidway(t, func(ctx context.Context, e Engine) error { return e.ForceMerge(ctx) })
	require.ErrorIs(t, err, bucketindex.ErrFenced)

	assert.Len(t, k.loadIndex(t, be).Entries, 3, "the merge committed nothing")
	assert.Equal(t, 2, e.PartCount(), "and its sources are still the live set")
	assert.Equal(t, []Row{api(100, 1), api(200, 2)}, rows(t, e, apiStream))
}

func flushFencedMidwayKeepsRows(t *testing.T, k Kind) {
	t.Helper()

	be, e, err := k.fencedMidway(t, func(ctx context.Context, e Engine) error {
		e.Append(t, api(400, 4))

		return e.Flush(ctx)
	})
	require.ErrorIs(t, err, bucketindex.ErrFenced)

	assert.Len(t, k.loadIndex(t, be).Entries, 3, "the flush committed nothing")
	assert.Equal(t, 2, e.PartCount(), "nor published its part")
	assert.Equal(t, 1, e.HeadRows(), "its rows are back in the head")
	assert.Equal(t, []Row{api(100, 1), api(200, 2), api(400, 4)}, rows(t, e, apiStream))
}

// reloadKeepsUncommittedFlush: a flush whose commit failed has already moved its rows out of the
// head into a part no index names. A reload in between must keep that part for the next commit, or
// the rows are readable nowhere until a restart replays the WAL.
func reloadKeepsUncommittedFlush(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	be := faultbackend.Wrap(backend.Memory())
	e := k.open(t, be)

	e.Append(t, api(100, 1))
	be.Add(faultbackend.Rule{Kind: faultbackend.CompareAndSwap, Match: k.indexCommit, Lose: true})
	require.ErrorIs(t, e.Flush(ctx), bucketindex.ErrConflict)
	be.Reset()

	require.NoError(t, e.RefreshReplica(ctx))
	assert.Equal(t, []Row{api(100, 1)}, rows(t, e, apiStream))

	e.Append(t, api(200, 2))
	require.NoError(t, e.Flush(ctx))

	r := k.open(t, be)
	require.NoError(t, r.LoadParts(ctx))
	assert.Equal(t, []Row{api(100, 1), api(200, 2)}, rows(t, r, apiStream), "the next commit published it")
}
