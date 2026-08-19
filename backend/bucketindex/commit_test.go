package bucketindex_test

import (
	"context"
	"slices"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/backend/faultbackend"
)

// TestSaveDoesNotDropAConcurrentWritersPart isolates the commit itself from the engines above it.
// Two writers load one index, each adds a part, and each saves. A save may lose the race — but it
// must then say so, so its caller can reload and retry. Reporting success while dropping the other
// writer's entry is the failure this asserts against, and is what a shared object store gets today
// (see #392): the entry names a part whose objects are in the store, durable and unreachable.
//
// The assertion is deliberately loose about the mechanism — a conditional write and a
// generation-named object both satisfy it.
func TestSaveDoesNotDropAConcurrentWritersPart(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	const key = "default/metrics/" + bucketindex.Object

	base := &bucketindex.Index{}
	base.Add(bucketindex.Entry{Prefix: "default/metrics/0000000000", MinTime: 1, MaxTime: 2})
	require.NoError(t, base.Save(ctx, be, key))

	a, err := bucketindex.Load(ctx, be, key)
	require.NoError(t, err)
	b, err := bucketindex.Load(ctx, be, key)
	require.NoError(t, err)

	a.Add(bucketindex.Entry{Prefix: "default/metrics/0000000001", MinTime: 3, MaxTime: 4})
	b.Add(bucketindex.Entry{Prefix: "default/metrics/0000000002", MinTime: 5, MaxTime: 6})

	require.NoError(t, a.Save(ctx, be, key))

	if err := b.Save(ctx, be, key); err != nil {
		return // The loser is told, which is all this asks for.
	}

	got, err := bucketindex.Load(ctx, be, key)
	require.NoError(t, err)

	var prefixes []string
	for _, e := range got.Entries {
		prefixes = append(prefixes, e.Prefix)
	}

	require.Contains(t, prefixes, "default/metrics/0000000001",
		"a save reporting success must not drop an entry committed since it was prepared")
	require.Contains(t, prefixes, "default/metrics/0000000002")
}

// entry is a part entry naming prefix, with a distinct time range per part.
func entry(prefix string, minTime int64) bucketindex.Entry {
	return bucketindex.Entry{Prefix: prefix, MinTime: minTime, MaxTime: minTime + 1}
}

// prefixesOf lists the parts an index names, in index order.
func prefixesOf(ix *bucketindex.Index) []string {
	out := make([]string, 0, len(ix.Entries))
	for _, e := range ix.Entries {
		out = append(out, e.Prefix)
	}

	return out
}

// addPart returns a build function committing base's parts plus one more.
func addPart(prefix string, minTime int64) func(*bucketindex.Index, bucketindex.Generation) *bucketindex.Index {
	return func(base *bucketindex.Index, g bucketindex.Generation) *bucketindex.Index {
		ix := &bucketindex.Index{Entries: slices.Clone(base.Entries), Generation: g}
		ix.Add(entry(prefix, minTime))

		return ix
	}
}

const indexKey = "default/metrics/" + bucketindex.Object

func TestGenerationKeyOrdersByGeneration(t *testing.T) {
	t.Parallel()

	g := bucketindex.Generation{Term: 3, Counter: 17}
	key := bucketindex.GenerationKey(indexKey, g)

	assert.Equal(t, "default/metrics/bucket-index/0000000000000003-0000000000000011.bin", key)
	assert.True(t, bucketindex.IsGenerationKey(key))
	assert.Less(t, key, bucketindex.GenerationKey(indexKey, bucketindex.Generation{Term: 3, Counter: 18}))
	assert.Less(t, key, bucketindex.GenerationKey(indexKey, bucketindex.Generation{Term: 4}))
}

func TestIsGenerationKeyRejectsOtherObjects(t *testing.T) {
	t.Parallel()

	for _, key := range []string{
		indexKey,
		"default/metrics/0000000001/manifest",
		"default/metrics/bucket-index/not-a-generation.bin",
		"default/metrics/bucket-index/0000000000000003.bin",
		"default/metrics/other/0000000000000003-0000000000000011.bin",
		"",
	} {
		assert.False(t, bucketindex.IsGenerationKey(key), key)
	}
}

// TestLoadResolvesAClaimTheKeyDoesNotName covers the crash between claiming a generation and
// refreshing the conventional key: the claim is the commit, so a load must find it — and repair
// the key, which is all a reader that knows only the old layout ever sees.
func TestLoadResolvesAClaimTheKeyDoesNotName(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()

	old := &bucketindex.Index{}
	old.Add(entry("default/metrics/0000000000", 1))
	require.NoError(t, old.Save(ctx, be, indexKey))

	newer := &bucketindex.Index{Generation: old.Generation.Next(0)}
	newer.Add(entry("default/metrics/0000000001", 3))

	claimed, err := be.PutIfAbsent(ctx, bucketindex.GenerationKey(indexKey, newer.Generation), newer.AppendBinary(nil))
	require.NoError(t, err)
	require.True(t, claimed)

	got, err := bucketindex.Load(ctx, be, indexKey)
	require.NoError(t, err)
	assert.Equal(t, []string{"default/metrics/0000000001"}, prefixesOf(got))

	raw, err := be.Read(ctx, indexKey)
	require.NoError(t, err)
	repaired, err := bucketindex.Decode(raw)
	require.NoError(t, err)
	assert.Equal(t, newer.Generation, repaired.Generation, "the conventional key must be repaired to what resolved")
}

// TestLoadKeepsAnIndexWrittenWithNoGeneration is the upgrade path: a prefix an older build wrote
// carries the conventional object and no claims at all.
func TestLoadKeepsAnIndexWrittenWithNoGeneration(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()

	legacy := &bucketindex.Index{}
	legacy.Add(entry("default/metrics/0000000000", 1))
	require.NoError(t, be.Write(ctx, indexKey, legacy.AppendBinary(nil)))

	got, err := bucketindex.Load(ctx, be, indexKey)
	require.NoError(t, err)
	assert.Equal(t, []string{"default/metrics/0000000000"}, prefixesOf(got))
	assert.True(t, got.Generation.Zero())

	// And the first commit over it supersedes without a migration step.
	got.Add(entry("default/metrics/0000000001", 3))
	require.NoError(t, got.Save(ctx, be, indexKey))

	after, err := bucketindex.Load(ctx, be, indexKey)
	require.NoError(t, err)
	assert.Equal(t, []string{"default/metrics/0000000000", "default/metrics/0000000001"}, prefixesOf(after))
}

// TestLoadPrefersTheNewerOfKeyAndClaims covers a rollback to a build that overwrites the
// conventional key without claiming: what it wrote is newer, and stays the answer.
func TestLoadPrefersTheNewerOfKeyAndClaims(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()

	committed := &bucketindex.Index{}
	committed.Add(entry("default/metrics/0000000000", 1))
	require.NoError(t, committed.Save(ctx, be, indexKey))

	ahead := &bucketindex.Index{Generation: committed.Generation.Next(0).Next(0)}
	ahead.Add(entry("default/metrics/0000000009", 9))
	require.NoError(t, be.Write(ctx, indexKey, ahead.AppendBinary(nil)))

	got, err := bucketindex.Load(ctx, be, indexKey)
	require.NoError(t, err)
	assert.Equal(t, []string{"default/metrics/0000000009"}, prefixesOf(got))
}

// TestCommitRetriesOntoTheWinnersState states the interleaving the shared store produces: one
// writer's claim is suspended inside the backend, the other commits under it, and the suspended
// one must be told it lost, rebuild on what got committed, and keep the winner's part.
func TestCommitRetriesOntoTheWinnersState(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := faultbackend.Wrap(backend.Memory())

	gate := faultbackend.NewGate()
	be.Add(gate.Rule(faultbackend.PutIfAbsent, nil))

	base := &bucketindex.Index{}
	base.Add(entry("default/metrics/0000000000", 1))

	loser := make(chan error, 1)

	go func() {
		_, err := bucketindex.Commit(ctx, be, indexKey, 0, base, addPart("default/metrics/0000000001", 3))
		loser <- err
	}()

	gate.Await(t) // the loser is suspended holding its claim of the next generation

	winner, err := bucketindex.Commit(ctx, be, indexKey, 0, base, addPart("default/metrics/0000000002", 5))
	require.NoError(t, err)
	assert.Equal(t, []string{"default/metrics/0000000000", "default/metrics/0000000002"}, prefixesOf(winner))

	gate.Release()
	require.NoError(t, <-loser)

	got, err := bucketindex.Load(ctx, be, indexKey)
	require.NoError(t, err)
	assert.Equal(t, []string{
		"default/metrics/0000000000",
		"default/metrics/0000000001",
		"default/metrics/0000000002",
	}, prefixesOf(got), "the retry must build on the winner's state, not overwrite it")

	assert.Equal(t, 3, be.Count(func(op faultbackend.Op) bool { return op.Kind == faultbackend.PutIfAbsent }),
		"the loser claims twice: once for the generation it lost, once for the one it won")
}

// alwaysClaimed is a backend whose claims never succeed, as they would not for a writer starved by
// peers committing faster than it can rebuild.
type alwaysClaimed struct{ backend.Backend }

func (alwaysClaimed) PutIfAbsent(context.Context, string, []byte) (bool, error) { return false, nil }

func TestCommitFailsWhenItRunsOutOfAttempts(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := alwaysClaimed{backend.Memory()}

	ix, err := bucketindex.Commit(ctx, be, indexKey, 0, nil, addPart("default/metrics/0000000001", 3))
	require.ErrorIs(t, err, bucketindex.ErrConflict, "an exhausted retry must report the conflict, not success")
	assert.Nil(t, ix)
}

// TestCommitReclaimsSupersededGenerations keeps the directory a load lists bounded: without it the
// LIST every load pays for would grow with the prefix's whole commit history.
func TestCommitReclaimsSupersededGenerations(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()

	ix := &bucketindex.Index{}
	for i := range 40 {
		ix.Add(entry("default/metrics/"+strconv.Itoa(i), int64(i)))
		require.NoError(t, ix.Save(ctx, be, indexKey))
	}

	claims, err := be.List(ctx, "default/metrics/bucket-index/")
	require.NoError(t, err)
	assert.LessOrEqual(t, len(claims), 16, "superseded generations must be reclaimed")

	got, err := bucketindex.Load(ctx, be, indexKey)
	require.NoError(t, err)
	assert.Len(t, got.Entries, 40)
	assert.Equal(t, ix.Generation, got.Generation)
}

// TestConcurrentCommitsAllLand runs the commits genuinely in parallel: every writer must either be
// told it lost or have its part in the committed index, and the retry makes it the second.
func TestConcurrentCommitsAllLand(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()

	const writers = 4

	var (
		wg   sync.WaitGroup
		errs = make([]error, writers)
	)

	for i := range writers {
		wg.Go(func() {
			_, errs[i] = bucketindex.Commit(ctx, be, indexKey, 0, nil,
				addPart("default/metrics/000000000"+strconv.Itoa(i), int64(i)*10+1))
		})
	}

	wg.Wait()

	got, err := bucketindex.Load(ctx, be, indexKey)
	require.NoError(t, err)

	for i := range writers {
		prefix := "default/metrics/000000000" + strconv.Itoa(i)
		if errs[i] != nil {
			require.ErrorIs(t, errs[i], bucketindex.ErrConflict)

			continue
		}

		assert.Contains(t, prefixesOf(got), prefix, "a commit that reported success must be in the index")
	}
}
