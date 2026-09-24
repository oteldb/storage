package enginetest

import (
	"context"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/backend/faultbackend"
)

// gonePartBecomesWant: a part whose objects are permanently absent no longer fails the load — it
// leaves Entries into Wanted, the rest of the part set still serves, and the obligation is in the
// committed index.
func gonePartBecomesWant(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	be := backend.Memory()
	ids := k.twoParts(t, k.open(t, be), be)

	k.erasePart(ctx, t, be, ids[0])

	r := k.open(t, be)
	require.NoError(t, r.LoadParts(ctx), "one gone part must not fail the load")
	require.Equal(t, 1, r.PartCount())
	require.Equal(t, []Row{api(200, 2)}, rows(t, r, apiStream))

	ix := k.loadIndex(t, be)
	require.Equal(t, []string{k.Prefix + "/" + ids[1]}, prefixes(ix.Entries))
	require.Equal(t, []string{k.Prefix + "/" + ids[0]}, wantPrefixes(ix.Wanted))
	require.Empty(t, ix.Removed, "a loss is not a removal")
	require.NotZero(t, ix.Wanted[0].Generation, "the want records when it was discovered")
}

// partiallyGonePartBecomesWant: a part that lost the objects [Kind.Erodes] names cannot answer a
// read, so it is as lost as one with no objects at all — and the open-time orphan sweep spares what
// is left of it, because those objects are the remains of a part repair is owed rather than the
// residue of a failed flush.
func partiallyGonePartBecomesWant(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	be := backend.Memory()
	ids := k.twoParts(t, k.open(t, be), be)

	for _, key := range k.partKeys(ctx, t, be, ids[0]) {
		if k.Erodes(key) {
			require.NoError(t, be.Delete(ctx, key))
		}
	}

	r := k.open(t, be)
	require.NoError(t, r.LoadParts(ctx))

	ix := k.loadIndex(t, be)
	require.Equal(t, []string{k.Prefix + "/" + ids[0]}, wantPrefixes(ix.Wanted))
	require.Equal(t, []string{k.Prefix + "/" + ids[1]}, prefixes(ix.Entries))
	require.NotEmpty(t, k.partKeys(ctx, t, be, ids[0]), "a wanted part's remaining objects are not orphans")
}

// transientOpenFailureRecordsNoWant: a backend that could not answer says nothing about whether the
// part exists, so it fails the load and records nothing. Converting one into a want would drop a
// live part and start a repair for data that was never missing.
func transientOpenFailureRecordsNoWant(t *testing.T, k Kind) {
	t.Helper()

	for _, tc := range []struct {
		name string
		err  error
	}{
		{"timeout", errors.New("i/o timeout")},
		{"canceled", context.Canceled},
		{"no space", errors.New("no space left on device")},
		{"denied", errors.New("AccessDenied")},
		{"throttled", errors.New("SlowDown: please reduce your request rate")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			be := faultbackend.Wrap(backend.Memory())
			ids := k.twoParts(t, k.open(t, be), be)

			before := k.loadIndex(t, be)

			rejectReads(be, "/manifest", tc.err)
			err := k.open(t, be).LoadParts(ctx)
			be.Reset()

			after := k.loadIndex(t, be)
			assert.Empty(t, after.Wanted, "a transient failure is not evidence of loss")
			assert.Equal(t, prefixes(before.Entries), prefixes(after.Entries),
				"a transient failure must not strip live parts from the index")
			assert.Len(t, after.Entries, 2)
			assert.Equal(t, before.Generation, after.Generation, "nothing was committed")
			assert.NotEmpty(t, k.partKeys(ctx, t, be, ids[0]))
			assert.Error(t, err, "a transient failure must still fail the load")
		})
	}
}

// corruptPartIsNotAWant pins the narrow trigger: objects that are present but unreadable are a
// different failure with a different remedy, and widening the trigger to cover them is how a repair
// path becomes a data-destruction path.
func corruptPartIsNotAWant(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	be := backend.Memory()
	ids := k.twoParts(t, k.open(t, be), be)

	objs := k.partKeys(ctx, t, be, ids[0])
	i := slices.IndexFunc(objs, func(key string) bool { return strings.HasSuffix(key, "/manifest") })
	require.GreaterOrEqual(t, i, 0)
	require.NoError(t, be.Write(ctx, objs[i], []byte("not a manifest")))

	require.Error(t, k.open(t, be).LoadParts(ctx))
	require.Empty(t, k.loadIndex(t, be).Wanted)
}

// wantIsRecordedInOneCommit: dropping the entry and recording the want is a single compare-and-swap,
// so there is no window in which a crash could land one without the other.
func wantIsRecordedInOneCommit(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	be := faultbackend.Wrap(backend.Memory())
	ids := k.twoParts(t, k.open(t, be), be)

	k.erasePart(ctx, t, be, ids[0])

	before := be.Count(k.indexCommit)
	require.NoError(t, k.open(t, be).LoadParts(ctx))

	require.Equal(t, 1, be.Count(k.indexCommit)-before, "the drop and the want are one commit, not two")

	ix := k.loadIndex(t, be)
	require.NotContains(t, prefixes(ix.Entries), k.Prefix+"/"+ids[0])
	require.Contains(t, wantPrefixes(ix.Wanted), k.Prefix+"/"+ids[0])
}

// failedWantCommitAppliesNeither: a lost race leaves the stored index untouched, so the retry
// re-reads and re-derives both halves rather than inheriting half of an applied commit.
func failedWantCommitAppliesNeither(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	be := faultbackend.Wrap(backend.Memory())
	ids := k.twoParts(t, k.open(t, be), be)

	before := k.loadIndex(t, be)
	k.erasePart(ctx, t, be, ids[0])

	be.Add(faultbackend.Rule{Kind: faultbackend.CompareAndSwap, Match: k.indexCommit, Lose: true})
	require.Error(t, k.open(t, be).LoadParts(ctx), "a commit that never lands must not be reported as success")
	be.Reset()

	after := k.loadIndex(t, be)
	require.Equal(t, prefixes(before.Entries), prefixes(after.Entries), "the entry is still there")
	require.Empty(t, after.Wanted, "and the want is not")

	require.NoError(t, k.open(t, be).LoadParts(ctx))

	ix := k.loadIndex(t, be)
	require.Equal(t, []string{k.Prefix + "/" + ids[1]}, prefixes(ix.Entries))
	require.Equal(t, []string{k.Prefix + "/" + ids[0]}, wantPrefixes(ix.Wanted))
}

// wantSurvivesLaterCommits: an obligation is carried through every later index this engine writes,
// so a crash between recording a want and satisfying it is benign.
func wantSurvivesLaterCommits(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	be := backend.Memory()
	ids := k.twoParts(t, k.open(t, be), be)

	k.erasePart(ctx, t, be, ids[0])

	r := k.open(t, be)
	require.NoError(t, r.LoadParts(ctx))

	r.Append(t, api(300, 3))
	require.NoError(t, r.Flush(ctx))
	require.NoError(t, r.Merge(ctx, 0))

	want := []string{k.Prefix + "/" + ids[0]}
	require.Equal(t, want, wantPrefixes(k.loadIndex(t, be).Wanted))

	require.NoError(t, k.open(t, be).LoadParts(ctx))
	require.Equal(t, want, wantPrefixes(k.loadIndex(t, be).Wanted), "a restart reads it back off the index")
}

// invariantBackend asserts, on every index commit that lands, that no part vanished from Entries
// without landing in exactly one of Removed and Wanted. It sits on the real commit path, so every
// flush, merge, retention drop and lossy load in a test is checked.
func invariantBackend(ctx context.Context, t *testing.T, key string) *faultbackend.Backend {
	t.Helper()

	inner := backend.Memory()
	be := faultbackend.Wrap(inner)

	// Shared by one commit's Before and After; the tests commit from one goroutine at a time.
	var prev *bucketindex.Index

	be.Add(faultbackend.Rule{
		Kind:  faultbackend.CompareAndSwap,
		Match: func(op faultbackend.Op) bool { return op.Key == key },
		Before: func(faultbackend.Op) {
			var err error

			prev, err = bucketindex.Load(ctx, inner, key)
			require.NoError(t, err)
		},
		After: func(_ faultbackend.Op, data []byte) {
			next, err := bucketindex.Decode(data)
			require.NoError(t, err)
			assertLeavesIntoOneList(t, prev, next)
		},
	})

	return be
}

func assertLeavesIntoOneList(t *testing.T, prev, next *bucketindex.Index) {
	t.Helper()

	live := prefixes(next.Entries)

	for _, p := range prefixes(prev.Entries) {
		if slices.Contains(live, p) {
			continue
		}

		removed := slices.ContainsFunc(next.Removed, func(r bucketindex.Removal) bool { return r.Prefix == p })
		wanted := slices.ContainsFunc(next.Wanted, func(w bucketindex.Want) bool { return w.Prefix == p })

		switch {
		case removed && wanted:
			require.Fail(t, "a part left Entries into both Removed and Wanted", "part %q", p)
		case !removed && !wanted:
			require.Fail(t, "a part vanished from Entries into neither Removed nor Wanted", "part %q", p)
		}
	}
}

// entriesLeaveOnlyIntoRemovedOrWanted is the design in one line, as a property over the real commit
// path: random append, flush, merge, retention and part-loss sequences, with every committed index
// checked against the one it replaced.
func entriesLeaveOnlyIntoRemovedOrWanted(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()

	for seed := range uint64(24) {
		t.Run("", func(t *testing.T) {
			t.Parallel()

			rng := rand.New(rand.NewPCG(seed, 0x5eed)) //nolint:gosec // a reproducible property-test sequence
			be := invariantBackend(ctx, t, k.indexKey())
			e := k.open(t, be)
			ts := int64(100)

			for step := range 12 {
				ts += 100

				switch rng.IntN(5) {
				case 0, 1:
					e.Append(t, api(ts, int64(step)))
					require.NoError(t, e.Flush(ctx))
				case 2:
					require.NoError(t, e.Merge(ctx, 0))
				case 3:
					// Retention: parts wholly below the horizon are dropped, not merged.
					require.NoError(t, e.Merge(ctx, ts-150))
				case 4:
					ids := k.partDirs(ctx, t, be)
					if len(ids) == 0 {
						continue
					}

					k.erasePart(ctx, t, be, ids[rng.IntN(len(ids))])

					e = k.open(t, be)
					require.NoError(t, e.LoadParts(ctx), "step %d", step)
				}
			}
		})
	}
}
