package engine_test

import (
	"context"
	"maps"
	"math/rand/v2"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/internal/partid"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

const lostPrefix = "default/metrics"

func lostIndexKey() string { return lostPrefix + "/" + bucketindex.Object }

func prefixes(entries []bucketindex.Entry) []string {
	out := make([]string, len(entries))
	for i, e := range entries {
		out[i] = e.Prefix
	}

	return out
}

func wantPrefixes(wants []bucketindex.Want) []string {
	out := make([]string, len(wants))
	for i, w := range wants {
		out[i] = w.Prefix
	}

	return out
}

// diskPartKeys returns the backend objects of the part with the given id.
func diskPartKeys(ctx context.Context, t *testing.T, be backend.Backend, id string) []string {
	t.Helper()

	keys, err := be.List(ctx, lostPrefix+"/"+id+"/")
	require.NoError(t, err)

	return keys
}

// diskPartIDs returns the sorted ids of the parts that have objects under the engine prefix — part ids
// are minted, so a test cannot name them up front and asserts over this set instead.
func diskPartIDs(ctx context.Context, t *testing.T, be backend.Backend) []string {
	t.Helper()

	keys, err := be.List(ctx, lostPrefix+"/")
	require.NoError(t, err)

	seen := make(map[string]struct{}, len(keys))

	for _, k := range keys {
		if dir, _, ok := strings.Cut(strings.TrimPrefix(k, lostPrefix+"/"), "/"); ok && partid.Valid(dir) {
			seen[dir] = struct{}{}
		}
	}

	return slices.Sorted(maps.Keys(seen))
}

// erasePart deletes every backend object of the part with the given id, the disk failure a repair
// exists for.
func erasePart(ctx context.Context, t *testing.T, be backend.Backend, id string) {
	t.Helper()

	for _, k := range diskPartKeys(ctx, t, be, id) {
		require.NoError(t, be.Delete(ctx, k))
	}
}

// failReads wraps a backend and answers Read for keys with a given suffix with err — an injected
// *transient* failure when err is anything but [backend.ErrNotExist].
type failReads struct {
	backend.Backend

	armed atomic.Bool
	only  string
	err   error
}

func (f *failReads) Read(ctx context.Context, key string) ([]byte, error) {
	if f.armed.Load() && strings.HasSuffix(key, f.only) {
		return nil, f.err
	}

	return f.Backend.Read(ctx, key)
}

// countCAS wraps a backend and counts the compare-and-swaps against one key, optionally refusing
// them all so a caller sees a permanently lost race.
type countCAS struct {
	backend.Backend

	key    string
	n      atomic.Int64
	refuse atomic.Bool
}

func (c *countCAS) CompareAndSwap(
	ctx context.Context, key string, expected backend.Version, data []byte,
) (backend.Version, bool, error) {
	if key != c.key {
		return c.Backend.CompareAndSwap(ctx, key, expected, data)
	}

	c.n.Add(1)

	if c.refuse.Load() {
		return backend.VersionAbsent, false, nil
	}

	return c.Backend.CompareAndSwap(ctx, key, expected, data)
}

func newLostEngine(be backend.Backend) *engine.Engine {
	return engine.New(engine.Config{Backend: be, Prefix: lostPrefix})
}

// twoParts flushes two parts and returns their ids in index order.
func twoParts(t *testing.T, e *engine.Engine, be backend.Backend, s signal.Series) []string {
	t.Helper()

	ctx := context.Background()

	mustAppend(t, e, s, 100, 1.0)
	require.NoError(t, e.Flush(ctx))
	mustAppend(t, e, s, 200, 2.0)
	require.NoError(t, e.Flush(ctx))

	ids := diskPartIDs(ctx, t, be)
	require.Len(t, ids, 2)

	return ids
}

func lostValues(t *testing.T, e *engine.Engine) []float64 {
	t.Helper()

	var out []float64

	for _, b := range fetchAll(t, e, fetch.Request{
		Matchers: []fetch.Matcher{eqMatcher("job", "api")},
		Start:    0, End: 1 << 60,
	}) {
		out = append(out, b.Values...)
	}

	slices.Sort(out)

	return out
}

// TestGonePartBecomesWant is the whole of step 2: a part whose objects are permanently absent no
// longer fails the load — it leaves Entries into Wanted, the rest of the part set still serves, and
// the obligation is in the committed index.
func TestGonePartBecomesWant(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	s := mkSeries("job", "api")
	e := newLostEngine(be)
	ids := twoParts(t, e, be, s)

	erasePart(ctx, t, be, ids[0])

	r := newLostEngine(be)
	require.NoError(t, r.LoadParts(ctx), "one gone part must not fail the load")
	require.Equal(t, 1, r.PartCount())
	require.Equal(t, []float64{2.0}, lostValues(t, r))

	ix := loadIndex(t, be, lostPrefix)
	require.Equal(t, []string{lostPrefix + "/" + ids[1]}, prefixes(ix.Entries))
	require.Equal(t, []string{lostPrefix + "/" + ids[0]}, wantPrefixes(ix.Wanted))
	require.Empty(t, ix.Removed, "a loss is not a removal")
	require.NotZero(t, ix.Wanted[0].Generation, "the want records when it was discovered")
}

// TestPartiallyGonePartBecomesWant covers the half-erased part: the manifest is there and nothing
// it names is. It cannot answer a read, so it is as lost as one with no objects at all — and
// the open-time orphan sweep spares what is left of it, because those objects are the remains of a
// part repair is owed rather than the residue of a failed flush.
func TestPartiallyGonePartBecomesWant(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	e := newLostEngine(be)
	ids := twoParts(t, e, be, mkSeries("job", "api"))

	for _, k := range diskPartKeys(ctx, t, be, ids[0]) {
		if !strings.HasSuffix(k, "/manifest") {
			require.NoError(t, be.Delete(ctx, k))
		}
	}

	r := newLostEngine(be)
	require.NoError(t, r.LoadParts(ctx))

	ix := loadIndex(t, be, lostPrefix)
	require.Equal(t, []string{lostPrefix + "/" + ids[0]}, wantPrefixes(ix.Wanted))
	require.Equal(t, []string{lostPrefix + "/" + ids[1]}, prefixes(ix.Entries))
	require.NotEmpty(t, diskPartKeys(ctx, t, be, ids[0]), "a wanted part's remaining objects are not orphans")
}

// TestTransientOpenFailureRecordsNoWant is the guard the whole step turns on: a backend that could
// not answer says nothing about whether the part exists, so it fails the load exactly as before and
// records nothing. Converting one into a want would drop a live part and start a repair for data
// that was never missing — shard-wide, when many parts share the failing backend.
func TestTransientOpenFailureRecordsNoWant(t *testing.T) {
	t.Parallel()

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
			be := &failReads{Backend: backend.Memory(), only: "/manifest", err: tc.err}
			e := newLostEngine(be)
			ids := twoParts(t, e, be, mkSeries("job", "api"))

			before := loadIndex(t, be, lostPrefix)

			be.armed.Store(true)
			err := newLostEngine(be).LoadParts(ctx)
			be.armed.Store(false)

			after := loadIndex(t, be, lostPrefix)
			assert.Empty(t, after.Wanted, "a transient failure is not evidence of loss")
			assert.Equal(t, prefixes(before.Entries), prefixes(after.Entries),
				"a transient failure must not strip live parts from the index")
			assert.Len(t, after.Entries, 2)
			assert.Equal(t, before.Generation, after.Generation, "nothing was committed")
			assert.NotEmpty(t, diskPartKeys(ctx, t, be, ids[0]))
			assert.Error(t, err, "a transient failure must still fail the load")
		})
	}
}

// TestCorruptPartIsNotAWant pins the deliberately narrow trigger: objects that are present but
// unreadable are a different failure with a different remedy, and widening the trigger to cover
// them is how a repair path becomes a data-destruction path.
func TestCorruptPartIsNotAWant(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	e := newLostEngine(be)
	ids := twoParts(t, e, be, mkSeries("job", "api"))

	objs := diskPartKeys(ctx, t, be, ids[0])
	i := slices.IndexFunc(objs, func(k string) bool { return strings.HasSuffix(k, "/manifest") })
	require.GreaterOrEqual(t, i, 0)
	require.NoError(t, be.Write(ctx, objs[i], []byte("not a manifest")))

	require.Error(t, newLostEngine(be).LoadParts(ctx))
	require.Empty(t, loadIndex(t, be, lostPrefix).Wanted)
}

// TestWantIsRecordedInOneCommit verifies the atomicity by construction: dropping the entry and
// recording the want is a single compare-and-swap, so there is no window in which a crash could
// land one without the other.
func TestWantIsRecordedInOneCommit(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := &countCAS{Backend: backend.Memory(), key: lostIndexKey()}
	e := newLostEngine(be)
	ids := twoParts(t, e, be, mkSeries("job", "api"))

	erasePart(ctx, t, be, ids[0])

	be.n.Store(0)
	require.NoError(t, newLostEngine(be).LoadParts(ctx))

	require.Equal(t, int64(1), be.n.Load(), "the drop and the want are one commit, not two")

	ix := loadIndex(t, be, lostPrefix)
	require.NotContains(t, prefixes(ix.Entries), lostPrefix+"/"+ids[0])
	require.Contains(t, wantPrefixes(ix.Wanted), lostPrefix+"/"+ids[0])
}

// TestFailedWantCommitAppliesNeither verifies a lost race leaves the stored index untouched: the
// entry is still there and no want is, so the retry re-reads and re-derives both halves rather
// than inheriting half of an applied commit.
func TestFailedWantCommitAppliesNeither(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := &countCAS{Backend: backend.Memory(), key: lostIndexKey()}
	e := newLostEngine(be)
	ids := twoParts(t, e, be, mkSeries("job", "api"))

	before := loadIndex(t, be, lostPrefix)
	erasePart(ctx, t, be, ids[0])

	be.refuse.Store(true)
	require.Error(t, newLostEngine(be).LoadParts(ctx),
		"a commit that never lands must not be reported as success")
	be.refuse.Store(false)

	after := loadIndex(t, be, lostPrefix)
	require.Equal(t, prefixes(before.Entries), prefixes(after.Entries), "the entry is still there")
	require.Empty(t, after.Wanted, "and the want is not")

	// The retry re-derives both halves from the same evidence and commits them together.
	require.NoError(t, newLostEngine(be).LoadParts(ctx))

	ix := loadIndex(t, be, lostPrefix)
	require.Equal(t, []string{lostPrefix + "/" + ids[1]}, prefixes(ix.Entries))
	require.Equal(t, []string{lostPrefix + "/" + ids[0]}, wantPrefixes(ix.Wanted))
}

// TestWantSurvivesLaterCommits verifies an obligation is durable: it is carried through every later
// index this engine writes, so a crash between recording a want and satisfying it is benign.
func TestWantSurvivesLaterCommits(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	s := mkSeries("job", "api")
	e := newLostEngine(be)
	ids := twoParts(t, e, be, s)

	erasePart(ctx, t, be, ids[0])

	r := newLostEngine(be)
	require.NoError(t, r.LoadParts(ctx))

	mustAppend(t, r, s, 300, 3.0)
	require.NoError(t, r.Flush(ctx))
	require.NoError(t, r.Merge(ctx, 0))

	require.Equal(t, []string{lostPrefix + "/" + ids[0]}, wantPrefixes(loadIndex(t, be, lostPrefix).Wanted))

	// And across a restart, which reads it back off the index rather than rediscovering it.
	require.NoError(t, newLostEngine(be).LoadParts(ctx))
	require.Equal(t, []string{lostPrefix + "/" + ids[0]}, wantPrefixes(loadIndex(t, be, lostPrefix).Wanted))
}

// invariantCAS asserts, on every index commit that lands, that no part vanished from Entries
// without landing in exactly one of Removed and Wanted. It sits on the real commit path, so every
// flush, merge, retention drop and lossy load in a test is checked.
type invariantCAS struct {
	backend.Backend

	t   *testing.T
	key string
}

func (c *invariantCAS) CompareAndSwap(
	ctx context.Context, key string, expected backend.Version, data []byte,
) (backend.Version, bool, error) {
	if key != c.key {
		return c.Backend.CompareAndSwap(ctx, key, expected, data)
	}

	prev, err := bucketindex.Load(ctx, c.Backend, key)
	require.NoError(c.t, err)

	version, ok, err := c.Backend.CompareAndSwap(ctx, key, expected, data)
	if err != nil || !ok {
		return version, ok, err
	}

	next, decErr := bucketindex.Decode(data)
	require.NoError(c.t, decErr)
	assertLeavesIntoOneList(c.t, prev, next)

	return version, ok, err
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

// TestEntriesLeaveOnlyIntoRemovedOrWanted is the design in one line, as a property over the real
// commit path: random append, flush, merge, retention and part-loss sequences, with every committed
// index checked against the one it replaced.
func TestEntriesLeaveOnlyIntoRemovedOrWanted(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	for seed := range uint64(24) {
		t.Run("", func(t *testing.T) {
			t.Parallel()

			rng := rand.New(rand.NewPCG(seed, 0x5eed))
			be := &invariantCAS{Backend: backend.Memory(), t: t, key: lostIndexKey()}
			s := mkSeries("job", "api")
			e := newLostEngine(be)
			ts := int64(100)

			for step := range 12 {
				ts += 100

				switch rng.IntN(5) {
				case 0, 1:
					mustAppend(t, e, s, ts, float64(step))
					require.NoError(t, e.Flush(ctx))
				case 2:
					require.NoError(t, e.Merge(ctx, 0))
				case 3:
					// Retention: parts wholly below the horizon are dropped, not merged.
					require.NoError(t, e.Merge(ctx, ts-150))
				case 4:
					ids := diskPartIDs(ctx, t, be)
					if len(ids) == 0 {
						continue
					}

					erasePart(ctx, t, be, ids[rng.IntN(len(ids))])

					e = newLostEngine(be)
					require.NoError(t, e.LoadParts(ctx), "step %d", step)
				}
			}
		})
	}
}
