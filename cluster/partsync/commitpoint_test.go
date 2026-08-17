package partsync_test

// Commit-point discipline under a crashed sync. The package doc promises that a mirroring pass
// interrupted at ANY point leaves the local backend in a state the engine can still load: a part's
// manifest is copied after the part's other objects, and the bucket index after every part, so the
// local index never references a half-copied part. What survives a crash is an unreferenced orphan
// the next pass retries, never a dangling index entry.
//
// The tests below fail the local backend's n-th write for every n a full mirror performs, and check
// that invariant after each crash — then heal the backend and require the retry to converge.

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/cluster/partsync"
)

var errCrashed = errors.New("partsync test: backend crashed")

// crashBackend passes writes through until the budget runs out, then fails every further write —
// modeling a node that dies part-way through a mirroring pass. Reads, lists and deletes always
// pass through, so the post-crash state is inspectable.
type crashBackend struct {
	backend.Backend

	mu     sync.Mutex
	budget int
	writes int
}

func (b *crashBackend) Write(ctx context.Context, key string, data []byte) error {
	b.mu.Lock()
	if b.budget <= 0 {
		b.mu.Unlock()

		return errCrashed
	}
	b.budget--
	b.writes++
	b.mu.Unlock()

	return b.Backend.Write(ctx, key, data)
}

func (b *crashBackend) heal() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.budget = 1 << 30
}

func (b *crashBackend) written() int {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.writes
}

// ownerWithParts builds a peer holding parts 1..n under prefix, plus the mutable identity object,
// and returns its address.
func ownerWithParts(t *testing.T, prefix string, n int) string {
	t.Helper()

	be := backend.Memory()
	ix := &bucketindex.Index{}

	for seq := 1; seq <= n; seq++ {
		writePart(t, be, ix, prefix, seq, int64(seq*100), int64(seq*100+50))
	}

	require.NoError(t, be.Write(context.Background(), prefix+"/streams.bin", []byte("streams-v1")))
	saveIndex(t, be, prefix, ix)

	return serve(t, be)
}

// requireCommitPoint asserts the two ordering guarantees over whatever the local backend holds: a
// part's manifest implies the part's other objects, and the bucket index implies every part it
// references is complete.
func requireCommitPoint(t *testing.T, local backend.Backend, prefix string) {
	t.Helper()
	ctx := context.Background()

	keys, err := local.List(ctx, prefix)
	require.NoError(t, err)

	have := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		have[k] = struct{}{}
	}

	requirePartComplete := func(part string) {
		t.Helper()

		for _, suffix := range []string{"/c/0", "/marks", "/manifest"} {
			_, ok := have[part+suffix]
			assert.Truef(t, ok, "part %q is missing %q", part, suffix)
		}
	}

	for k := range have {
		if part, ok := strings.CutSuffix(k, "/manifest"); ok {
			requirePartComplete(part)
		}
	}

	raw, err := backend.ReadView(ctx, local, prefix+"/"+bucketindex.Object)
	if errors.Is(err, backend.ErrNotExist) {
		return
	}
	require.NoError(t, err)

	ix, err := bucketindex.Decode(raw)
	require.NoError(t, err)

	for _, e := range ix.Entries {
		requirePartComplete(e.Prefix)
	}
}

// TestSyncCrashNeverInstallsIndexOverHalfCopiedPart crashes the local backend at every write offset
// a complete mirror performs. After each crash the local state must still satisfy the commit-point
// ordering, and healing the backend must let the very next pass converge to a byte-identical copy —
// the "orphan retried next pass" contract.
func TestSyncCrashNeverInstallsIndexOverHalfCopiedPart(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	const prefix = "default/metrics"

	addr := ownerWithParts(t, prefix, 3)

	// One uninterrupted mirror establishes how many writes a full pass performs, and the reference
	// bytes every healed retry below must reproduce.
	reference := &crashBackend{Backend: backend.Memory(), budget: 1 << 30}
	_, err := partsync.New(reference, &partsync.Client{}).Sync(ctx, prefix, []string{addr}, false, nil)
	require.NoError(t, err)

	total := reference.written()
	require.Positive(t, total)

	want := make(map[string][]byte)
	keys, err := reference.List(ctx, prefix)
	require.NoError(t, err)
	require.NotEmpty(t, keys)

	for _, k := range keys {
		v, err := reference.Read(ctx, k)
		require.NoError(t, err)
		want[k] = v
	}

	for budget := range total {
		local := &crashBackend{Backend: backend.Memory(), budget: budget}
		s := partsync.New(local, &partsync.Client{})

		_, err := s.Sync(ctx, prefix, []string{addr}, false, nil)
		require.Errorf(t, err, "budget %d: an interrupted pass reports the failure", budget)
		require.ErrorIsf(t, err, errCrashed, "budget %d", budget)

		requireCommitPoint(t, local, prefix)

		// The next pass, on a healthy backend, finishes the job: the orphans left behind are
		// completed rather than confusing the diff.
		local.heal()

		_, err = s.Sync(ctx, prefix, []string{addr}, false, nil)
		require.NoErrorf(t, err, "budget %d: the retry pass converges", budget)

		requireCommitPoint(t, local, prefix)

		got, err := local.List(ctx, prefix)
		require.NoError(t, err)
		assert.Lenf(t, got, len(want), "budget %d: the retry mirrors exactly the owner's object set", budget)

		for _, k := range got {
			v, err := local.Read(ctx, k)
			require.NoError(t, err)
			assert.Equalf(t, want[k], v, "budget %d: object %q mirrored verbatim", budget, k)
		}
	}
}

// TestSyncCrashLeavesNoIndexBeforeParts is the sharp end of the same rule: a pass that dies before
// it has copied every part must leave the local index untouched, so a reader that loads the local
// backend meanwhile never resolves an entry to objects that are not there.
func TestSyncCrashLeavesNoIndexBeforeParts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	const prefix = "t/metrics"

	addr := ownerWithParts(t, prefix, 4)

	// Enough budget for a few objects, never enough for all four parts plus the index.
	local := &crashBackend{Backend: backend.Memory(), budget: 5}

	_, err := partsync.New(local, &partsync.Client{}).Sync(ctx, prefix, []string{addr}, false, nil)
	require.ErrorIs(t, err, errCrashed)

	_, err = local.Read(ctx, prefix+"/"+bucketindex.Object)
	require.ErrorIs(t, err, backend.ErrNotExist,
		"the index is the commit point: it is never written before the parts it references")
}
