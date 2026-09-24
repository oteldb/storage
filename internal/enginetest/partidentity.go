package enginetest

import (
	"context"
	"encoding/binary"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/faultbackend"
	"github.com/oteldb/storage/signal"
)

// partIdentityRetentionSelfCleaning is the point of scoping identity to the part: dropping a part
// drops the identities that named its rows, with no sweep, no live-set walk and no ownership rule.
func partIdentityRetentionSelfCleaning(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	be := backend.Memory()
	e := k.open(t, be)

	// One flush per stream, so each lands in its own part: old is retained by nothing, new survives.
	k.flushEach(t, e, be, Row{Stream: "old", Ts: 100, Val: 1}, Row{Stream: "new", Ts: 5000, Val: 2})

	fresh := k.open(t, be)
	require.NoError(t, fresh.LoadParts(ctx))
	require.Equal(t, 2, fresh.StreamCount())

	// Retention past the old part's rows: the merge drops them, so the part goes — and with it the
	// only durable copy of that identity.
	require.NoError(t, e.Merge(ctx, 1000))

	reloaded := k.open(t, be)
	require.NoError(t, reloaded.LoadParts(ctx))
	assert.Equal(t, 1, reloaded.StreamCount(), "the dropped part's identity is gone with it")
	assert.Empty(t, rows(t, reloaded, "old"))
}

// partIdentityMergedPartCarriesUnion: a merge writes the identities of what it produced, the union
// of its inputs minus whatever retention dropped.
func partIdentityMergedPartCarriesUnion(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	be := backend.Memory()
	e := k.open(t, be)

	for i := range int64(3) {
		e.Append(t, Row{Stream: "svc-" + strconv.FormatInt(i, 10), Ts: 100 + i, Val: 1})
		require.NoError(t, e.Flush(ctx))
	}

	require.NoError(t, e.Merge(ctx, 0))
	require.Equal(t, 1, e.PartCount(), "the parts merged into one")

	fresh := k.open(t, be)
	require.NoError(t, fresh.LoadParts(ctx))
	assert.Equal(t, 3, fresh.StreamCount(), "the merged part carries every input identity")
}

// legacyIdentityBin builds the whole-set identity object as builds before part-scoped identity
// wrote it: a count followed by length-delimited hash-input records.
func legacyIdentityBin(set ...signal.Series) []byte {
	buf := binary.AppendUvarint(nil, uint64(len(set)))
	for i := range set {
		enc := set[i].AppendHashInput(nil)
		buf = binary.AppendUvarint(buf, uint64(len(enc)))
		buf = append(buf, enc...)
	}

	return buf
}

// legacyIdentityObjectStillResolves covers a prefix written by an older build: its parts carry no
// identity object, so the whole-set object is the only place their identities exist. It must still
// be read, and kept, since dropping it would strand those parts' rows.
func legacyIdentityObjectStillResolves(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	be := backend.Memory()
	e := k.open(t, be)
	dirs := k.flushEach(t, e, be, api(100, 1))

	// Age the prefix: drop the part's identity object and leave the whole-set one in its place.
	legacy := k.Prefix + "/" + k.LegacyIdentityObject
	require.NoError(t, be.Delete(ctx, k.Prefix+"/"+dirs[0]+"/identity"))
	require.NoError(t, be.Write(ctx, legacy, legacyIdentityBin(k.Identity(apiStream))))

	r := k.open(t, be)
	require.NoError(t, r.LoadParts(ctx))
	assert.Equal(t, 1, r.StreamCount(), "the legacy object still names the old part's stream")
	assert.Equal(t, []Row{api(100, 1)}, rows(t, r, apiStream))

	_, err := backend.ReadUncached(ctx, be, legacy)
	require.NoError(t, err, "the legacy object is kept while a part still depends on it")
}

// legacyIdentityObjectDeletedOnceMigrated: once every live part carries its own identities, the
// whole-set object holds nothing live and is removed, or recovery would keep resurrecting
// identities whose data is long gone.
func legacyIdentityObjectDeletedOnceMigrated(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	be := backend.Memory()
	e := k.open(t, be)
	k.flushEach(t, e, be, api(100, 1))

	// A leftover whole-set object from before the upgrade, naming a stream whose data is gone.
	legacy := k.Prefix + "/" + k.LegacyIdentityObject
	require.NoError(t, be.Write(ctx, legacy, legacyIdentityBin(k.Identity("dead"))))

	r := k.open(t, be)
	require.NoError(t, r.LoadParts(ctx))

	_, err := backend.ReadUncached(ctx, be, legacy)
	require.ErrorIs(t, err, backend.ErrNotExist, "the legacy object is deleted once every part carries identity")

	r2 := k.open(t, be)
	require.NoError(t, r2.LoadParts(ctx))
	assert.Equal(t, 1, r2.StreamCount(), "the dead identity is not resurrected")
}

func identityWrite(op faultbackend.Op) bool {
	return op.Kind == faultbackend.Write && strings.HasSuffix(op.Key, "/identity")
}

// partIdentityWriteAmplification is the write-side point of part-scoping: a flush persists the
// identities it wrote, not every identity the tenant has ever had. The whole-set object it replaces
// was rewritten whenever the set changed, so under churn cost tracked cardinality instead of churn.
func partIdentityWriteAmplification(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	be := faultbackend.Wrap(backend.Memory())
	e := k.open(t, be)

	for i := range k.IdentitySeed {
		e.Append(t, Row{Stream: "svc-" + strconv.Itoa(i), Ts: 100, Val: 1})
	}

	require.NoError(t, e.Flush(ctx))

	first := be.Bytes(identityWrite)
	require.Positive(t, first)

	// One new stream arrives; the tenant's cardinality is unchanged otherwise.
	e.Append(t, Row{Stream: "svc-new", Ts: 200, Val: 1})
	require.NoError(t, e.Flush(ctx))

	second := be.Bytes(identityWrite) - first
	t.Logf("identity bytes: first flush (%d streams) %d B (%.1f B/stream), second flush (1 stream) %d B",
		k.IdentitySeed, first, float64(first)/float64(k.IdentitySeed), second)

	assert.Less(t, second, first/k.IdentityRatio, "a flush persists what it wrote, not the whole identity set")
}

// walResolvesStreamAfterCheckpoint: a flush checkpoints the WAL, discarding the identity records
// written when a stream was first seen. A later row for that stream must still replay on its own,
// because the part that would otherwise name it can be dropped by retention at any time.
func walResolvesStreamAfterCheckpoint(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	dir := t.TempDir()
	w := createWAL(t, dir)

	src := k.Open(t, Config{Backend: backend.Memory(), WAL: w})
	src.Append(t, api(100, 1))
	require.NoError(t, src.Flush(ctx)) // checkpoints: the identity record written above is discarded
	src.Append(t, api(200, 2))         // known identity, fresh buffer
	require.NoError(t, w.Close())

	// Replay alone, with no parts at all — the log must be self-contained.
	restored := k.Open(t, Config{})
	require.NoError(t, restored.Replay(t.Context(), dir))

	require.Equal(t, 1, restored.StreamCount(), "the log re-registers the stream it still references")
	assert.Equal(t, []Row{api(200, 2)}, rows(t, restored, apiStream), "the post-checkpoint row survives")
}
