package recordengine_test

import (
	"context"
	"encoding/binary"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"
)

// TestPartIdentityRetentionSelfCleaning is the point of scoping stream identity to the part:
// dropping a part drops the identities that named its rows, with no sweep and no ownership rule.
func TestPartIdentityRetentionSelfCleaning(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	cfg := recordengine.Config{Schema: testSchema, Backend: be, Prefix: "t/recs"}

	e := recordengine.New(cfg)
	ingest(t, e, mkBatch("old", rrec{ts: 100, body: "a"}))
	require.NoError(t, e.Flush(ctx))
	ingest(t, e, mkBatch("new", rrec{ts: 5000, body: "b"}))
	require.NoError(t, e.Flush(ctx))

	fresh := recordengine.New(cfg)
	require.NoError(t, fresh.LoadParts(ctx))
	require.EqualValues(t, 2, fresh.Stats().Streams)

	// Retention past the old part's records: the merge drops its rows, so the part goes — and with
	// it the only durable copy of that stream's identity.
	require.NoError(t, e.Merge(ctx, 1000))

	reloaded := recordengine.New(cfg)
	require.NoError(t, reloaded.LoadParts(ctx))
	assert.EqualValues(t, 1, reloaded.Stats().Streams, "the dropped part's identity is gone with it")
}

// TestPartIdentityMergedPartCarriesUnion checks a merge writes the identities of what it produced.
func TestPartIdentityMergedPartCarriesUnion(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	cfg := recordengine.Config{Schema: testSchema, Backend: be, Prefix: "t/recs"}

	e := recordengine.New(cfg)
	for i := range 3 {
		ingest(t, e, mkBatch("svc-"+strconv.Itoa(i), rrec{ts: int64(100 + i), body: "x"}))
		require.NoError(t, e.Flush(ctx))
	}

	require.NoError(t, e.Merge(ctx, 0))
	require.Equal(t, 1, e.PartCount(), "the parts merged into one")

	fresh := recordengine.New(cfg)
	require.NoError(t, fresh.LoadParts(ctx))
	assert.EqualValues(t, 3, fresh.Stats().Streams, "the merged part carries every input identity")
}

// sizeKeyWrite sums the bytes written to keys ending in suffix.
type sizeKeyWrite struct {
	backend.Backend

	suffix string
	bytes  int
}

func (c *sizeKeyWrite) Write(ctx context.Context, key string, data []byte) error {
	if strings.HasSuffix(key, c.suffix) {
		c.bytes += len(data)
	}

	return c.Backend.Write(ctx, key, data)
}

// TestPartIdentityWriteAmplification: a flush persists the identities it wrote, not every stream
// the tenant has ever had. The whole-set streams.bin it replaces was re-encoded and rewritten on
// **every** flush and merge, whether or not the set had changed.
func TestPartIdentityWriteAmplification(t *testing.T) {
	t.Parallel()

	const seed = 2_000

	ctx := context.Background()
	be := &sizeKeyWrite{Backend: backend.Memory(), suffix: "/identity"}
	e := recordengine.New(recordengine.Config{Schema: testSchema, Backend: be, Prefix: "t/recs"})

	for i := range seed {
		ingest(t, e, mkBatch("svc-"+strconv.Itoa(i), rrec{ts: 100, body: "x"}))
	}

	require.NoError(t, e.Flush(ctx))

	first := be.bytes
	require.Positive(t, first)

	ingest(t, e, mkBatch("svc-new", rrec{ts: 200, body: "x"}))
	require.NoError(t, e.Flush(ctx))

	second := be.bytes - first
	t.Logf("identity bytes: first flush (%d streams) %d B (%.1f B/stream), second flush (1 stream) %d B",
		seed, first, float64(first)/seed, second)

	assert.Less(t, second, first/100, "a flush persists what it wrote, not the whole identity set")
}

// legacyStreamsBin builds the whole-set identity object as builds before part-scoped identity wrote
// it: a count followed by length-delimited hash-input records.
func legacyStreamsBin(set ...signal.Series) []byte {
	buf := binary.AppendUvarint(nil, uint64(len(set)))
	for i := range set {
		enc := set[i].AppendHashInput(nil)
		buf = binary.AppendUvarint(buf, uint64(len(enc)))
		buf = append(buf, enc...)
	}

	return buf
}

// streamIdentity is the identity mkBatch gives a stream of the named service.
func streamIdentity(svc string) signal.Series {
	return signal.Series{Resource: signal.Resource{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte(svc))},
	)}}
}

// TestLegacyIdentityObjectStillResolves covers a prefix written by an older build: its parts carry
// no identity object, so streams.bin is the only place their identities exist.
func TestLegacyIdentityObjectStillResolves(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	cfg := recordengine.Config{Schema: testSchema, Backend: be, Prefix: "t/recs"}

	e := recordengine.New(cfg)
	ingest(t, e, mkBatch("api", rrec{ts: 100, body: "hello"}))
	require.NoError(t, e.Flush(ctx))

	dirs := partDirs(t, be)
	require.Len(t, dirs, 1)

	// Age the prefix: drop the part's identity object, leave the whole-set one in its place.
	require.NoError(t, be.Delete(ctx, "t/recs/"+dirs[0]+"/identity"))
	require.NoError(t, be.Write(ctx, "t/recs/streams.bin", legacyStreamsBin(streamIdentity("api"))))

	r := recordengine.New(cfg)
	require.NoError(t, r.LoadParts(ctx))
	assert.EqualValues(t, 1, r.Stats().Streams, "the legacy object still names the old part's stream")
	assert.Equal(t, []string{"hello"}, streamBodies(t, r))

	_, err := backend.ReadUncached(ctx, be, "t/recs/streams.bin")
	require.NoError(t, err, "the legacy object is kept while a part still depends on it")
}

// TestLegacyIdentityObjectDeletedOnceMigrated checks the migration completes itself.
func TestLegacyIdentityObjectDeletedOnceMigrated(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	cfg := recordengine.Config{Schema: testSchema, Backend: be, Prefix: "t/recs"}

	e := recordengine.New(cfg)
	ingest(t, e, mkBatch("api", rrec{ts: 100, body: "hello"}))
	require.NoError(t, e.Flush(ctx))

	// A leftover whole-set object from before the upgrade, naming a stream whose data is gone.
	require.NoError(t, be.Write(ctx, "t/recs/streams.bin", legacyStreamsBin(streamIdentity("dead"))))

	r := recordengine.New(cfg)
	require.NoError(t, r.LoadParts(ctx))

	_, err := backend.ReadUncached(ctx, be, "t/recs/streams.bin")
	require.ErrorIs(t, err, backend.ErrNotExist, "deleted once every part carries identity")

	r2 := recordengine.New(cfg)
	require.NoError(t, r2.LoadParts(ctx))
	assert.EqualValues(t, 1, r2.Stats().Streams, "the dead identity is not resurrected")
}

// TestWALResolvesStreamAfterCheckpoint covers what the whole-set object used to hide: a flush
// checkpoints the WAL, discarding the stream records written when identities were first seen. A
// later record for one of those streams must still be replayable on its own, because the part that
// would otherwise name it can be dropped by retention at any time.
func TestWALResolvesStreamAfterCheckpoint(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dir := t.TempDir()

	w, err := wal.Create(dir, 0)
	require.NoError(t, err)

	src := recordengine.New(recordengine.Config{Schema: testSchema, Backend: backend.Memory(), Prefix: "t/recs", WAL: w})
	ingest(t, src, mkBatch("api", rrec{ts: 100, body: "flushed"}))
	require.NoError(t, src.Flush(ctx)) // checkpoints: the stream record written above is discarded
	ingest(t, src, mkBatch("api", rrec{ts: 200, body: "unflushed"}))
	require.NoError(t, w.Close())

	// Replay alone, with no parts at all — the log must be self-contained.
	restored := recordengine.New(recordengine.Config{Schema: testSchema})
	require.NoError(t, restored.Replay(dir))

	require.EqualValues(t, 1, restored.Stats().Streams, "the log re-registers the stream it references")
	assert.Equal(t, []string{"unflushed"}, streamBodies(t, restored), "the post-checkpoint record survives")
}
