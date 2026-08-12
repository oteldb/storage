package engine_test

import (
	"context"
	"encoding/binary"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"
)

// TestPartIdentityRetentionSelfCleaning is the point of scoping identity to the part: dropping a
// part drops the identities that named its rows, with no sweep, no live-set walk and no ownership
// rule — a reader reconstructing from the object store sees only what data still backs.
func TestPartIdentityRetentionSelfCleaning(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	cfg := engine.Config{Backend: be, Prefix: "t/retention"}

	e := engine.New(cfg)

	// One flush per series, so each lands in its own part: old is retained by nothing, new survives.
	mustAppend(t, e, mkSeries("job", "old"), 100, 1)
	require.NoError(t, e.Flush(ctx))
	mustAppend(t, e, mkSeries("job", "new"), 5000, 2)
	require.NoError(t, e.Flush(ctx))

	fresh := engine.New(cfg)
	require.NoError(t, fresh.LoadParts(ctx))
	require.Equal(t, 2, fresh.SeriesCount())

	// Retention past the old part's samples: the merge drops its rows, so the part goes — and with
	// it the only durable copy of that identity.
	require.NoError(t, e.MergeWith(ctx, engine.MergeOptions{RetainFrom: 1000}))

	reloaded := engine.New(cfg)
	require.NoError(t, reloaded.LoadParts(ctx))
	assert.Equal(t, 1, reloaded.SeriesCount(), "the dropped part's identity is gone with it")

	got := fetchAll(t, reloaded, fetch.Request{
		Start: 0, End: 1 << 60,
		Matchers: []fetch.Matcher{eqMatcher("job", "old")},
	})
	assert.Empty(t, got)
}

// TestPartIdentityMergedPartCarriesUnion checks a merge writes the identities of what it produced:
// the union of its inputs, minus whatever retention dropped.
func TestPartIdentityMergedPartCarriesUnion(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	cfg := engine.Config{Backend: be, Prefix: "t/merge-identity"}

	e := engine.New(cfg)
	for i := range 3 {
		mustAppend(t, e, mkSeries("job", "api", "inst", strconv.Itoa(i)), int64(100+i), 1)
		require.NoError(t, e.Flush(ctx))
	}

	require.NoError(t, e.MergeWith(ctx, engine.MergeOptions{}))
	require.Equal(t, 1, e.PartCount(), "the parts merged into one")

	fresh := engine.New(cfg)
	require.NoError(t, fresh.LoadParts(ctx))
	assert.Equal(t, 3, fresh.SeriesCount(), "the merged part carries every input identity")
}

// legacySeriesBin builds the whole-set identity object as builds before part-scoped identity wrote
// it: a count followed by length-delimited hash-input records.
func legacySeriesBin(set ...signal.Series) []byte {
	buf := binary.AppendUvarint(nil, uint64(len(set)))
	for i := range set {
		enc := set[i].AppendHashInput(nil)
		buf = binary.AppendUvarint(buf, uint64(len(enc)))
		buf = append(buf, enc...)
	}

	return buf
}

// TestLegacyIdentityObjectStillResolves covers a prefix written by an older build: its parts carry
// no identity object, so the whole-set series.bin is the only place their identities exist. It must
// still be read — and kept, since dropping it would strand those parts' rows.
func TestLegacyIdentityObjectStillResolves(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	cfg := engine.Config{Backend: be, Prefix: "t/legacy"}

	api := mkSeries("job", "api")

	e := engine.New(cfg)
	mustAppend(t, e, api, 100, 1)
	require.NoError(t, e.Flush(ctx))

	// Age the prefix: drop the part's identity object and leave the whole-set one in its place.
	require.NoError(t, be.Delete(ctx, "t/legacy/0000000000/identity"))
	require.NoError(t, be.Write(ctx, "t/legacy/series.bin", legacySeriesBin(api)))

	r := engine.New(cfg)
	require.NoError(t, r.LoadParts(ctx))
	assert.Equal(t, 1, r.SeriesCount(), "the legacy object still names the old part's series")

	got := fetchAll(t, r, fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("job", "api")}})
	require.Len(t, got, 1)

	_, err := backend.ReadUncached(ctx, be, "t/legacy/series.bin")
	require.NoError(t, err, "the legacy object is kept while a part still depends on it")
}

// TestLegacyIdentityObjectDeletedOnceMigrated checks the migration completes itself: once every
// live part carries its own identities, the whole-set object holds nothing live and is removed —
// otherwise recovery would keep resurrecting identities whose data is long gone.
func TestLegacyIdentityObjectDeletedOnceMigrated(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	cfg := engine.Config{Backend: be, Prefix: "t/migrate"}

	api := mkSeries("job", "api")

	e := engine.New(cfg)
	mustAppend(t, e, api, 100, 1)
	require.NoError(t, e.Flush(ctx))

	// A leftover whole-set object from before the upgrade, naming a series whose data is gone.
	require.NoError(t, be.Write(ctx, "t/migrate/series.bin", legacySeriesBin(mkSeries("job", "dead"))))

	r := engine.New(cfg)
	require.NoError(t, r.LoadParts(ctx))

	_, err := backend.ReadUncached(ctx, be, "t/migrate/series.bin")
	require.ErrorIs(t, err, backend.ErrNotExist, "the legacy object is deleted once every part carries identity")

	// Re-opening now sees only the live identity: the dead one is not resurrected.
	r2 := engine.New(cfg)
	require.NoError(t, r2.LoadParts(ctx))
	assert.Equal(t, 1, r2.SeriesCount())
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

// TestPartIdentityWriteAmplification is the write-side point of part-scoping: a flush persists the
// identities it actually wrote, not every identity the tenant has ever had. The whole-set object it
// replaces was re-serialized in full whenever the set changed — so under churn, where the set
// changes on nearly every flush, cost tracked cardinality instead of the churn that caused it.
func TestPartIdentityWriteAmplification(t *testing.T) {
	t.Parallel()

	const seed = 20_000

	ctx := context.Background()
	be := &sizeKeyWrite{Backend: backend.Memory(), suffix: "/identity"}
	e := engine.New(engine.Config{Backend: be, Prefix: "t/amplification"})

	for i := range seed {
		mustAppend(t, e, mkSeries("job", "api", "inst", strconv.Itoa(i)), 100, 1)
	}

	require.NoError(t, e.Flush(ctx))

	first := be.bytes
	require.Positive(t, first)

	// One new series arrives; the tenant's cardinality is unchanged otherwise.
	mustAppend(t, e, mkSeries("job", "api", "inst", "new"), 200, 1)
	require.NoError(t, e.Flush(ctx))

	second := be.bytes - first
	t.Logf("identity bytes: first flush (%d series) %d B (%.1f B/series), second flush (1 series) %d B",
		seed, first, float64(first)/seed, second)

	assert.Less(t, second, first/1000, "a flush persists what it wrote, not the whole identity set")
}

// TestWALResolvesSeriesAfterCheckpoint covers what the whole-set object used to hide: a flush
// checkpoints the WAL, discarding the series records written when identities were first seen. A
// later sample for one of those series must still be replayable on its own, because the part that
// would otherwise name it can be dropped by retention at any time.
func TestWALResolvesSeriesAfterCheckpoint(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dir := t.TempDir()

	sw, err := wal.Create(dir, 0)
	require.NoError(t, err)

	api := mkSeries("job", "api")

	src := engine.New(engine.Config{WAL: sw, Backend: backend.Memory(), Prefix: "t/wal"})
	mustAppend(t, src, api, 100, 1)
	require.NoError(t, src.Flush(ctx)) // checkpoints: the series record written above is discarded
	mustAppend(t, src, api, 200, 2)    // known identity, fresh buffer
	require.NoError(t, sw.Close())

	// Replay alone, with no parts at all — the log must be self-contained.
	restored := engine.New(engine.Config{})
	require.NoError(t, restored.Replay(dir))

	require.Equal(t, 1, restored.SeriesCount(), "the log re-registers the series it still references")

	got := fetchAll(t, restored, fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("job", "api")}})
	require.Len(t, got, 1)
	assert.Equal(t, []int64{200}, got[0].Timestamps, "the post-checkpoint sample survives")
}

// BenchmarkLoadPartsIdentity measures recovery: the resident index is rebuilt by reading one
// identity object per live part instead of a single whole-set object, which is the cost #273 trades
// for retention that cleans itself. The part count is what varies — merge bounds it, so this is the
// working range.
func BenchmarkLoadPartsIdentity(b *testing.B) {
	const seriesPerPart = 20_000

	for _, parts := range []int{4, 16} {
		b.Run(strconv.Itoa(parts)+"parts", func(b *testing.B) {
			ctx := context.Background()
			be := backend.Memory()
			cfg := engine.Config{Backend: be, Prefix: "bench/identity"}

			e := engine.New(cfg)
			for p := range parts {
				for i := range seriesPerPart {
					if _, err := e.Append(mkSeries("job", "api", "inst", strconv.Itoa(p*seriesPerPart+i)), int64(100+p), 1); err != nil {
						b.Fatal(err)
					}
				}

				if err := e.Flush(ctx); err != nil {
					b.Fatal(err)
				}
			}

			b.ReportAllocs()

			for b.Loop() {
				r := engine.New(cfg)
				if err := r.LoadParts(ctx); err != nil {
					b.Fatal(err)
				}

				if got := r.SeriesCount(); got != parts*seriesPerPart {
					b.Fatalf("loaded %d identities, want %d", got, parts*seriesPerPart)
				}
			}
		})
	}
}
