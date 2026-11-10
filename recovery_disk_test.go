package storage

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/backend/faultbackend"
	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"
)

type diskStartupFixture struct {
	backend  backend.Backend
	key      string
	good     []byte
	walRoot  string
	identity signal.Series
}

func corruptDiskStartupFixture(t *testing.T, withWAL bool) diskStartupFixture {
	t.Helper()
	ctx := t.Context()
	b, err := file.New(t.TempDir())
	require.NoError(t, err)
	var journal *wal.SegmentWriter
	walRoot := ""
	if withWAL {
		walRoot = t.TempDir()
		journal, err = wal.Create(filepath.Join(walRoot, "default", "metrics"), 0)
		require.NoError(t, err)
	}
	e := engine.New(engine.Config{Backend: b, Prefix: "default/metrics", DiskSeries: true, WAL: journal, OOOWindow: 10})
	identity := signal.Series{Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("__name__"), Value: signal.StringValue([]byte("m"))})}
	accepted, err := e.Append(identity, 100, 1)
	require.NoError(t, err)
	require.True(t, accepted)
	require.NoError(t, e.Flush(ctx))
	parts := e.Parts()
	require.Len(t, parts, 1)
	if withWAL {
		accepted, err = e.Append(identity, 95, 2)
		require.NoError(t, err)
		require.True(t, accepted)
	}
	require.NoError(t, e.CloseWAL())
	key := parts[0].ID + "/series-index"
	good, err := b.Read(ctx, key)
	require.NoError(t, err)
	bad := append([]byte(nil), good...)
	bad[len(bad)-1] ^= 1
	require.NoError(t, b.Write(ctx, key, bad))
	return diskStartupFixture{backend: b, key: key, good: good, walRoot: walRoot, identity: identity}
}

func TestOpenDiskSeriesCorruptionCanRepair(t *testing.T) {
	t.Parallel()
	for _, readonly := range []bool{false, true} {
		t.Run(map[bool]string{false: "owner", true: "readonly"}[readonly], func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			fixture := corruptDiskStartupFixture(t, false)
			b, key, good := fixture.backend, fixture.key, fixture.good
			catalogKey := "default/metrics/" + bucketindex.Object
			before, version, err := b.ReadVersioned(ctx, catalogKey)
			require.NoError(t, err)
			require.NotEmpty(t, before)
			require.NotEqual(t, backend.VersionAbsent, version)
			s, err := Open(ctx, Options{Backend: b, MetricDiskSeries: true, ReadOnly: readonly, FlushInterval: -1, OOOWindow: 10})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, s.Close(context.Background())) })
			eng, ok := s.lookupEngine("default")
			require.True(t, ok)
			require.True(t, eng.HasWants())
			require.False(t, eng.IndexFenced())
			require.False(t, eng.Stats().SeriesComplete)
			_, err = s.Fetcher("default").Fetch(ctx, fetch.Request{Start: 90, End: 110})
			require.ErrorIs(t, err, cluster.ErrShardIncomplete)
			if readonly {
				after, afterVersion, err := b.ReadVersioned(ctx, catalogKey)
				require.NoError(t, err)
				require.Equal(t, before, after)
				require.Equal(t, version, afterVersion)
				return
			}
			_, err = s.WriteMetrics(ctx, gaugeBatch("api", "new", []int64{101}, []float64{3}))
			require.ErrorIs(t, err, bucketindex.ErrFenced)
			require.NoError(t, b.Write(ctx, key, good))
			require.NoError(t, s.Admin().MaintainNow(ctx))
			require.False(t, eng.HasWants())
			require.True(t, eng.Stats().SeriesComplete)
			require.Equal(t, []float64{1}, readSamples(t, s, "m"))
		})
	}
}

func TestOpenDiskSeriesTransientIndexFailureIsFatal(t *testing.T) {
	t.Parallel()
	fixture := corruptDiskStartupFixture(t, false)
	b, key := fixture.backend, fixture.key
	injected := errors.New("index temporarily unavailable")
	fault := faultbackend.Wrap(b).Add(faultbackend.Rule{Kind: faultbackend.Read, Match: func(op faultbackend.Op) bool { return op.Key == key }, Err: injected})
	s, err := Open(t.Context(), Options{Backend: fault, MetricDiskSeries: true, FlushInterval: -1})
	require.ErrorIs(t, err, injected)
	require.Nil(t, s)
	require.Equal(t, 1, fault.Count(func(op faultbackend.Op) bool { return op.Kind == faultbackend.Read && op.Key == key }), "transient I/O must not be retried as persistent corruption")
}

func TestOpenDiskSeriesCorruptionRestoresAuthoritativeWAL(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	fixture := corruptDiskStartupFixture(t, true)
	b, key, good := fixture.backend, fixture.key, fixture.good
	walRoot, identity := fixture.walRoot, fixture.identity
	files, err := filepath.Glob(filepath.Join(walRoot, "default", "metrics", "*.wal"))
	require.NoError(t, err)
	require.NotEmpty(t, files)
	journal := make(map[string][]byte, len(files))
	for _, name := range files {
		journal[name], err = os.ReadFile(name)
		require.NoError(t, err)
	}
	s, err := Open(ctx, Options{Backend: b, MetricDiskSeries: true, WALDir: walRoot, FlushInterval: -1, OOOWindow: 10})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close(context.Background())) })
	eng, ok := s.lookupEngine("default")
	require.True(t, ok)
	require.True(t, eng.HasWants())
	require.False(t, eng.Stats().SeriesComplete)
	require.EqualValues(t, 1, eng.Stats().HeadSamples)
	for name, original := range journal {
		after, err := os.ReadFile(name)
		require.NoError(t, err)
		require.Equal(t, original, after, "startup must retain acknowledged WAL while disk history is incomplete")
	}
	accepted, err := eng.Append(identity, 86, 3)
	require.ErrorIs(t, err, bucketindex.ErrFenced)
	require.False(t, accepted)
	require.NoError(t, b.Write(ctx, key, good))
	require.NoError(t, s.Admin().MaintainNow(ctx))
	require.False(t, eng.HasWants())
	require.True(t, eng.Stats().SeriesComplete)
	accepted, err = eng.Append(identity, 86, 3)
	require.NoError(t, err)
	require.False(t, accepted, "restored durable timestamp100 must dominate replayed timestamp95")
	accepted, err = eng.Append(identity, 101, 3)
	require.NoError(t, err)
	require.True(t, accepted)
	require.Equal(t, []float64{2, 1, 3}, readSamples(t, s, "m"))
}
