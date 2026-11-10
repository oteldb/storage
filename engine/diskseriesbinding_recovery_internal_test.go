package engine

import (
	"context"
	"strings"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/wal"
)

type nonceFailureBackend struct {
	backend.Backend

	manifestWrites map[string]int
	failure        error
}

func (b *nonceFailureBackend) Write(ctx context.Context, key string, data []byte) error {
	if strings.HasSuffix(key, "/manifest") {
		b.manifestWrites[key]++
		if b.manifestWrites[key] == 2 {
			return b.failure
		}
	}
	return b.Backend.Write(ctx, key, data)
}
func (b *nonceFailureBackend) NativeSize(ctx context.Context, key string) (int64, bool, error) {
	return backend.NativeSizeOf(ctx, b.Backend, key)
}
func (b *nonceFailureBackend) ReadAt(ctx context.Context, key string, off, n int64) ([]byte, error) {
	return backend.ReadAt(ctx, b.Backend, key, off, n)
}

func TestDiskSeriesNonceRewriteFailurePreservesWAL(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	dir := t.TempDir()
	journal, err := wal.Create(dir, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = journal.Close() })
	base := backend.Memory()
	injected := errors.New("nonce rewrite failure")
	b := &nonceFailureBackend{Backend: base, manifestWrites: make(map[string]int), failure: injected}
	e := New(Config{Backend: b, Prefix: "nonce-failure", DiskSeries: true, WAL: journal, OOOWindow: 10})
	identity := benchSeries(1)
	accepted, err := e.Append(identity, 100, 1)
	require.NoError(t, err)
	require.True(t, accepted)
	require.ErrorIs(t, e.Flush(ctx), injected)
	require.Zero(t, e.PartCount())
	require.EqualValues(t, 1, e.Stats().HeadSamples)
	require.Nil(t, e.flushing)
	_, version, err := base.ReadVersioned(ctx, e.indexKey())
	require.NoError(t, err)
	require.Equal(t, backend.VersionAbsent, version, "a failed nonce rewrite must not publish a catalog")
	it, err := e.Fetch(ctx, fetch.Request{Start: 0, End: 200})
	require.NoError(t, err)
	rows, err := fetch.Drain(ctx, it)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, []float64{1}, rows[0].Values)
	require.NoError(t, journal.Close())
	recovered := New(Config{Backend: base, Prefix: e.cfg.Prefix, DiskSeries: true, OOOWindow: 10})
	t.Cleanup(func() { require.NoError(t, recovered.Close(context.Background())) })
	require.NoError(t, recovered.LoadPartsReadOnly(ctx))
	require.Zero(t, recovered.PartCount())
	require.NoError(t, recovered.Replay(ctx, dir))
	require.EqualValues(t, 1, recovered.Stats().HeadSamples)
	require.EqualValues(t, 1, recovered.Stats().Series)
	require.NoError(t, recovered.Flush(ctx))
	require.Positive(t, recovered.PartCount())
	it, err = recovered.Fetch(ctx, fetch.Request{Start: 0, End: 200})
	require.NoError(t, err)
	rows, err = fetch.Drain(ctx, it)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, identity.Hash(), rows[0].Series.Hash())
	require.Equal(t, []float64{1}, rows[0].Values)
}

func TestDiskSeriesWholePartCopyPreservesBinding(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	b := backend.Memory()
	writer := New(Config{Backend: b, Prefix: "binding-copy", DiskSeries: true, AggregateStats: true})
	t.Cleanup(func() { require.NoError(t, writer.Close(context.Background())) })
	identity := benchSeries(1)
	for _, ts := range []int64{10, 20} {
		accepted, err := writer.Append(identity, ts, float64(ts))
		require.NoError(t, err)
		require.True(t, accepted)
	}
	require.NoError(t, writer.Flush(ctx))
	require.Len(t, writer.parts, 1)
	original := writer.parts[0]
	keys, err := b.List(ctx, original.prefix+"/")
	require.NoError(t, err)
	copiedPrefix := "binding-renamed/part"
	for _, key := range keys {
		raw, err := b.Read(ctx, key)
		require.NoError(t, err)
		require.NoError(t, b.Write(ctx, copiedPrefix+strings.TrimPrefix(key, original.prefix), raw))
	}
	reader := New(Config{Backend: b, Prefix: "binding-renamed", DiskSeries: true})
	t.Cleanup(func() { require.NoError(t, reader.Close(context.Background())) })
	catalog, err := bucketindex.Load(ctx, b, writer.indexKey())
	require.NoError(t, err)
	require.Len(t, catalog.Entries, 1)
	catalog.Entries[0].Prefix = copiedPrefix
	_, err = catalog.Save(ctx, b, reader.indexKey(), backend.VersionAbsent)
	require.NoError(t, err)
	require.NoError(t, reader.LoadPartsReadOnly(ctx))
	require.Len(t, reader.parts, 1)
	copied := reader.parts[0]
	require.Equal(t, original.reader.ManifestDigest(), copied.reader.ManifestDigest())
	require.Equal(t, original.reader.Manifest().BindingNonce, copied.reader.Manifest().BindingNonce)
	index, err := copied.seriesIndex(ctx)
	require.NoError(t, err)
	meta, found, err := index.Metadata(ctx, identity.Hash())
	require.NoError(t, err)
	require.True(t, found)
	require.EqualValues(t, 20, meta.Covered)
	require.True(t, meta.HasStats)
	require.InDelta(t, 30, meta.Stats.Sum, 0)
	it, err := reader.Fetch(ctx, fetch.Request{Start: 0, End: 30})
	require.NoError(t, err)
	rows, err := fetch.Drain(ctx, it)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, identity.Hash(), rows[0].Series.Hash())
	require.Equal(t, []float64{10, 20}, rows[0].Values)
}
