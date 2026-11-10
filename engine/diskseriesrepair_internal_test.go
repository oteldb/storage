package engine

import (
	"context"
	"encoding/binary"
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/internal/seriesindex"
)

type restoreIndexFetcher struct {
	backend backend.Backend
	key     string
	data    []byte
	calls   int
}

func (f *restoreIndexFetcher) FetchWants(ctx context.Context, wants []bucketindex.Want) []bucketindex.FetchResult {
	f.calls++
	results := make([]bucketindex.FetchResult, len(wants))
	for i := range wants {
		err := f.backend.Write(ctx, f.key, f.data)
		results[i] = bucketindex.FetchResult{Entry: wants[i].Entry(), Outcome: bucketindex.WantSatisfied, Err: err}
	}
	return results
}

func TestDiskSeriesPersistentCorruptionBecomesRepairWant(t *testing.T) {
	t.Parallel()
	for _, readonly := range []bool{false, true} {
		t.Run(map[bool]string{false: "owner", true: "readonly"}[readonly], func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			b := backend.Memory()
			writer := New(Config{Backend: b, Prefix: "metadata-repair", DiskSeries: true})
			t.Cleanup(func() { require.NoError(t, writer.Close(context.Background())) })
			s := benchSeries(1)
			_, err := writer.Append(s, 100, 1)
			require.NoError(t, err)
			require.NoError(t, writer.Flush(ctx))
			key := seriesIndexKey(writer.parts[0].prefix)
			good, err := b.Read(ctx, key)
			require.NoError(t, err)
			bad := append([]byte(nil), good...)
			bad[len(bad)-1] ^= 1
			require.NoError(t, b.Write(ctx, key, bad))
			before, version, err := b.ReadVersioned(ctx, writer.indexKey())
			require.NoError(t, err)
			peer := &restoreIndexFetcher{backend: b, key: key, data: good}
			reader := New(Config{Backend: b, Prefix: writer.cfg.Prefix, DiskSeries: true, Repair: peer})
			t.Cleanup(func() { require.NoError(t, reader.Close(context.Background())) })
			load := reader.LoadParts
			if readonly {
				load = reader.LoadPartsReadOnly
			}
			for range corruptLoadsBeforeWant {
				require.ErrorIs(t, load(ctx), seriesindex.ErrCorrupt)
				require.True(t, reader.IndexFenced())
				require.False(t, reader.HasWants())
			}
			require.NoError(t, load(ctx))
			require.False(t, reader.IndexFenced())
			require.True(t, reader.HasWants())
			require.False(t, reader.Stats().SeriesComplete)
			require.Empty(t, reader.parts)
			if readonly {
				after, afterVersion, err := b.ReadVersioned(ctx, writer.indexKey())
				require.NoError(t, err)
				require.Equal(t, before, after)
				require.Equal(t, version, afterVersion)
				return
			}
			reader.repairWants(ctx)
			require.Equal(t, 1, peer.calls, "corrupt metadata must make the local hold fail so a healthy peer is asked")
			require.False(t, reader.HasWants())
			require.True(t, reader.Stats().SeriesComplete)
			require.EqualValues(t, 1, reader.Stats().Series)
			require.Len(t, reader.parts, 1)
		})
	}
}

func TestDiskSeriesCachedCorruptionEventuallyBecomesWant(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	b := backend.Memory()
	writer := New(Config{Backend: b, Prefix: "cached-metadata-repair", DiskSeries: true})
	t.Cleanup(func() { require.NoError(t, writer.Close(context.Background())) })
	_, err := writer.Append(benchSeries(1), 100, 1)
	require.NoError(t, err)
	require.NoError(t, writer.Flush(ctx))
	reader := New(Config{Backend: b, Prefix: writer.cfg.Prefix, DiskSeries: true})
	t.Cleanup(func() { require.NoError(t, reader.Close(context.Background())) })
	require.NoError(t, reader.LoadPartsReadOnly(ctx))
	key := seriesIndexKey(writer.parts[0].prefix)
	raw, err := b.Read(ctx, key)
	require.NoError(t, err)
	raw[0] ^= 1
	require.NoError(t, b.Write(ctx, key, raw))
	require.ErrorIs(t, reader.LoadPartsReadOnly(ctx), seriesindex.ErrCorrupt)
	for range corruptLoadsBeforeWant {
		require.ErrorIs(t, reader.LoadPartsReadOnly(ctx), seriesindex.ErrCorrupt)
	}
	require.NoError(t, reader.LoadPartsReadOnly(ctx))
	require.True(t, reader.HasWants())
	require.False(t, reader.Stats().SeriesComplete)
}

func TestDiskSeriesNewerIndexVersionStaysFenced(t *testing.T) {
	t.Parallel()
	for _, feature := range []string{"version", "flags"} {
		t.Run(feature, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			b := backend.Memory()
			writer := New(Config{Backend: b, Prefix: "newer-index", DiskSeries: true})
			t.Cleanup(func() { require.NoError(t, writer.Close(context.Background())) })
			_, err := writer.Append(benchSeries(1), 100, 1)
			require.NoError(t, err)
			require.NoError(t, writer.Flush(ctx))
			key := seriesIndexKey(writer.parts[0].prefix)
			raw, err := b.Read(ctx, key)
			require.NoError(t, err)
			footer := raw[len(raw)-72:]
			if feature == "version" {
				binary.BigEndian.PutUint32(footer[4:], 2)
			} else {
				binary.BigEndian.PutUint32(footer[64:], 1)
			}
			binary.BigEndian.PutUint32(footer[68:], crc32.Checksum(footer[:68], crc32.MakeTable(crc32.Castagnoli)))
			require.NoError(t, b.Write(ctx, key, raw))
			reader := New(Config{Backend: b, Prefix: writer.cfg.Prefix, DiskSeries: true})
			t.Cleanup(func() { require.NoError(t, reader.Close(context.Background())) })
			for range corruptLoadsBeforeWant + 2 {
				require.ErrorIs(t, reader.LoadPartsReadOnly(ctx), seriesindex.ErrUnsupportedVersion)
				require.True(t, reader.IndexFenced())
				require.False(t, reader.HasWants())
				require.Empty(t, reader.corruptLoads)
			}
		})
	}
}
